"""Central pipeline runner orchestrating ingestion, multi-stage validation, quarantine, and target upsert."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.database.connection import get_db_session, get_engine, init_db
from src.database.repositories import CustomerRepository, QuarantineRepository
from src.ingestion.csv_reader import CsvReader, IngestionError
from src.monitoring.metrics import PipelineMetricsTracker
from src.transformation.transformer import Transformer
from src.validation.business_validator import BusinessValidator
from src.validation.duplicate_validator import DuplicateValidator
from src.validation.models import ErrorCodes, QuarantineCandidate
from src.validation.schema_validator import SchemaValidator

logger = logging.getLogger(__name__)


class PipelineRunner:
    """Executes the Data Quality Ingestion & Quarantine Pipeline."""

    def __init__(self, engine: Optional[Engine] = None, batch_size: int = 1000):
        self.engine = engine or get_engine()
        self.batch_size = batch_size
        self.csv_reader = CsvReader(batch_size=batch_size)
        self.schema_validator = SchemaValidator()
        self.business_validator = BusinessValidator()
        self.duplicate_validator = DuplicateValidator()
        self.transformer = Transformer()

    def generate_run_id(self) -> str:
        """Generates a structured, deterministic ISO-prefixed unique execution ID."""
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        short_id = uuid.uuid4().hex[:6].upper()
        return f"RUN-{ts}-{short_id}"

    def run(self, input_file_path: Path | str, run_id: Optional[str] = None) -> PipelineMetricsTracker:
        """Runs the complete ingestion pipeline for a given input CSV file."""
        path = Path(input_file_path)
        active_run_id = run_id or self.generate_run_id()

        logger.info(f"Starting pipeline execution: run_id={active_run_id}, file={path.name}")

        # Ensure database and reference table are initialized
        init_db(self.engine)

        with get_db_session(self.engine) as session:
            customer_repo = CustomerRepository(session)
            quarantine_repo = QuarantineRepository(session)

            # Refresh active reference keys from database
            active_types = customer_repo.get_active_customer_type_ids()
            if active_types:
                self.business_validator.set_valid_reference_keys(active_types)

            tracker = PipelineMetricsTracker(
                run_id=active_run_id, source_file=path.name, session=session
            )
            self.duplicate_validator.reset()

            try:
                # Check empty file conditions
                is_empty, is_header_only = self.csv_reader.check_file_empty(path)
                if is_empty or is_header_only:
                    logger.info(f"File {path.name} is empty or header-only. Completing gracefully.")
                    tracker.complete(status="SUCCESS")
                    return tracker

                for batch_records in self.csv_reader.read_records(path, active_run_id):
                    tracker.record_input(len(batch_records))

                    # 1. Schema Validation
                    schema_passed = []
                    quarantine_batch: list[QuarantineCandidate] = []

                    for raw_rec in batch_records:
                        cleaned_rec, cand = self.schema_validator.validate_record(raw_rec)
                        if cand:
                            quarantine_batch.append(cand)
                        else:
                            schema_passed.append(cleaned_rec)

                    # 2. Business & Referential Validation
                    business_passed = []
                    for rec in schema_passed:
                        biz_rec, cand = self.business_validator.validate_record(rec)
                        if cand:
                            quarantine_batch.append(cand)
                        else:
                            business_passed.append(biz_rec)

                    # 3. Duplicate Detection (Intra-batch)
                    unique_records, duplicate_cands = self.duplicate_validator.process_records(business_passed)
                    quarantine_batch.extend(duplicate_cands)
                    tracker.record_duplicate(len(duplicate_cands))

                    # 4. Transformation
                    valid_to_load = []
                    for rec in unique_records:
                        transformed, cand = self.transformer.transform_record(rec)
                        if cand:
                            quarantine_batch.append(cand)
                        else:
                            valid_to_load.append(transformed)

                    # 5. Persist Quarantined Records
                    if quarantine_batch:
                        quarantine_repo.save_quarantine_candidates(quarantine_batch)
                        error_codes = [c.primary_error.error_code for c in quarantine_batch]
                        # Don't double count duplicate records in rejected_count
                        non_dupe_errors = [e for e in error_codes if e != ErrorCodes.DUPLICATE_RECORD]
                        tracker.record_rejected(len(non_dupe_errors), non_dupe_errors)

                    # 6. Persist Valid Records into Target Table (Idempotent Upsert)
                    if valid_to_load:
                        inserted, updated = customer_repo.upsert_customers(valid_to_load)
                        tracker.record_valid(len(valid_to_load), inserted=inserted, updated=updated)

                tracker.complete()
                logger.info(f"Pipeline finished: status={tracker.status}, valid={tracker.valid_count}")
                return tracker

            except IngestionError as e:
                logger.error(f"Ingestion error: {e}")
                tracker.complete(status="FAILED", error_message=str(e))
                return tracker
            except Exception as e:
                logger.error(f"Unexpected pipeline failure: {e}", exc_info=True)
                tracker.complete(status="FAILED", error_message=str(e))
                return tracker

