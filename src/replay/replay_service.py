"""Controlled replay service for corrected quarantine records."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from src.database.models import QuarantineRecord
from src.database.repositories import (
    CustomerRepository,
    QuarantineRepository,
    ReplayRunRepository,
)
from src.transformation.transformer import Transformer
from src.validation.business_validator import BusinessValidator
from src.validation.schema_validator import SchemaValidator

logger = logging.getLogger(__name__)


class ReplayResult:
    """Encapsulates aggregate results of a replay execution batch."""

    def __init__(self):
        self.eligible_count: int = 0
        self.success_count: int = 0
        self.failed_count: int = 0
        self.details: List[Dict[str, Any]] = []

    def format_summary(self) -> str:
        lines = [
            "=" * 60,
            "CONTROLLED REPLAY REPORT",
            "=" * 60,
            f"Eligible Records Found : {self.eligible_count}",
            f"Successfully Replayed  : {self.success_count}",
            f"Failed Replays         : {self.failed_count}",
            "=" * 60,
        ]
        if self.details:
            lines.append("Replay Details:")
            for d in self.details:
                lines.append(
                    f" - QID: {d['quarantine_id'][:8]}... | Status: {d['status']} | Record: {d.get('record_id')} | Msg: {d.get('message', '')}"
                )
            lines.append("=" * 60)
        return "\n".join(lines)


class ReplayService:
    """Controlled replay coordinator that safely selects, re-validates, and loads corrected records."""

    def __init__(self, session: Session):
        self.session = session
        self.customer_repo = CustomerRepository(session)
        self.quarantine_repo = QuarantineRepository(session)
        self.replay_repo = ReplayRunRepository(session)

        self.schema_validator = SchemaValidator()
        self.business_validator = BusinessValidator()
        self.transformer = Transformer()

    def replay_corrected_records(self, limit: int = 500) -> ReplayResult:
        """Pulls records marked CORRECTED, re-runs validations, updates target and audit trail."""
        result = ReplayResult()

        # Update valid customer types from DB reference table
        active_types = self.customer_repo.get_active_customer_type_ids()
        if active_types:
            self.business_validator.set_valid_reference_keys(active_types)

        records_to_replay = self.quarantine_repo.get_records_by_status(status="CORRECTED", limit=limit)
        result.eligible_count = len(records_to_replay)

        if not records_to_replay:
            logger.info("No records with status 'CORRECTED' found for replay.")
            return result

        logger.info(f"Beginning controlled replay for {len(records_to_replay)} records.")

        for q_record in records_to_replay:
            self._process_single_replay(q_record, result)

        self.session.flush()
        return result

    def _process_single_replay(self, q_record: QuarantineRecord, result: ReplayResult) -> None:
        """Processes and re-validates one quarantine record."""
        started_at = datetime.utcnow()
        old_error = q_record.error_code

        try:
            payload = json.loads(q_record.raw_payload)
        except Exception as e:
            self._handle_replay_failure(
                q_record,
                new_error="CORRUPTED_PAYLOAD",
                error_msg=f"Cannot parse JSON payload: {e}",
                started_at=started_at,
                result=result,
            )
            return

        # Inject lineage context for validation
        payload["_source_file"] = q_record.source_file
        payload["_source_row_number"] = q_record.source_row_number
        payload["_run_id"] = f"REPLAY-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

        # 1. Re-run Schema Validation
        schema_valid, schema_cand = self.schema_validator.validate_record(payload)
        if schema_cand:
            primary = schema_cand.primary_error
            self._handle_replay_failure(
                q_record,
                new_error=primary.error_code,
                error_msg=primary.error_message,
                started_at=started_at,
                result=result,
            )
            return

        # 2. Re-run Business Validation
        biz_valid, biz_cand = self.business_validator.validate_record(schema_valid)
        if biz_cand:
            primary = biz_cand.primary_error
            self._handle_replay_failure(
                q_record,
                new_error=primary.error_code,
                error_msg=primary.error_message,
                started_at=started_at,
                result=result,
            )
            return

        # 3. Transform
        transformed, trans_cand = self.transformer.transform_record(biz_valid)
        if trans_cand:
            primary = trans_cand.primary_error
            self._handle_replay_failure(
                q_record,
                new_error=primary.error_code,
                error_msg=primary.error_message,
                started_at=started_at,
                result=result,
            )
            return

        # 4. Idempotently load into target customers table
        self.customer_repo.upsert_customers([transformed])

        # 5. Update Quarantine status to REPLAYED
        now = datetime.utcnow()
        q_record.status = "REPLAYED"
        q_record.replayed_at = now
        q_record.last_seen_at = now

        # 6. Audit in replay_runs
        self.replay_repo.log_replay(
            quarantine_id=q_record.quarantine_id,
            original_run_id=q_record.run_id,
            status="SUCCESS",
            old_error=old_error,
            new_error=None,
            target_record_id=str(transformed.customer_id),
            started_at=started_at,
        )

        result.success_count += 1
        result.details.append({
            "quarantine_id": q_record.quarantine_id,
            "status": "SUCCESS",
            "record_id": str(transformed.customer_id),
            "message": "Successfully revalidated and loaded into target customers table.",
        })
        logger.info(f"Replay succeeded for quarantine_id {q_record.quarantine_id} (customer_id={transformed.customer_id}).")

    def _handle_replay_failure(
        self,
        q_record: QuarantineRecord,
        new_error: str,
        error_msg: str,
        started_at: datetime,
        result: ReplayResult,
    ) -> None:
        """Handles replay rejection: increments retry count, updates quarantine, logs audit."""
        now = datetime.utcnow()
        q_record.status = "REPLAY_FAILED"
        q_record.retry_count += 1
        q_record.last_seen_at = now
        q_record.error_code = new_error
        q_record.error_message = f"[REPLAY_FAILURE] {error_msg}"

        self.replay_repo.log_replay(
            quarantine_id=q_record.quarantine_id,
            original_run_id=q_record.run_id,
            status="FAILED",
            old_error=q_record.error_code,
            new_error=new_error,
            target_record_id=q_record.source_record_id,
            started_at=started_at,
        )

        result.failed_count += 1
        result.details.append({
            "quarantine_id": q_record.quarantine_id,
            "status": "REPLAY_FAILED",
            "record_id": q_record.source_record_id,
            "message": error_msg,
        })
        logger.warning(
            f"Replay failed for quarantine_id {q_record.quarantine_id} (retry #{q_record.retry_count}): {new_error} - {error_msg}"
        )

