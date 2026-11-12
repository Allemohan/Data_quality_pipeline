"""Database repositories for Target Customers, Quarantine, Pipeline Runs, and Replay Audit."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.database.models import Customer, CustomerType, PipelineRun, QuarantineRecord, ReplayRun
from src.validation.models import QuarantineCandidate, ValidatedRecord

logger = logging.getLogger(__name__)


class CustomerRepository:
    """Repository managing persistence and idempotent upsert into the target customers table."""

    def __init__(self, session: Session):
        self.session = session

    def get_active_customer_type_ids(self) -> Set[int]:
        """Returns set of valid customer_type_ids from reference table."""
        stmt = select(CustomerType.customer_type_id).where(CustomerType.is_active.is_(True))
        return set(self.session.scalars(stmt).all())

    def get_existing_ids(self, ids: List[int]) -> Set[int]:
        """Finds which customer_ids in the list already exist in target table."""
        if not ids:
            return set()
        stmt = select(Customer.customer_id).where(Customer.customer_id.in_(ids))
        return set(self.session.scalars(stmt).all())

    def upsert_customers(self, records: List[ValidatedRecord]) -> Tuple[int, int]:
        """Idempotently inserts or updates customer records.

        Returns:
            (inserted_count, updated_count)
        """
        if not records:
            return 0, 0

        bind = self.session.get_bind()
        dialect_name = bind.dialect.name if bind else "postgresql"

        # Check existing IDs to accurately report inserted vs updated count
        incoming_ids = [r.customer_id for r in records]
        existing_ids = self.get_existing_ids(incoming_ids)

        inserted_count = len(incoming_ids) - len(existing_ids)
        updated_count = len(existing_ids)

        now = datetime.utcnow()
        values_list = []
        for r in records:
            values_list.append({
                "customer_id": r.customer_id,
                "customer_name": r.customer_name,
                "email": r.email,
                "phone": r.phone,
                "country_code": r.country_code,
                "customer_type_id": r.customer_type_id,
                "registration_date": r.registration_date,
                "status": r.status,
                "source_file": r.source_file,
                "source_row_number": r.source_row_number,
                "run_id": r.run_id,
                "created_at": now,
                "updated_at": r.updated_at or now,
            })

        update_cols = {
            "customer_name": None,
            "email": None,
            "phone": None,
            "country_code": None,
            "customer_type_id": None,
            "registration_date": None,
            "status": None,
            "source_file": None,
            "source_row_number": None,
            "run_id": None,
            "updated_at": None,
        }

        if dialect_name == "postgresql":
            stmt = pg_insert(Customer).values(values_list)
            stmt = stmt.on_conflict_do_update(
                index_elements=[Customer.customer_id],
                set_={col: getattr(stmt.excluded, col) for col in update_cols},
            )
            self.session.execute(stmt)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(Customer).values(values_list)
            stmt = stmt.on_conflict_do_update(
                index_elements=[Customer.customer_id],
                set_={col: getattr(stmt.excluded, col) for col in update_cols},
            )
            self.session.execute(stmt)
        else:
            # Fallback standard merge
            for row in values_list:
                cust = Customer(**row)
                self.session.merge(cust)

        return inserted_count, updated_count


class QuarantineRepository:
    """Repository managing quarantine record storage, updates, and replay state queries."""

    def __init__(self, session: Session):
        self.session = session

    def save_quarantine_candidates(self, candidates: List[QuarantineCandidate]) -> int:
        """Persists invalid records into quarantine_records table with full error context."""
        if not candidates:
            return 0

        saved = 0
        now = datetime.utcnow()

        for c in candidates:
            primary = c.primary_error
            # Concatenate multiple errors if present so no reasons are lost
            all_errors_msg = " | ".join(
                f"[{e.error_code}] {e.error_message}" for e in c.errors
            )
            payload_str = json.dumps(c.raw_payload, default=str)

            q_record = QuarantineRecord(
                source_record_id=c.source_record_id,
                source_file=c.source_file,
                source_row_number=c.source_row_number,
                run_id=c.run_id,
                raw_payload=payload_str,
                error_code=primary.error_code,
                error_message=all_errors_msg,
                validation_stage=primary.validation_stage,
                status="QUARANTINED",
                retry_count=0,
                first_seen_at=now,
                last_seen_at=now,
            )
            self.session.add(q_record)
            saved += 1

        return saved

    def get_records_by_status(self, status: str = "CORRECTED", limit: int = 500) -> List[QuarantineRecord]:
        """Retrieves quarantine records eligible for replay or inspection."""
        stmt = (
            select(QuarantineRecord)
            .where(QuarantineRecord.status == status)
            .limit(limit)
        )
        return list(self.session.scalars(stmt).all())

    def get_by_id(self, quarantine_id: str) -> Optional[QuarantineRecord]:
        """Fetches a specific quarantine record by primary key."""
        return self.session.get(QuarantineRecord, quarantine_id)

    def mark_as_corrected(self, quarantine_id: str, corrected_payload: Dict[str, Any]) -> bool:
        """Marks a quarantine record as CORRECTED with new payload ready for replay."""
        rec = self.get_by_id(quarantine_id)
        if not rec:
            return False
        rec.raw_payload = json.dumps(corrected_payload, default=str)
        rec.status = "CORRECTED"
        rec.corrected_at = datetime.utcnow()
        return True


class PipelineRunRepository:
    """Tracks overall execution lifecycle and aggregated operational metrics."""

    def __init__(self, session: Session):
        self.session = session

    def create_run(self, run_id: str, source_file: str) -> PipelineRun:
        """Registers a new pipeline run with STARTED status."""
        run = PipelineRun(
            run_id=run_id,
            source_file=source_file,
            started_at=datetime.utcnow(),
            status="STARTED",
            input_count=0,
            valid_count=0,
            rejected_count=0,
            duplicate_count=0,
            processing_time_seconds=0.0,
        )
        self.session.add(run)
        self.session.flush()
        return run

    def complete_run(
        self,
        run_id: str,
        status: str,
        input_count: int,
        valid_count: int,
        rejected_count: int,
        duplicate_count: int,
        processing_time_seconds: float,
        error_message: Optional[str] = None,
    ) -> Optional[PipelineRun]:
        """Finalizes run metrics and status."""
        run = self.session.get(PipelineRun, run_id)
        if run:
            run.status = status
            run.completed_at = datetime.utcnow()
            run.input_count = input_count
            run.valid_count = valid_count
            run.rejected_count = rejected_count
            run.duplicate_count = duplicate_count
            run.processing_time_seconds = round(processing_time_seconds, 4)
            run.error_message = error_message
        return run


class ReplayRunRepository:
    """Logs individual quarantine replay operations."""

    def __init__(self, session: Session):
        self.session = session

    def log_replay(
        self,
        quarantine_id: str,
        original_run_id: str,
        status: str,
        old_error: Optional[str],
        new_error: Optional[str] = None,
        target_record_id: Optional[str] = None,
        started_at: Optional[datetime] = None,
    ) -> ReplayRun:
        """Creates an audit log entry for a replay event."""
        run = ReplayRun(
            quarantine_id=quarantine_id,
            original_run_id=original_run_id,
            replay_started_at=started_at or datetime.utcnow(),
            replay_completed_at=datetime.utcnow(),
            status=status,
            old_error=old_error,
            new_error=new_error,
            target_record_id=target_record_id,
        )
        self.session.add(run)
        return run

