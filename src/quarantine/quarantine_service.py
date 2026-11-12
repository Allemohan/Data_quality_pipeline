"""Quarantine service managing rejected records, error tracking, and record correction."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from src.database.models import QuarantineRecord
from src.database.repositories import QuarantineRepository
from src.validation.models import QuarantineCandidate

logger = logging.getLogger(__name__)


class QuarantineService:
    """Business service governing quarantine persistence, inspection, and correction."""

    def __init__(self, session: Session):
        self.session = session
        self.repo = QuarantineRepository(session)

    def quarantine_records(self, candidates: List[QuarantineCandidate]) -> int:
        """Stores rejected records into quarantine table with error details and lineage."""
        if not candidates:
            return 0
        count = self.repo.save_quarantine_candidates(candidates)
        logger.info(f"Quarantined {count} invalid records.")
        return count

    def get_record(self, quarantine_id: str) -> Optional[QuarantineRecord]:
        """Retrieves a quarantine record by ID."""
        return self.repo.get_by_id(quarantine_id)

    def list_records(self, status: str = "QUARANTINED", limit: int = 100) -> List[QuarantineRecord]:
        """Lists records currently in quarantine matching status."""
        return self.repo.get_records_by_status(status=status, limit=limit)

    def correct_record(self, quarantine_id: str, updated_fields: Dict[str, Any]) -> bool:
        """Applies manual or automated corrections to a quarantined record's payload.

        Transitions state from QUARANTINED (or REPLAY_FAILED) to CORRECTED.
        """
        record = self.get_record(quarantine_id)
        if not record:
            logger.warning(f"Quarantine record {quarantine_id} not found.")
            return False

        try:
            payload = json.loads(record.raw_payload)
        except Exception:
            payload = {}

        payload.update(updated_fields)
        record.raw_payload = json.dumps(payload, default=str)
        record.status = "CORRECTED"
        from datetime import datetime
        record.corrected_at = datetime.utcnow()
        self.session.flush()
        logger.info(f"Quarantine record {quarantine_id} marked as CORRECTED.")
        return True

