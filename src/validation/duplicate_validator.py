"""Duplicate detection engine distinguishing in-batch duplicates from idempotent target reruns."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from src.validation.models import (
    ErrorCodes,
    QuarantineCandidate,
    ValidationErrorDetail,
)

logger = logging.getLogger(__name__)


class DuplicateValidator:
    """Detects duplicate customer IDs within the current stream/batch."""

    def __init__(self):
        self.seen_customer_ids: Set[int] = set()

    def reset(self) -> None:
        """Resets the batch seen cache."""
        self.seen_customer_ids.clear()

    def process_records(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[QuarantineCandidate]]:
        """Separates distinct records from intra-batch duplicates.

        Returns:
            (unique_records, duplicate_quarantine_candidates)
        """
        unique_records: List[Dict[str, Any]] = []
        quarantine_candidates: List[QuarantineCandidate] = []

        for record in records:
            cust_id = record.get("customer_id")
            if cust_id in self.seen_customer_ids:
                # Flag as intra-batch duplicate
                source_file = record.get("_source_file", "unknown")
                source_row = record.get("_source_row_number", -1)
                run_id = record.get("_run_id", "unknown")
                raw_payload = record.get("_raw_payload", {})

                candidate = QuarantineCandidate(
                    source_record_id=str(cust_id),
                    source_file=source_file,
                    source_row_number=source_row,
                    run_id=run_id,
                    raw_payload=raw_payload,
                    errors=[
                        ValidationErrorDetail(
                            error_code=ErrorCodes.DUPLICATE_RECORD,
                            error_message=f"Duplicate customer_id {cust_id} encountered inside the same file/batch",
                            validation_stage="DuplicateValidation",
                            field_name="customer_id",
                        )
                    ],
                )
                quarantine_candidates.append(candidate)
            else:
                self.seen_customer_ids.add(cust_id)
                unique_records.append(record)

        return unique_records, quarantine_candidates
