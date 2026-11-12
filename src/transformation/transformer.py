"""Data transformation and canonicalization layer."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.validation.models import (
    ErrorCodes,
    QuarantineCandidate,
    ValidatedRecord,
    ValidationErrorDetail,
)

logger = logging.getLogger(__name__)


class Transformer:
    """Canonicalizes, cleanses, and transforms validated record dictionaries into target entities."""

    def sanitize_phone(self, phone_str: Optional[str]) -> Optional[str]:
        """Cleans and standardizes phone number strings."""
        if not phone_str:
            return None
        # Remove extra whitespace
        cleaned = phone_str.strip()
        return cleaned if cleaned else None

    def transform_record(
        self, record: Dict[str, Any]
    ) -> Tuple[Optional[ValidatedRecord], Optional[QuarantineCandidate]]:
        """Transforms a validated dictionary into a canonical ValidatedRecord entity.

        Returns:
            (validated_record, None) on success,
            or (None, QuarantineCandidate) on transformation exception.
        """
        source_file = record.get("_source_file", "unknown")
        source_row = record.get("_source_row_number", -1)
        run_id = record.get("_run_id", "unknown")
        raw_payload = record.get("_raw_payload", {})
        customer_id = record.get("customer_id")

        try:
            # 1. Cleanse customer name: collapse internal spaces
            raw_name = str(record["customer_name"]).strip()
            clean_name = re.sub(r"\s+", " ", raw_name)

            # 2. Normalize email to lowercase
            clean_email = str(record["email"]).strip().lower()

            # 3. Standardize phone and country
            clean_phone = self.sanitize_phone(record.get("phone"))
            clean_country = (
                str(record["country_code"]).strip().upper()
                if record.get("country_code")
                else None
            )

            # 4. Standardize timestamps
            updated_at = record.get("updated_at")
            if not updated_at:
                updated_at = datetime.utcnow()

            transformed = ValidatedRecord(
                customer_id=int(customer_id),
                customer_name=clean_name,
                email=clean_email,
                phone=clean_phone,
                country_code=clean_country,
                customer_type_id=int(record["customer_type_id"]),
                registration_date=record["registration_date"],
                status=str(record["status"]).strip().upper(),
                updated_at=updated_at,
                source_file=source_file,
                source_row_number=source_row,
                run_id=run_id,
            )
            return transformed, None

        except Exception as e:
            logger.error(f"Transformation failed on row {source_row} (cust_id={customer_id}): {e}")
            return None, QuarantineCandidate(
                source_record_id=str(customer_id) if customer_id else None,
                source_file=source_file,
                source_row_number=source_row,
                run_id=run_id,
                raw_payload=raw_payload,
                errors=[
                    ValidationErrorDetail(
                        error_code=ErrorCodes.TRANSFORMATION_ERROR,
                        error_message=f"Transformation error: {str(e)}",
                        validation_stage="Transformation",
                    )
                ],
            )

