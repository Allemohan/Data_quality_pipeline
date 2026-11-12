"""Business and referential integrity validator."""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple

from src.validation.models import (
    ErrorCodes,
    QuarantineCandidate,
    ValidationErrorDetail,
)

# Standard RFC 5322 compliant regex for email syntax checking
EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
)


class BusinessValidator:
    """Validates business domain rules and referential integrity constraints."""

    def __init__(
        self,
        valid_statuses: Optional[List[str]] = None,
        valid_customer_type_ids: Optional[Set[int]] = None,
    ):
        self.valid_statuses: Set[str] = set(
            valid_statuses or ["ACTIVE", "INACTIVE", "SUSPENDED"]
        )
        self.valid_customer_type_ids: Set[int] = (
            valid_customer_type_ids if valid_customer_type_ids is not None else {1, 2, 3, 4, 5}
        )

    def set_valid_reference_keys(self, type_ids: Set[int]) -> None:
        """Update valid customer_type_ids dynamically from database reference table."""
        self.valid_customer_type_ids = type_ids

    def validate_record(
        self, record: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[QuarantineCandidate]]:
        """Validates domain business rules for a schema-valid record.

        Returns:
            (record, None) if business rules pass,
            or (None, QuarantineCandidate) with exact error codes if rules fail.
        """
        source_file = record.get("_source_file", "unknown")
        source_row = record.get("_source_row_number", -1)
        run_id = record.get("_run_id", "unknown")
        raw_payload = record.get("_raw_payload", {})
        customer_id = record.get("customer_id")
        source_record_id = str(customer_id) if customer_id is not None else None

        errors: List[ValidationErrorDetail] = []

        # Rule 1: customer_id > 0
        if customer_id is None or customer_id <= 0:
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.INVALID_CUSTOMER_ID,
                    error_message=f"customer_id {customer_id} must be a strictly positive integer (> 0)",
                    validation_stage="BusinessValidation",
                    field_name="customer_id",
                )
            )

        # Rule 2: Email format regex validation
        email = record.get("email", "")
        if not email or not EMAIL_REGEX.match(email):
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.INVALID_EMAIL,
                    error_message=f"Email address '{email}' is not a valid email syntax",
                    validation_stage="BusinessValidation",
                    field_name="email",
                )
            )

        # Rule 3: Account status enum validation
        status = record.get("status", "")
        if status not in self.valid_statuses:
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.INVALID_STATUS,
                    error_message=f"Status '{status}' is invalid. Allowed: {sorted(list(self.valid_statuses))}",
                    validation_stage="BusinessValidation",
                    field_name="status",
                )
            )

        # Rule 4: Registration date cannot be in the future
        reg_date = record.get("registration_date")
        if isinstance(reg_date, date) and reg_date > date.today():
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.FUTURE_DATE,
                    error_message=f"Registration date {reg_date} cannot be in the future (today: {date.today()})",
                    validation_stage="BusinessValidation",
                    field_name="registration_date",
                )
            )

        # Rule 5: Referential integrity: customer_type_id must exist in reference table
        cust_type_id = record.get("customer_type_id")
        if cust_type_id not in self.valid_customer_type_ids:
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.INVALID_REFERENCE,
                    error_message=f"customer_type_id {cust_type_id} does not exist in reference table customer_types",
                    validation_stage="BusinessValidation",
                    field_name="customer_type_id",
                )
            )

        if errors:
            return None, QuarantineCandidate(
                source_record_id=source_record_id,
                source_file=source_file,
                source_row_number=source_row,
                run_id=run_id,
                raw_payload=raw_payload,
                errors=errors,
            )

        return record, None

