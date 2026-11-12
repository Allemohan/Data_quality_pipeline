"""Pydantic-based schema and required-field validator."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field, ValidationError

from src.ingestion.csv_reader import EXPECTED_COLUMNS
from src.validation.models import (
    ErrorCodes,
    QuarantineCandidate,
    ValidationErrorDetail,
)


class CustomerRawModel(BaseModel):
    """Pydantic model representing expected types and constraints for raw incoming rows."""
    customer_id: Any
    customer_name: Any
    email: Any
    phone: Optional[Any] = None
    country_code: Optional[Any] = None
    customer_type_id: Any
    registration_date: Any
    status: Any
    updated_at: Optional[Any] = None


class SchemaValidator:
    """Validates CSV structure, column sets, required fields, and structural data types."""

    def __init__(self, expected_columns: Optional[List[str]] = None):
        self.expected_columns: List[str] = expected_columns or EXPECTED_COLUMNS
        self.expected_set: Set[str] = set(self.expected_columns)

    def validate_file_columns(self, actual_columns: List[str]) -> List[ValidationErrorDetail]:
        """Validates that the file header matches the expected column contract."""
        errors: List[ValidationErrorDetail] = []
        actual_set = set(actual_columns)

        missing = self.expected_set - actual_set
        if missing:
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.SCHEMA_MISSING_COLUMN,
                    error_message=f"Missing required columns in CSV header: {sorted(list(missing))}",
                    validation_stage="SchemaValidation",
                )
            )

        unexpected = actual_set - self.expected_set
        # Ignore lineage metadata internal columns starting with '_'
        unexpected = {c for c in unexpected if not c.startswith("_")}
        if unexpected:
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.SCHEMA_UNEXPECTED_COLUMN,
                    error_message=f"Unexpected columns found in CSV header: {sorted(list(unexpected))}",
                    validation_stage="SchemaValidation",
                )
            )

        return errors

    def validate_record(self, record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[QuarantineCandidate]]:
        """Validates a single record's schema, required fields, and structural types.

        Returns:
            (cleaned_dict, None) if valid at schema level,
            or (None, QuarantineCandidate) if schema validation failed.
        """
        source_file = record.get("_source_file", "unknown")
        source_row = record.get("_source_row_number", -1)
        run_id = record.get("_run_id", "unknown")
        raw_cust_id = record.get("customer_id")
        source_record_id = str(raw_cust_id).strip() if raw_cust_id is not None and str(raw_cust_id).strip() != "" else None

        # Clean payload without internal lineage keys
        payload = {k: v for k, v in record.items() if not k.startswith("_")}
        errors: List[ValidationErrorDetail] = []

        # 1. Check required fields for presence and non-emptiness
        required_fields = ["customer_id", "customer_name", "email", "customer_type_id", "registration_date", "status"]
        for field in required_fields:
            val = record.get(field)
            if val is None or str(val).strip() == "" or str(val).strip().upper() == "NULL":
                errors.append(
                    ValidationErrorDetail(
                        error_code=ErrorCodes.MISSING_REQUIRED_FIELD,
                        error_message=f"Required field '{field}' is missing or null",
                        validation_stage="RequiredFieldValidation",
                        field_name=field,
                    )
                )

        if errors:
            return None, QuarantineCandidate(
                source_record_id=source_record_id,
                source_file=source_file,
                source_row_number=source_row,
                run_id=run_id,
                raw_payload=payload,
                errors=errors,
            )

        # 2. Type conversions and syntax checks
        parsed_customer_id: Optional[int] = None
        try:
            parsed_customer_id = int(str(record["customer_id"]).strip())
        except (ValueError, TypeError):
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.INVALID_DATA_TYPE,
                    error_message=f"customer_id '{record['customer_id']}' cannot be parsed as an integer",
                    validation_stage="SchemaValidation",
                    field_name="customer_id",
                )
            )

        parsed_type_id: Optional[int] = None
        try:
            parsed_type_id = int(str(record["customer_type_id"]).strip())
        except (ValueError, TypeError):
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.INVALID_DATA_TYPE,
                    error_message=f"customer_type_id '{record['customer_type_id']}' cannot be parsed as an integer",
                    validation_stage="SchemaValidation",
                    field_name="customer_type_id",
                )
            )

        parsed_reg_date: Optional[date] = None
        reg_date_str = str(record["registration_date"]).strip()
        try:
            parsed_reg_date = datetime.strptime(reg_date_str, "%Y-%m-%d").date()
        except ValueError:
            errors.append(
                ValidationErrorDetail(
                    error_code=ErrorCodes.MALFORMED_RECORD,
                    error_message=f"registration_date '{reg_date_str}' does not match expected format YYYY-MM-DD",
                    validation_stage="SchemaValidation",
                    field_name="registration_date",
                )
            )

        parsed_updated_at: Optional[datetime] = None
        raw_updated = record.get("updated_at")
        if raw_updated is not None and str(raw_updated).strip() != "" and str(raw_updated).strip().upper() != "NULL":
            updated_str = str(raw_updated).strip()
            # Try parsing ISO timestamp or datetime formats
            try:
                parsed_updated_at = datetime.fromisoformat(updated_str)
            except ValueError:
                try:
                    parsed_updated_at = datetime.strptime(updated_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    errors.append(
                        ValidationErrorDetail(
                            error_code=ErrorCodes.MALFORMED_RECORD,
                            error_message=f"updated_at '{updated_str}' is not a valid timestamp format",
                            validation_stage="SchemaValidation",
                            field_name="updated_at",
                        )
                    )

        if errors:
            return None, QuarantineCandidate(
                source_record_id=source_record_id,
                source_file=source_file,
                source_row_number=source_row,
                run_id=run_id,
                raw_payload=payload,
                errors=errors,
            )

        # Passed schema and type validation: construct normalized record dict
        normalized = {
            "customer_id": parsed_customer_id,
            "customer_name": str(record["customer_name"]).strip(),
            "email": str(record["email"]).strip(),
            "phone": str(record["phone"]).strip() if record.get("phone") else None,
            "country_code": str(record["country_code"]).strip() if record.get("country_code") else None,
            "customer_type_id": parsed_type_id,
            "registration_date": parsed_reg_date,
            "status": str(record["status"]).strip().upper(),
            "updated_at": parsed_updated_at,
            "_source_file": source_file,
            "_source_row_number": source_row,
            "_run_id": run_id,
            "_raw_payload": payload,
        }
        return normalized, None
