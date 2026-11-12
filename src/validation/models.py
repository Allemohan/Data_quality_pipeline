"""Data validation models, error definitions, and validation outcome structures."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class ErrorCodes:
    """Standardized error codes catalog for validation and quarantine tracking."""
    SCHEMA_MISSING_COLUMN = "SCHEMA_MISSING_COLUMN"
    SCHEMA_UNEXPECTED_COLUMN = "SCHEMA_UNEXPECTED_COLUMN"
    MALFORMED_RECORD = "MALFORMED_RECORD"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_DATA_TYPE = "INVALID_DATA_TYPE"
    INVALID_CUSTOMER_ID = "INVALID_CUSTOMER_ID"
    INVALID_EMAIL = "INVALID_EMAIL"
    INVALID_STATUS = "INVALID_STATUS"
    FUTURE_DATE = "FUTURE_DATE"
    INVALID_REFERENCE = "INVALID_REFERENCE"
    DUPLICATE_RECORD = "DUPLICATE_RECORD"
    TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    REPLAY_ERROR = "REPLAY_ERROR"


@dataclass
class ValidationErrorDetail:
    """Detailed error reason for a quarantined record."""
    error_code: str
    error_message: str
    validation_stage: str
    field_name: Optional[str] = None


@dataclass
class QuarantineCandidate:
    """A record flagged for quarantine with full source lineage and error details."""
    source_record_id: Optional[str]
    source_file: str
    source_row_number: int
    run_id: str
    raw_payload: Dict[str, Any]
    errors: List[ValidationErrorDetail] = field(default_factory=list)

    @property
    def primary_error(self) -> ValidationErrorDetail:
        return self.errors[0] if self.errors else ValidationErrorDetail(
            error_code="UNKNOWN_ERROR",
            error_message="Unknown validation error",
            validation_stage="Validation",
        )


@dataclass
class ValidatedRecord:
    """A verified valid customer record ready for canonical transformation."""
    customer_id: int
    customer_name: str
    email: str
    phone: Optional[str]
    country_code: Optional[str]
    customer_type_id: int
    registration_date: date
    status: str
    updated_at: Optional[datetime]
    source_file: str
    source_row_number: int
    run_id: str
