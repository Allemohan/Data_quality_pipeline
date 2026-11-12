"""Tests for schema, required-field, and business rule validations."""

from __future__ import annotations

from datetime import date, timedelta
import pytest

from src.validation.business_validator import BusinessValidator
from src.validation.models import ErrorCodes
from src.validation.schema_validator import SchemaValidator


def test_schema_validator_missing_column():
    validator = SchemaValidator()
    headers = ["customer_id", "customer_name", "email"]  # Missing status, customer_type_id, etc.
    errors = validator.validate_file_columns(headers)

    assert len(errors) == 1
    assert errors[0].error_code == ErrorCodes.SCHEMA_MISSING_COLUMN
    assert "Missing required columns" in errors[0].error_message


def test_schema_validator_unexpected_column():
    validator = SchemaValidator()
    headers = [
        "customer_id", "customer_name", "email", "phone", "country_code",
        "customer_type_id", "registration_date", "status", "updated_at",
        "unexpected_secret_col",
    ]
    errors = validator.validate_file_columns(headers)

    assert len(errors) == 1
    assert errors[0].error_code == ErrorCodes.SCHEMA_UNEXPECTED_COLUMN


def test_schema_validator_missing_required_fields():
    validator = SchemaValidator()
    raw_record = {
        "customer_id": "100",
        "customer_name": "",  # Empty
        "email": "user@example.com",
        "customer_type_id": "1",
        "registration_date": "2024-01-01",
        "status": "ACTIVE",
    }
    valid_rec, cand = validator.validate_record(raw_record)
    assert valid_rec is None
    assert cand is not None
    assert any(e.error_code == ErrorCodes.MISSING_REQUIRED_FIELD for e in cand.errors)


def test_schema_validator_invalid_integer_type():
    validator = SchemaValidator()
    raw_record = {
        "customer_id": "NOT_AN_INT",
        "customer_name": "Test User",
        "email": "user@example.com",
        "customer_type_id": "1",
        "registration_date": "2024-01-01",
        "status": "ACTIVE",
    }
    valid_rec, cand = validator.validate_record(raw_record)
    assert valid_rec is None
    assert cand is not None
    assert any(e.error_code == ErrorCodes.INVALID_DATA_TYPE for e in cand.errors)


def test_business_validator_invalid_email():
    validator = BusinessValidator()
    record = {
        "customer_id": 100,
        "customer_name": "Test User",
        "email": "plainaddress_without_at_sign",
        "customer_type_id": 1,
        "registration_date": date(2024, 1, 1),
        "status": "ACTIVE",
    }
    valid_rec, cand = validator.validate_record(record)
    assert valid_rec is None
    assert cand is not None
    assert cand.primary_error.error_code == ErrorCodes.INVALID_EMAIL


def test_business_validator_invalid_status():
    validator = BusinessValidator()
    record = {
        "customer_id": 100,
        "customer_name": "Test User",
        "email": "valid@example.com",
        "customer_type_id": 1,
        "registration_date": date(2024, 1, 1),
        "status": "UNKNOWN_CUSTOM_STATUS",
    }
    valid_rec, cand = validator.validate_record(record)
    assert valid_rec is None
    assert cand is not None
    assert cand.primary_error.error_code == ErrorCodes.INVALID_STATUS


def test_business_validator_future_registration_date():
    validator = BusinessValidator()
    future_date = date.today() + timedelta(days=30)
    record = {
        "customer_id": 100,
        "customer_name": "Test User",
        "email": "valid@example.com",
        "customer_type_id": 1,
        "registration_date": future_date,
        "status": "ACTIVE",
    }
    valid_rec, cand = validator.validate_record(record)
    assert valid_rec is None
    assert cand is not None
    assert cand.primary_error.error_code == ErrorCodes.FUTURE_DATE


def test_business_validator_invalid_reference_key():
    validator = BusinessValidator(valid_customer_type_ids={1, 2, 3})
    record = {
        "customer_id": 100,
        "customer_name": "Test User",
        "email": "valid@example.com",
        "customer_type_id": 999,  # Orphan ID
        "registration_date": date(2024, 1, 1),
        "status": "ACTIVE",
    }
    valid_rec, cand = validator.validate_record(record)
    assert valid_rec is None
    assert cand is not None
    assert cand.primary_error.error_code == ErrorCodes.INVALID_REFERENCE


def test_business_validator_negative_customer_id():
    validator = BusinessValidator()
    record = {
        "customer_id": -5,  # <= 0
        "customer_name": "Test User",
        "email": "valid@example.com",
        "customer_type_id": 1,
        "registration_date": date(2024, 1, 1),
        "status": "ACTIVE",
    }
    valid_rec, cand = validator.validate_record(record)
    assert valid_rec is None
    assert cand is not None
    assert cand.primary_error.error_code == ErrorCodes.INVALID_CUSTOMER_ID

