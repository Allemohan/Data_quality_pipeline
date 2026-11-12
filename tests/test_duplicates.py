"""Tests for duplicate detection engine."""

from __future__ import annotations

from src.validation.duplicate_validator import DuplicateValidator
from src.validation.models import ErrorCodes


def test_batch_duplicate_detection():
    validator = DuplicateValidator()
    records = [
        {"customer_id": 101, "customer_name": "First Record", "email": "a@example.com"},
        {"customer_id": 102, "customer_name": "Second Record", "email": "b@example.com"},
        {"customer_id": 101, "customer_name": "Duplicate of First", "email": "c@example.com"},
        {"customer_id": 103, "customer_name": "Third Record", "email": "d@example.com"},
        {"customer_id": 101, "customer_name": "Triplicate of First", "email": "e@example.com"},
    ]

    unique, duplicates = validator.process_records(records)

    assert len(unique) == 3
    assert [r["customer_id"] for r in unique] == [101, 102, 103]

    assert len(duplicates) == 2
    for d in duplicates:
        assert d.primary_error.error_code == ErrorCodes.DUPLICATE_RECORD
        assert d.source_record_id == "101"
