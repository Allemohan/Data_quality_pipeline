"""Tests for quarantine storage, traceability, and correction."""

from __future__ import annotations

import json
from src.database.models import QuarantineRecord
from src.database.repositories import QuarantineRepository
from src.quarantine.quarantine_service import QuarantineService
from src.validation.models import ErrorCodes, QuarantineCandidate, ValidationErrorDetail


def test_quarantine_storage_and_lineage(db_session):
    service = QuarantineService(db_session)
    candidate = QuarantineCandidate(
        source_record_id="CUST-999",
        source_file="batch_01.csv",
        source_row_number=14,
        run_id="RUN-20261112-TEST",
        raw_payload={"customer_id": "CUST-999", "email": "bad_email"},
        errors=[
            ValidationErrorDetail(
                error_code=ErrorCodes.INVALID_EMAIL,
                error_message="Email is invalid",
                validation_stage="BusinessValidation",
                field_name="email",
            )
        ],
    )

    count = service.quarantine_records([candidate])
    assert count == 1

    records = service.list_records("QUARANTINED")
    assert len(records) == 1
    stored = records[0]

    assert stored.source_record_id == "CUST-999"
    assert stored.source_file == "batch_01.csv"
    assert stored.source_row_number == 14
    assert stored.run_id == "RUN-20261112-TEST"
    assert stored.error_code == ErrorCodes.INVALID_EMAIL
    assert stored.status == "QUARANTINED"
    assert stored.retry_count == 0

    payload = json.loads(stored.raw_payload)
    assert payload["email"] == "bad_email"


def test_quarantine_correction_transition(db_session):
    service = QuarantineService(db_session)
    candidate = QuarantineCandidate(
        source_record_id="CUST-100",
        source_file="batch_02.csv",
        source_row_number=5,
        run_id="RUN-TEST",
        raw_payload={"customer_id": "100", "email": "bad_email"},
        errors=[ValidationErrorDetail(error_code=ErrorCodes.INVALID_EMAIL, error_message="bad", validation_stage="Biz")],
    )
    service.quarantine_records([candidate])
    q_rec = service.list_records("QUARANTINED")[0]

    # Apply correction
    success = service.correct_record(q_rec.quarantine_id, {"email": "fixed@example.com"})
    assert success is True

    updated_rec = service.get_record(q_rec.quarantine_id)
    assert updated_rec.status == "CORRECTED"
    assert updated_rec.corrected_at is not None
    payload = json.loads(updated_rec.raw_payload)
    assert payload["email"] == "fixed@example.com"

