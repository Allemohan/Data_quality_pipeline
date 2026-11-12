"""Tests for controlled replay mechanism and replay audit tracking."""

from __future__ import annotations

import json
from datetime import date
from src.database.models import Customer, QuarantineRecord, ReplayRun
from src.database.repositories import CustomerRepository, QuarantineRepository
from src.quarantine.quarantine_service import QuarantineService
from src.replay.replay_service import ReplayService
from src.validation.models import ErrorCodes, QuarantineCandidate, ValidationErrorDetail


def test_replay_success(db_session):
    q_service = QuarantineService(db_session)
    candidate = QuarantineCandidate(
        source_record_id="500",
        source_file="batch_03.csv",
        source_row_number=10,
        run_id="RUN-ORIGINAL",
        raw_payload={
            "customer_id": "500",
            "customer_name": "John Doe",
            "email": "invalid_email_syntax",
            "customer_type_id": "1",
            "registration_date": "2024-01-01",
            "status": "ACTIVE",
        },
        errors=[ValidationErrorDetail(error_code=ErrorCodes.INVALID_EMAIL, error_message="bad email", validation_stage="Biz")],
    )
    q_service.quarantine_records([candidate])
    q_rec = q_service.list_records("QUARANTINED")[0]

    # Operator corrects email
    q_service.correct_record(q_rec.quarantine_id, {"email": "john.doe@example.com"})

    # Execute replay
    replay_service = ReplayService(db_session)
    result = replay_service.replay_corrected_records()

    assert result.eligible_count == 1
    assert result.success_count == 1
    assert result.failed_count == 0

    # Verify quarantine status
    updated_q = q_service.get_record(q_rec.quarantine_id)
    assert updated_q.status == "REPLAYED"
    assert updated_q.replayed_at is not None

    # Verify record in target customers table
    cust = db_session.get(Customer, 500)
    assert cust is not None
    assert cust.customer_name == "John Doe"
    assert cust.email == "john.doe@example.com"

    # Verify audit in replay_runs
    audit = db_session.query(ReplayRun).filter_by(quarantine_id=q_rec.quarantine_id).first()
    assert audit is not None
    assert audit.status == "SUCCESS"
    assert audit.target_record_id == "500"


def test_replay_failure_with_retry_increment(db_session):
    q_service = QuarantineService(db_session)
    candidate = QuarantineCandidate(
        source_record_id="501",
        source_file="batch_04.csv",
        source_row_number=12,
        run_id="RUN-ORIGINAL",
        raw_payload={
            "customer_id": "501",
            "customer_name": "Jane Doe",
            "email": "bad_email",
            "customer_type_id": "1",
            "registration_date": "2024-01-01",
            "status": "ACTIVE",
        },
        errors=[ValidationErrorDetail(error_code=ErrorCodes.INVALID_EMAIL, error_message="bad email", validation_stage="Biz")],
    )
    q_service.quarantine_records([candidate])
    q_rec = q_service.list_records("QUARANTINED")[0]

    # Operator incorrectly corrects with another invalid email
    q_service.correct_record(q_rec.quarantine_id, {"email": "still_no_at_sign_domain"})

    replay_service = ReplayService(db_session)
    result = replay_service.replay_corrected_records()

    assert result.eligible_count == 1
    assert result.success_count == 0
    assert result.failed_count == 1

    updated_q = q_service.get_record(q_rec.quarantine_id)
    assert updated_q.status == "REPLAY_FAILED"
    assert updated_q.retry_count == 1
    assert updated_q.error_code == ErrorCodes.INVALID_EMAIL

    # Target table does NOT contain record
    assert db_session.get(Customer, 501) is None

    # Audit logged failure
    audit = db_session.query(ReplayRun).filter_by(quarantine_id=q_rec.quarantine_id).first()
    assert audit is not None
    assert audit.status == "FAILED"

