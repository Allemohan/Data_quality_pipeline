"""End-to-end pipeline integration tests."""

from __future__ import annotations

from pathlib import Path
from sqlalchemy import text

from src.pipeline.runner import PipelineRunner


def test_pipeline_end_to_end_mixed_dataset(in_memory_engine, make_csv_file):
    records = [
        # Valid
        {
            "customer_id": "1",
            "customer_name": "Valid Alpha",
            "email": "alpha@example.com",
            "phone": "1234567890",
            "country_code": "US",
            "customer_type_id": "1",
            "registration_date": "2024-01-01",
            "status": "ACTIVE",
            "updated_at": "2024-01-01 10:00:00",
        },
        # Duplicate of Valid Alpha
        {
            "customer_id": "1",
            "customer_name": "Duplicate Alpha",
            "email": "alpha.dupe@example.com",
            "phone": "",
            "country_code": "US",
            "customer_type_id": "1",
            "registration_date": "2024-01-01",
            "status": "ACTIVE",
            "updated_at": "2024-01-01 11:00:00",
        },
        # Invalid email
        {
            "customer_id": "2",
            "customer_name": "Invalid Email Beta",
            "email": "invalid_email_beta",
            "phone": "",
            "country_code": "CA",
            "customer_type_id": "2",
            "registration_date": "2024-01-02",
            "status": "ACTIVE",
            "updated_at": "",
        },
        # Missing required name
        {
            "customer_id": "3",
            "customer_name": "",
            "email": "gamma@example.com",
            "phone": "",
            "country_code": "FR",
            "customer_type_id": "3",
            "registration_date": "2024-01-03",
            "status": "INACTIVE",
            "updated_at": "",
        },
        # Valid
        {
            "customer_id": "4",
            "customer_name": "Valid Delta",
            "email": "delta@example.com",
            "phone": "",
            "country_code": "DE",
            "customer_type_id": "4",
            "registration_date": "2024-01-04",
            "status": "SUSPENDED",
            "updated_at": "",
        },
    ]

    csv_file = make_csv_file(records, "mixed_test.csv")
    runner = PipelineRunner(engine=in_memory_engine)
    tracker = runner.run(csv_file)

    assert tracker.input_count == 5
    assert tracker.valid_count == 2
    assert tracker.rejected_count == 2
    assert tracker.duplicate_count == 1
    assert tracker.is_reconciled is True
    assert tracker.status == "PARTIAL_SUCCESS"

    with in_memory_engine.connect() as conn:
        cust_count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        q_count = conn.execute(text("SELECT COUNT(*) FROM quarantine_records")).scalar()
        run_count = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs")).scalar()

    assert cust_count == 2
    assert q_count == 3  # 2 rejected + 1 duplicate
    assert run_count == 1


def test_pipeline_empty_file_handling(in_memory_engine, tmp_path: Path):
    empty_file = tmp_path / "empty_input.csv"
    empty_file.write_text("")

    runner = PipelineRunner(engine=in_memory_engine)
    tracker = runner.run(empty_file)

    assert tracker.input_count == 0
    assert tracker.valid_count == 0
    assert tracker.rejected_count == 0
    assert tracker.status == "SUCCESS"
