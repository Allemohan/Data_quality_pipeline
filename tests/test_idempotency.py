"""Tests for pipeline idempotency and rerun safety."""

from __future__ import annotations

from sqlalchemy import func
from src.database.models import Customer
from src.pipeline.runner import PipelineRunner


def test_pipeline_idempotency(in_memory_engine, make_csv_file):
    records = [
        {
            "customer_id": "201",
            "customer_name": "Idempotent User 1",
            "email": "user201@example.com",
            "phone": "555-0101",
            "country_code": "US",
            "customer_type_id": "1",
            "registration_date": "2024-01-01",
            "status": "ACTIVE",
            "updated_at": "2024-01-01 10:00:00",
        },
        {
            "customer_id": "202",
            "customer_name": "Idempotent User 2",
            "email": "user202@example.com",
            "phone": "555-0102",
            "country_code": "US",
            "customer_type_id": "2",
            "registration_date": "2024-01-02",
            "status": "ACTIVE",
            "updated_at": "2024-01-02 10:00:00",
        },
    ]
    csv_file = make_csv_file(records, "idempotent_test.csv")

    runner = PipelineRunner(engine=in_memory_engine)

    # First execution
    tracker1 = runner.run(csv_file)
    assert tracker1.valid_count == 2
    assert tracker1.target_inserted_count == 2
    assert tracker1.target_updated_count == 0

    with runner.engine.connect() as conn:
        from sqlalchemy import text
        count_1 = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
    assert count_1 == 2

    # Second execution (identical file)
    tracker2 = runner.run(csv_file)
    assert tracker2.valid_count == 2
    assert tracker2.target_inserted_count == 0
    assert tracker2.target_updated_count == 2

    with runner.engine.connect() as conn:
        count_2 = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()

    # Crucial assertion: Total records in target table MUST NOT duplicate
    assert count_2 == 2

