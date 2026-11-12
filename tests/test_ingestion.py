"""Tests for CSV ingestion engine and lineage enrichment."""

from __future__ import annotations

from pathlib import Path
import pytest

from src.ingestion.csv_reader import CsvReader, IngestionError


def test_ingestion_valid_csv(make_csv_file):
    reader = CsvReader(batch_size=10)
    data = [
        {
            "customer_id": "1",
            "customer_name": "Alice Smith",
            "email": "alice@example.com",
            "phone": "123-456-7890",
            "country_code": "US",
            "customer_type_id": "1",
            "registration_date": "2024-01-01",
            "status": "ACTIVE",
            "updated_at": "2024-01-01 10:00:00",
        },
        {
            "customer_id": "2",
            "customer_name": "Bob Jones",
            "email": "bob@example.com",
            "phone": "",
            "country_code": "GB",
            "customer_type_id": "2",
            "registration_date": "2024-01-02",
            "status": "INACTIVE",
            "updated_at": "",
        },
    ]
    csv_path = make_csv_file(data, "customers.csv")
    chunks = list(reader.read_records(csv_path, run_id="TEST-RUN-1"))

    assert len(chunks) == 1
    batch = chunks[0]
    assert len(batch) == 2

    # Verify lineage metadata injection
    assert batch[0]["_source_file"] == "customers.csv"
    assert batch[0]["_source_row_number"] == 2  # Line 2 (header is 1)
    assert batch[0]["_run_id"] == "TEST-RUN-1"
    assert batch[1]["_source_row_number"] == 3


def test_ingestion_empty_file(tmp_path: Path):
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("")

    reader = CsvReader()
    is_empty, is_header = reader.check_file_empty(empty_file)
    assert is_empty is True

    records = list(reader.read_records(empty_file, run_id="RUN-EMPTY"))
    assert len(records) == 0


def test_ingestion_header_only_file(tmp_path: Path):
    header_file = tmp_path / "header_only.csv"
    header_file.write_text("customer_id,customer_name,email,customer_type_id,registration_date,status\n")

    reader = CsvReader()
    is_empty, is_header = reader.check_file_empty(header_file)
    assert is_empty is False
    assert is_header is True

    records = list(reader.read_records(header_file, run_id="RUN-HEADER"))
    assert len(records) == 0


def test_ingestion_missing_file_raises_error():
    reader = CsvReader()
    with pytest.raises(IngestionError, match="File not found"):
        list(reader.read_records(Path("non_existent_file.csv"), run_id="RUN-ERR"))

