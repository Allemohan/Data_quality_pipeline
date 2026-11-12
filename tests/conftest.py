"""Pytest fixtures and test harnesses for Data Quality Pipeline tests."""

from __future__ import annotations

import csv
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.database.connection import init_db
from src.database.models import Base, CustomerType


@pytest.fixture(scope="function")
def in_memory_engine() -> Generator[Engine, None, None]:
    """Provides an isolated in-memory SQLite database engine for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    init_db(engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(in_memory_engine: Engine) -> Generator[Session, None, None]:
    """Provides an active database session for a test function."""
    factory = sessionmaker(bind=in_memory_engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@pytest.fixture
def make_csv_file(tmp_path: Path):
    """Factory fixture to create temporary CSV files for ingestion tests."""
    def _create_csv(records: List[Dict[str, Any]], filename: str = "test.csv") -> Path:
        file_path = tmp_path / filename
        fieldnames = [
            "customer_id",
            "customer_name",
            "email",
            "phone",
            "country_code",
            "customer_type_id",
            "registration_date",
            "status",
            "updated_at",
        ]
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        return file_path

    return _create_csv
