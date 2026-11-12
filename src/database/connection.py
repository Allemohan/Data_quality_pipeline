"""Database connection, engine factory, and schema initialization."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config.settings import get_settings
from src.database.models import Base, CustomerType

logger = logging.getLogger(__name__)

_ENGINE: Optional[Engine] = None
_SESSION_FACTORY: Optional[sessionmaker] = None


def get_engine(db_url: Optional[str] = None) -> Engine:
    """Creates or returns the cached SQLAlchemy engine."""
    global _ENGINE
    if _ENGINE is not None and db_url is None:
        return _ENGINE

    settings = get_settings()
    url = db_url or settings.db.url

    # Handle SQLite specific dialect settings (e.g. check_same_thread)
    is_sqlite = url.startswith("sqlite")
    connect_args = {"check_same_thread": False} if is_sqlite else {}

    engine_kwargs = {
        "echo": settings.db.echo_sql,
        "connect_args": connect_args,
    }

    # Connection pooling only applies to non-SQLite databases
    if not is_sqlite:
        engine_kwargs["pool_size"] = settings.db.pool_size
        engine_kwargs["max_overflow"] = settings.db.max_overflow

    engine = create_engine(url, **engine_kwargs)

    # Enable foreign keys for SQLite
    if is_sqlite:
        from sqlalchemy import event

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    if db_url is None:
        _ENGINE = engine

    return engine


def get_session_factory(engine: Optional[Engine] = None) -> sessionmaker:
    """Returns a configured SQLAlchemy sessionmaker."""
    global _SESSION_FACTORY
    if _SESSION_FACTORY is not None and engine is None:
        return _SESSION_FACTORY

    eng = engine or get_engine()
    factory = sessionmaker(bind=eng, autoflush=False, autocommit=False)

    if engine is None:
        _SESSION_FACTORY = factory

    return factory


@contextmanager
def get_db_session(engine: Optional[Engine] = None) -> Generator[Session, None, None]:
    """Context manager for transactional database sessions."""
    factory = get_session_factory(engine)
    session: Session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def seed_reference_data(session: Session) -> None:
    """Seeds the customer_types reference table if empty."""
    default_types = [
        {"customer_type_id": 1, "type_name": "INDIVIDUAL", "description": "Retail individual consumer accounts", "is_active": True},
        {"customer_type_id": 2, "type_name": "BUSINESS", "description": "Small and medium business accounts", "is_active": True},
        {"customer_type_id": 3, "type_name": "ENTERPRISE", "description": "Large enterprise contractual accounts", "is_active": True},
        {"customer_type_id": 4, "type_name": "NON_PROFIT", "description": "Registered charities and educational non-profit entities", "is_active": True},
        {"customer_type_id": 5, "type_name": "INTERNAL_TEST", "description": "Internal staff and testing accounts", "is_active": True},
    ]

    for item in default_types:
        existing = session.query(CustomerType).filter_by(customer_type_id=item["customer_type_id"]).first()
        if not existing:
            session.add(CustomerType(**item))
    session.commit()
    logger.info("Reference data (customer_types) seeded successfully.")


def init_db(engine: Optional[Engine] = None) -> None:
    """Initializes the database schema and seeds reference data."""
    eng = engine or get_engine()
    Base.metadata.create_all(bind=eng)
    with get_db_session(eng) as session:
        seed_reference_data(session)
    logger.info("Database schema initialized and ready.")
