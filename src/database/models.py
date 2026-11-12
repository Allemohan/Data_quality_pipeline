"""SQLAlchemy ORM models for Target, Quarantine, Reference, and Audit tables."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base declarative class for all models."""
    pass


class CustomerType(Base):
    """Reference table for customer types to enforce referential integrity."""

    __tablename__ = "customer_types"

    customer_type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    type_name: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    # Relationships
    customers: Mapped[list["Customer"]] = relationship(
        "Customer", back_populates="customer_type"
    )

    def __repr__(self) -> str:
        return f"<CustomerType(id={self.customer_type_id}, name='{self.type_name}')>"


class Customer(Base):
    """Target production table for valid ingested customers."""

    __tablename__ = "customers"

    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    customer_name: Mapped[str] = mapped_column(String(100), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    country_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    customer_type_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("customer_types.customer_type_id"), nullable=False
    )
    registration_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    # Lineage columns
    source_file: Mapped[str] = mapped_column(String(255), nullable=False)
    source_row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    run_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    customer_type: Mapped["CustomerType"] = relationship(
        "CustomerType", back_populates="customers"
    )

    def __repr__(self) -> str:
        return f"<Customer(id={self.customer_id}, name='{self.customer_name}', email='{self.email}')>"


class QuarantineRecord(Base):
    """Quarantine table for storing rejected records with lossless context and error reasons."""

    __tablename__ = "quarantine_records"

    quarantine_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    source_record_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True
    )
    source_file: Mapped[str] = mapped_column(String(255), nullable=False)
    source_row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    run_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    raw_payload: Mapped[str] = mapped_column(Text, nullable=False)
    error_code: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    validation_stage: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="QUARANTINED", index=True
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    corrected_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    replayed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    replay_runs: Mapped[list["ReplayRun"]] = relationship(
        "ReplayRun", back_populates="quarantine_record"
    )

    def __repr__(self) -> str:
        return f"<QuarantineRecord(id={self.quarantine_id}, code='{self.error_code}', status='{self.status}')>"


class PipelineRun(Base):
    """Operational audit table tracking overall pipeline execution batches."""

    __tablename__ = "pipeline_runs"

    run_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    source_file: Mapped[str] = mapped_column(String(255), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    input_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    valid_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processing_time_seconds: Mapped[float] = mapped_column(
        Numeric(10, 4), nullable=False, default=0.0
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<PipelineRun(id='{self.run_id}', status='{self.status}', in={self.input_count}, valid={self.valid_count})>"


class ReplayRun(Base):
    """Detailed audit history of quarantine replay attempts."""

    __tablename__ = "replay_runs"

    replay_run_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    quarantine_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("quarantine_records.quarantine_id"), nullable=False, index=True
    )
    original_run_id: Mapped[str] = mapped_column(String(100), nullable=False)
    replay_started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    replay_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    old_error: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    new_error: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    target_record_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Relationships
    quarantine_record: Mapped["QuarantineRecord"] = relationship(
        "QuarantineRecord", back_populates="replay_runs"
    )

    def __repr__(self) -> str:
        return f"<ReplayRun(id={self.replay_run_id}, quarantine_id={self.quarantine_id}, status='{self.status}')>"

