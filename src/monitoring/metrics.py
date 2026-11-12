"""Operational metrics, run tracking, reconciliation checks, and formatted CLI reports."""

from __future__ import annotations

import logging
import time
from collections import Counter
from datetime import datetime
from typing import Dict, Optional

from sqlalchemy.orm import Session

from src.database.repositories import PipelineRunRepository

logger = logging.getLogger(__name__)


class PipelineMetricsTracker:
    """Tracks counts, execution durations, error code histograms, and run reconciliation."""

    def __init__(self, run_id: str, source_file: str, session: Optional[Session] = None):
        self.run_id = run_id
        self.source_file = source_file
        self.session = session
        self.repo = PipelineRunRepository(session) if session else None

        self.start_wall_time = time.perf_counter()
        self.start_datetime = datetime.utcnow()
        self.completed_datetime: Optional[datetime] = None
        self.processing_time_seconds: float = 0.0

        self.input_count: int = 0
        self.valid_count: int = 0
        self.rejected_count: int = 0
        self.duplicate_count: int = 0
        self.target_inserted_count: int = 0
        self.target_updated_count: int = 0

        self.error_code_counts: Counter[str] = Counter()
        self.status: str = "STARTED"
        self.error_message: Optional[str] = None

        if self.repo:
            self.repo.create_run(self.run_id, self.source_file)

    def record_input(self, count: int) -> None:
        self.input_count += count

    def record_valid(self, count: int, inserted: int = 0, updated: int = 0) -> None:
        self.valid_count += count
        self.target_inserted_count += inserted
        self.target_updated_count += updated

    def record_rejected(self, count: int, error_codes: Optional[list[str]] = None) -> None:
        self.rejected_count += count
        if error_codes:
            for code in error_codes:
                self.error_code_counts[code] += 1

    def record_duplicate(self, count: int) -> None:
        self.duplicate_count += count
        self.error_code_counts["DUPLICATE_RECORD"] += count

    def complete(self, status: Optional[str] = None, error_message: Optional[str] = None) -> None:
        """Calculates final duration, resolves final status, and commits to pipeline_runs table."""
        self.processing_time_seconds = time.perf_counter() - self.start_wall_time
        self.completed_datetime = datetime.utcnow()

        if status:
            self.status = status
        else:
            if self.rejected_count > 0 and self.valid_count > 0:
                self.status = "PARTIAL_SUCCESS"
            elif self.valid_count > 0 or self.input_count == 0:
                self.status = "SUCCESS"
            else:
                self.status = "FAILED"

        self.error_message = error_message

        if self.repo:
            self.repo.complete_run(
                run_id=self.run_id,
                status=self.status,
                input_count=self.input_count,
                valid_count=self.valid_count,
                rejected_count=self.rejected_count,
                duplicate_count=self.duplicate_count,
                processing_time_seconds=self.processing_time_seconds,
                error_message=self.error_message,
            )

    @property
    def is_reconciled(self) -> bool:
        """Verifies mathematical identity: Input = Valid + Rejected + Duplicates."""
        return self.input_count == (self.valid_count + self.rejected_count + self.duplicate_count)

    def generate_report(self) -> str:
        """Renders formatted execution and reconciliation report."""
        lines = [
            "=" * 60,
            "PIPELINE RUN REPORT",
            "=" * 60,
            f"Run ID              : {self.run_id}",
            f"Source File         : {self.source_file}",
            "",
            f"Input Records       : {self.input_count}",
            f"Valid Records       : {self.valid_count}",
            f"  - Target Inserted : {self.target_inserted_count}",
            f"  - Target Updated  : {self.target_updated_count}",
            f"Rejected Records    : {self.rejected_count}",
            f"Duplicate Records   : {self.duplicate_count}",
            "",
            f"Processing Time     : {self.processing_time_seconds:.4f} seconds",
            f"Throughput          : {(self.input_count / self.processing_time_seconds if self.processing_time_seconds > 0 else 0):.1f} records/sec",
            f"Status              : {self.status}",
        ]

        if self.error_message:
            lines.append(f"Error Message       : {self.error_message}")

        if self.error_code_counts:
            lines.extend([
                "",
                "=" * 60,
                "QUARANTINE SUMMARY",
                "=" * 60,
            ])
            for code, cnt in sorted(self.error_code_counts.items(), key=lambda x: -x[1]):
                lines.append(f"{code:<22} : {cnt}")

        lines.extend([
            "",
            "=" * 60,
            "RECONCILIATION",
            "=" * 60,
            "Equation: Input = Valid + Rejected + Duplicate",
            f"{self.input_count} = {self.valid_count} + {self.rejected_count} + {self.duplicate_count}",
            f"Result: {'PASS' if self.is_reconciled else 'FAIL (DISCREPANCY DETECTED)'}",
            "=" * 60,
        ])
        return "\n".join(lines)
