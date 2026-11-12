"""CSV Ingestion engine supporting chunked reading, empty-file detection, and lineage injection."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
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


class IngestionError(Exception):
    """Raised when ingestion encounters unrecoverable file errors."""
    pass


class CsvReader:
    """Reads CSV files with Pandas in configurable chunks and enriches rows with lineage metadata."""

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size

    def check_file_empty(self, file_path: Path) -> Tuple[bool, bool]:
        """Checks if file is completely empty or header-only.

        Returns:
            (is_completely_empty, is_header_only)
        """
        if not file_path.exists() or file_path.stat().st_size == 0:
            return True, False

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = [f.readline().strip() for _ in range(2)]
            non_empty_lines = [line for line in lines if line]
            if len(non_empty_lines) == 0:
                return True, False
            if len(non_empty_lines) == 1:
                return False, True

        return False, False

    def read_records(
        self, file_path: Path | str, run_id: str
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Reads CSV file in chunks, yielding a list of enriched record dictionaries with lineage context.

        Each yielded dictionary contains the raw parsed fields plus:
        - _source_file: string filename
        - _source_row_number: integer original line number (2 = first data row)
        - _run_id: unique pipeline execution ID
        - _ingested_at: ISO timestamp of read
        """
        path = Path(file_path)
        if not path.exists():
            raise IngestionError(f"File not found: {path}")

        is_empty, is_header_only = self.check_file_empty(path)
        if is_empty:
            logger.warning(f"Source file {path.name} is completely empty (0 bytes).")
            return
        if is_header_only:
            logger.info(f"Source file {path.name} contains only headers and no data rows.")
            return

        source_file_name = path.name
        ingested_at = datetime.utcnow().isoformat()
        current_row_num = 2  # Row 1 is header

        try:
            # We read with dtype=str (keep_default_na=False) to preserve raw payload exactly
            for chunk_df in pd.read_csv(
                path,
                dtype=str,
                keep_default_na=False,
                chunksize=self.batch_size,
                encoding="utf-8",
                on_bad_lines="warn",
            ):
                records: List[Dict[str, Any]] = []
                for _, row in chunk_df.iterrows():
                    raw_dict = row.to_dict()
                    raw_dict["_source_file"] = source_file_name
                    raw_dict["_source_row_number"] = current_row_num
                    raw_dict["_run_id"] = run_id
                    raw_dict["_ingested_at"] = ingested_at
                    records.append(raw_dict)
                    current_row_num += 1

                yield records
        except pd.errors.EmptyDataError:
            logger.warning(f"File {path.name} contained no columns or data.")
            return
        except Exception as e:
            logger.error(f"Error reading CSV {path.name}: {e}")
            raise IngestionError(f"Malformed or unreadable CSV file {path.name}: {e}") from e

