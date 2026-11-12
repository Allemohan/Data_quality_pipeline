"""Tests for CLI subcommands and configuration loading."""

from __future__ import annotations

import sys
from unittest.mock import patch
import pytest

from src.config.settings import Settings, get_settings
from src.main import handle_ingest, handle_reconcile, handle_replay, main


def test_settings_loading():
    settings = get_settings()
    assert settings.app_name == "data-quality-pipeline"
    assert "ACTIVE" in settings.validation.allowed_statuses
    assert settings.paths.input_dir.exists()


def test_cli_ingest_and_reconcile(tmp_path, make_csv_file):
    records = [
        {
            "customer_id": "801",
            "customer_name": "CLI User",
            "email": "cli@example.com",
            "phone": "555-1234",
            "country_code": "US",
            "customer_type_id": "1",
            "registration_date": "2024-01-01",
            "status": "ACTIVE",
            "updated_at": "2024-01-01 10:00:00",
        }
    ]
    csv_file = make_csv_file(records, "cli_test.csv")
    sqlite_db = tmp_path / "cli_test.db"
    db_url = f"sqlite:///{sqlite_db}"

    # Ingest via CLI handler
    test_args = ["main.py", "--db-url", db_url, "ingest", "--input", str(csv_file)]
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit) as exit_info:
            main()
        assert exit_info.value.code == 0

    # Reconcile via CLI handler
    test_args_reconcile = ["main.py", "--db-url", db_url, "reconcile"]
    with patch.object(sys, "argv", test_args_reconcile):
        with pytest.raises(SystemExit) as exit_info:
            main()
        assert exit_info.value.code == 0

    # Replay via CLI handler
    test_args_replay = ["main.py", "--db-url", db_url, "replay"]
    with patch.object(sys, "argv", test_args_replay):
        with pytest.raises(SystemExit) as exit_info:
            main()
        assert exit_info.value.code == 0
