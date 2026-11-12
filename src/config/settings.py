"""Application configuration and settings management."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

import yaml
from dotenv import load_dotenv

# Base project directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Load .env if present
load_dotenv(BASE_DIR / ".env")


class DatabaseSettings:
    def __init__(self, url: Optional[str] = None):
        self.url = url or os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/data_pipeline",
        )
        self.echo_sql = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
        self.pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
        self.max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "20"))


class ValidationSettings:
    def __init__(self, allowed_statuses: Optional[List[str]] = None):
        self.allowed_statuses = allowed_statuses or ["ACTIVE", "INACTIVE", "SUSPENDED"]
        self.date_format = "%Y-%m-%d"
        self.timestamp_format = "%Y-%m-%d %H:%M:%S"


class PathSettings:
    def __init__(self):
        self.base_dir = BASE_DIR
        self.data_dir = BASE_DIR / "data"
        self.input_dir = self.data_dir / "input"
        self.expected_dir = self.data_dir / "expected"
        self.output_dir = self.data_dir / "output"
        self.quarantine_dir = self.data_dir / "quarantine"
        self.sql_dir = BASE_DIR / "sql"

    def ensure_directories(self) -> None:
        """Ensure all required runtime data directories exist."""
        for path in [
            self.input_dir,
            self.expected_dir,
            self.output_dir,
            self.quarantine_dir,
            self.sql_dir,
        ]:
            path.mkdir(parents=True, exist_ok=True)


class Settings:
    """Master application configuration container."""

    def __init__(self):
        self.app_name = "data-quality-pipeline"
        self.version = "1.0.0"
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.batch_size = int(os.getenv("BATCH_SIZE", "1000"))
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.max_replay_retries = int(os.getenv("MAX_REPLAY_RETRIES", "3"))

        self.db = DatabaseSettings()
        self.validation = ValidationSettings()
        self.paths = PathSettings()

        # Load YAML config overrides if file exists
        config_file = BASE_DIR / "config" / "config.yaml"
        if config_file.exists():
            self._load_from_yaml(config_file)

    def _load_from_yaml(self, path: Path) -> None:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            if "app" in data:
                self.app_name = data["app"].get("name", self.app_name)
                self.version = data["app"].get("version", self.version)
                self.environment = data["app"].get("environment", self.environment)

            if "pipeline" in data:
                self.batch_size = int(data["pipeline"].get("batch_size", self.batch_size))
                self.max_replay_retries = int(
                    data["pipeline"].get("max_replay_retries", self.max_replay_retries)
                )

            if "validation" in data:
                if "allowed_statuses" in data["validation"]:
                    self.validation.allowed_statuses = data["validation"]["allowed_statuses"]
                if "date_format" in data["validation"]:
                    self.validation.date_format = data["validation"]["date_format"]
                if "timestamp_format" in data["validation"]:
                    self.validation.timestamp_format = data["validation"]["timestamp_format"]
        except Exception:
            # Fall back to default settings on YAML parse issue
            pass


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton getter for application settings."""
    settings = Settings()
    settings.paths.ensure_directories()
    return settings

