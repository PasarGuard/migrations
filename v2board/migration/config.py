"""
Configuration module for v2board to Pasarguard migration.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class V2BoardConfig:
    """v2board source configuration."""

    sql_dump_path: str


@dataclass
class SQLiteConfig:
    """SQLite database configuration."""

    db_path: str


@dataclass
class MigrationConfig:
    """Migration runtime configuration."""

    batch_size: int = 1000
    log_level: str = "INFO"
    log_file: Optional[str] = None


_script_dir = Path(__file__).parent.parent
_default_sql_dump = _script_dir.parent / "sql_multiserver_.sql"
_default_output_db = _script_dir / "output-db" / "db.sqlite3"

V2BOARD_CONFIG = V2BoardConfig(sql_dump_path=str(_default_sql_dump))
PASARGUARD_CONFIG = SQLiteConfig(db_path=str(_default_output_db))
MIGRATION_CONFIG = MigrationConfig()
