"""
Configuration module for x-ui to Pasarguard migration.
"""
from dataclasses import dataclass
from typing import Optional
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file (in x-ui directory or parent)
env_path = Path(__file__).parent.parent / '.env'
if not env_path.exists():
    env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = 'utf8mb4'


@dataclass
class SQLiteConfig:
    """SQLite database configuration."""
    db_path: str


@dataclass
class MigrationConfig:
    """Migration configuration."""
    # Migration settings
    batch_size: int = 1000
    truncate_strings: bool = True
    skip_on_error: bool = True
    
    max_usage_table_rows: int = 100000
    log_level: str = 'INFO'
    log_file: Optional[str] = None


def _get_env_required(key: str) -> str:
    """Get required environment variable or raise error."""
    value = os.getenv(key)
    if value is None:
        raise ValueError(f"Required environment variable '{key}' is not set")
    return value


def _get_env_int(key: str) -> int:
    """Get required integer environment variable or raise error."""
    value = _get_env_required(key)
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Environment variable '{key}' must be a valid integer")


def _get_env_optional(key: str, default: str = None) -> Optional[str]:
    """Get optional environment variable."""
    return os.getenv(key, default)


XUI_DB_PATH = _get_env_optional('XUI_DB_PATH', str(Path(__file__).parent.parent / 'x-ui.db'))
XUI_CONFIG = SQLiteConfig(db_path=XUI_DB_PATH)

_default_pasarguard_path = _get_env_optional(
    'PASARGUARD_DB_PATH', 
    str(Path(__file__).parent.parent / 'output-db' / 'db.sqlite3')
)
PASARGUARD_CONFIG = SQLiteConfig(db_path=_default_pasarguard_path)

MIGRATION_CONFIG = MigrationConfig()


TABLE_ORDER = [
    "admins",
    "core_configs",
    "nodes",
    "inbounds",
    "groups",
    "inbounds_groups_association",
    "hosts",
    "user_templates",
    "template_group_association",
    "users",
    "users_groups_association",
    "next_plans",
    "admin_usage_logs",
    "user_usage_logs",
    "notification_reminders",
    "user_subscription_updates",
    "node_user_usages",
    "node_usages",
    "node_stats",
]

EXCLUDE_TABLES = {
    "alembic_version",
    "django_migrations",
    "flyway_schema_history",
    "schema_migrations",
    "jwt",
    "system",
    "settings",
    "sqlite_sequence",
    "history_of_seeders",
    "inbound_client_ips",
    "outbound_traffics",
}

PASARGUARD_TABLES = {
    "admins",
    "admin_usage_logs",
    "users",
    "user_subscription_updates",
    "user_usage_logs",
    "next_plans",
    "user_templates",
    "inbounds",
    "hosts",
    "groups",
    "nodes",
    "node_user_usages",
    "node_usages",
    "node_stats",
    "notification_reminders",
    "core_configs",
    "system",
    "jwt",
    "settings",
    "inbounds_groups_association",
    "users_groups_association",
    "template_group_association",
    "alembic_version",
}

