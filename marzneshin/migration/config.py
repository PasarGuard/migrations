"""
Configuration module for Marzneshin to Pasarguard migration.
"""
from dataclasses import dataclass
from typing import Optional
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file (in marzneshin directory)
env_path = Path(__file__).parent.parent / '.env'
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
class MigrationConfig:
    """Migration configuration."""
    # Migration settings
    batch_size: int = 1000
    truncate_strings: bool = True
    skip_on_error: bool = True
    
    # Large table handling
    # Maximum rows to extract from usage/log tables (0 = no limit)
    max_usage_table_rows: int = 100000  # Limit usage tables to 100k most recent rows
    
    # Alembic version settings
    # Set this to the latest PasarGuard migration revision after successful migration
    # This tells Alembic that the database schema is up-to-date
    set_alembic_version: bool = True
    alembic_version: str = '5943013d0e49'  # Latest PasarGuard migration
    
    # Logging
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


# Default configurations (must be set via environment variables)
MARZNESHIN_CONFIG = DatabaseConfig(
    host=_get_env_required('MARZNESHIN_HOST'),
    port=_get_env_int('MARZNESHIN_PORT'),
    user=_get_env_required('MARZNESHIN_USER'),
    password=_get_env_required('MARZNESHIN_PASSWORD'),
    database=_get_env_required('MARZNESHIN_DB'),
)

PASARGUARD_CONFIG = DatabaseConfig(
    host=_get_env_required('PASARGUARD_HOST'),
    port=_get_env_int('PASARGUARD_PORT'),
    user=_get_env_required('PASARGUARD_USER'),
    password=_get_env_required('PASARGUARD_PASSWORD'),
    database=_get_env_required('PASARGUARD_DB'),
)

MIGRATION_CONFIG = MigrationConfig()


# Table migration order (respecting foreign key dependencies)
TABLE_ORDER = [
    # Core tables (no dependencies)
    "admins",
    "core_configs",
    "nodes",
    "inbounds",
    "groups",
    
    # Association tables
    "inbounds_groups_association",
    
    # Dependent tables
    "hosts",
    "user_templates",
    "template_group_association",
    
    # User tables
    "users",
    "users_groups_association",
    "next_plans",
    
    # Usage and log tables
    "admin_usage_logs",
    "user_usage_logs",
    "notification_reminders",
    "user_subscription_updates",
    "node_user_usages",
    "node_usages",
    "node_stats",
]

# Tables to exclude from migration
EXCLUDE_TABLES = {
    "alembic_version",
    "django_migrations",
    "flyway_schema_history",
    "schema_migrations",
    "jwt",  # Pasarguard-specific
    "system",  # Pasarguard-specific
    "settings",  # Pasarguard-specific
}

# Complete list of valid PasarGuard tables
# Any table not in this list will be dropped after migration
PASARGUARD_TABLES = {
    # Core tables
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
    
    # Association tables
    "inbounds_groups_association",
    "users_groups_association",
    "template_group_association",
    
    # System tables
    "alembic_version",
}

