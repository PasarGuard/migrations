"""
Models and mappings for x-ui migration.
"""

from .mappings import (
    get_target_column,
    get_mapping_info,
    get_target_table,
    COLUMN_MAPPINGS,
    TABLE_MAPPINGS,
    MappingType
)
from .schemas import (
    get_pasarguard_schema,
    get_column_info,
    table_exists,
    get_foreign_keys,
    get_primary_key,
    get_unique_constraints
)

__all__ = [
    'get_target_column',
    'get_mapping_info',
    'get_target_table',
    'COLUMN_MAPPINGS',
    'TABLE_MAPPINGS',
    'MappingType',
    'get_pasarguard_schema',
    'get_column_info',
    'table_exists',
    'get_foreign_keys',
    'get_primary_key',
    'get_unique_constraints',
]

