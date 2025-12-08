"""
Column and table mappings between x-ui and Pasarguard schemas.
"""

from enum import Enum
from typing import Dict, Optional, Any

"""
X-UI to Pasarguard Mapping Strategy:

1. x-ui.inbounds -> pasarguard.inbounds + pasarguard.core_configs
2. x-ui.client_traffics -> pasarguard.users (client users)
"""


class MappingType(Enum):
    """Type of column mapping."""
    DIRECT = "direct"  # Direct 1:1 mapping
    SKIP = "skip"  # Skip this column
    TRANSFORM = "transform"  # Needs transformation
    COMPUTED = "computed"  # Computed from multiple columns


# Column mappings: {xui_table: {source_column: (target_column, mapping_type, transform_func)}}
COLUMN_MAPPINGS: Dict[str, Dict[str, tuple]] = {
    "inbounds": {
        "id": ("id", MappingType.DIRECT, None),
        "tag": ("tag", MappingType.DIRECT, None),
        "settings": (None, MappingType.SKIP, None),
        "stream_settings": (None, MappingType.SKIP, None),
        "sniffing": (None, MappingType.SKIP, None),
        "protocol": (None, MappingType.SKIP, None),
        "port": (None, MappingType.SKIP, None),
        "listen": (None, MappingType.SKIP, None),
        "remark": (None, MappingType.SKIP, None),
        "enable": (None, MappingType.SKIP, None),
        "allocate": (None, MappingType.SKIP, None),
        "user_id": (None, MappingType.SKIP, None),
        "up": (None, MappingType.SKIP, None),
        "down": (None, MappingType.SKIP, None),
        "total": (None, MappingType.SKIP, None),
        "all_time": (None, MappingType.SKIP, None),
        "expiry_time": (None, MappingType.SKIP, None),
        "traffic_reset": (None, MappingType.SKIP, None),
        "last_traffic_reset_time": (None, MappingType.SKIP, None),
    },
    
    "client_traffics": {
        "id": ("id", MappingType.DIRECT, None),
        "email": ("username", MappingType.DIRECT, None),
        "inbound_id": (None, MappingType.SKIP, None),
        "enable": ("status", MappingType.TRANSFORM, "enable_to_status"),
        "up": (None, MappingType.SKIP, None),
        "down": (None, MappingType.SKIP, None),
        "all_time": (None, MappingType.SKIP, None),
        "expiry_time": ("expire", MappingType.TRANSFORM, "expiry_time_to_expire"),
        "total": (None, MappingType.SKIP, None),
        "reset": (None, MappingType.SKIP, None),
        "last_online": ("online_at", MappingType.TRANSFORM, "last_online_to_online_at"),
    },
}


# Table name mappings: {xui_table: pasarguard_table}
TABLE_MAPPINGS = {
    "client_traffics": "users",
}


def get_target_column(table: str, source_column: str) -> Optional[str]:
    """
    Get the target column name for a source column.
    
    Args:
        table: Source table name
        source_column: Source column name
        
    Returns:
        Target column name or None if should be skipped
    """
    if table not in COLUMN_MAPPINGS:
        # If no mapping defined, assume direct mapping
        return source_column
    
    if source_column not in COLUMN_MAPPINGS[table]:
        # If column not in mapping, assume direct mapping
        return source_column
    
    target_col, mapping_type, _ = COLUMN_MAPPINGS[table][source_column]
    
    if mapping_type == MappingType.SKIP:
        return None
    
    return target_col or source_column


def get_mapping_info(table: str, source_column: str) -> tuple:
    """
    Get complete mapping information for a column.
    
    Args:
        table: Source table name
        source_column: Source column name
        
    Returns:
        Tuple of (target_column, mapping_type, transform_function)
    """
    if table not in COLUMN_MAPPINGS:
        return (source_column, MappingType.DIRECT, None)
    
    if source_column not in COLUMN_MAPPINGS[table]:
        return (source_column, MappingType.DIRECT, None)
    
    return COLUMN_MAPPINGS[table][source_column]


def get_target_table(source_table: str) -> str:
    """
    Get target table name from source table name.
    
    Args:
        source_table: Source table name
        
    Returns:
        Target table name
    """
    return TABLE_MAPPINGS.get(source_table, source_table)

