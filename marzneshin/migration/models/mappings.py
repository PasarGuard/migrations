"""
Column and table mappings between Marzneshin and Pasarguard schemas.
"""

from enum import Enum
from typing import Dict, Optional, Any, Callable


class MappingType(Enum):
    """Type of column mapping."""
    DIRECT = "direct"  # Direct 1:1 mapping
    SKIP = "skip"  # Skip this column
    TRANSFORM = "transform"  # Needs transformation
    COMPUTED = "computed"  # Computed from multiple columns


# Column mappings: {table: {source_column: (target_column, mapping_type, transform_func)}}
COLUMN_MAPPINGS: Dict[str, Dict[str, tuple]] = {
    "users": {
        # Direct mappings
        "id": ("id", MappingType.DIRECT, None),
        "username": ("username", MappingType.DIRECT, None),
        "created_at": ("created_at", MappingType.DIRECT, None),
        "data_limit": ("data_limit", MappingType.DIRECT, None),
        "used_traffic": ("used_traffic", MappingType.DIRECT, None),
        "admin_id": ("admin_id", MappingType.DIRECT, None),
        "data_limit_reset_strategy": ("data_limit_reset_strategy", MappingType.DIRECT, None),
        
        # Transform mappings
        "enabled": ("status", MappingType.TRANSFORM, "enabled_to_status"),
        "expire_date": ("expire", MappingType.DIRECT, None),
        "key": (None, MappingType.TRANSFORM, "key_to_proxy_settings"),  # Used to compute proxy_settings
        "sub_updated_at": ("edit_at", MappingType.DIRECT, None),
        
        # Skip these Marzneshin-specific fields
        "sub_last_user_agent": (None, MappingType.SKIP, None),
        "ip_limit": (None, MappingType.SKIP, None),
        "usage_duration": (None, MappingType.SKIP, None),
        "activation_deadline": (None, MappingType.SKIP, None),
        "lifetime_used_traffic": (None, MappingType.SKIP, None),
        "traffic_reset_at": (None, MappingType.SKIP, None),
        "activated": (None, MappingType.SKIP, None),
        "expire_strategy": (None, MappingType.SKIP, None),
        "removed": (None, MappingType.SKIP, None),
        "settings": (None, MappingType.SKIP, None),  # Old Marzneshin settings format
    },
    
    "admins": {
        "id": ("id", MappingType.DIRECT, None),
        "username": ("username", MappingType.DIRECT, None),
        "hashed_password": ("hashed_password", MappingType.DIRECT, None),
        "created_at": ("created_at", MappingType.DIRECT, None),
        "is_sudo": ("is_sudo", MappingType.DIRECT, None),
        "password_reset_at": ("password_reset_at", MappingType.DIRECT, None),
        "enabled": ("is_disabled", MappingType.TRANSFORM, "invert_boolean"),
        "subscription_url_prefix": ("sub_domain", MappingType.DIRECT, None),
        "all_services_access": (None, MappingType.SKIP, None),
        "modify_users_access": (None, MappingType.SKIP, None),
    },
    
    "nodes": {
        "id": ("id", MappingType.DIRECT, None),
        "name": ("name", MappingType.DIRECT, None),
        "address": ("address", MappingType.DIRECT, None),
        "port": ("port", MappingType.DIRECT, None),
        "xray_version": ("xray_version", MappingType.DIRECT, None),
        "status": ("status", MappingType.TRANSFORM, "node_status_transform"),
        "last_status_change": ("last_status_change", MappingType.DIRECT, None),
        "message": ("message", MappingType.DIRECT, None),
        "created_at": ("created_at", MappingType.DIRECT, None),
        "uplink": ("uplink", MappingType.DIRECT, None),
        "downlink": ("downlink", MappingType.DIRECT, None),
        "usage_coefficient": ("usage_coefficient", MappingType.DIRECT, None),
        "connection_backend": ("connection_type", MappingType.TRANSFORM, "connection_backend_transform"),
    },
    
    "inbounds": {
        "id": ("id", MappingType.DIRECT, None),
        "tag": ("tag", MappingType.DIRECT, None),
        # Skip Marzneshin-specific fields for Pasarguard inbounds (only need id and tag)
        "protocol": (None, MappingType.SKIP, None),
        "config": (None, MappingType.SKIP, None),  # Used for core_configs, not inbounds
        "settings": (None, MappingType.SKIP, None),
        "sniffing": (None, MappingType.SKIP, None),
        "stream_settings": (None, MappingType.SKIP, None),
        "port": (None, MappingType.SKIP, None),
        "node_id": (None, MappingType.SKIP, None),
        "created_at": (None, MappingType.SKIP, None),
        "updated_at": (None, MappingType.SKIP, None),
    },
    
    "core_configs": {
        # This is computed from inbounds, so no direct mappings
        # Handled specially in converter
    },
    
    "hosts": {
        "id": ("id", MappingType.DIRECT, None),
        "name": ("remark", MappingType.DIRECT, None),
        "address": ("address", MappingType.DIRECT, None),
        "port": ("port", MappingType.DIRECT, None),
        "path": ("path", MappingType.DIRECT, None),
        "sni": ("sni", MappingType.DIRECT, None),
        "host": ("host", MappingType.DIRECT, None),
        "security": ("security", MappingType.DIRECT, None),
        "alpn": ("alpn", MappingType.TRANSFORM, "alpn_fix_none"),
        "fingerprint": ("fingerprint", MappingType.TRANSFORM, "fingerprint_transform"),
        "allowinsecure": ("allowinsecure", MappingType.DIRECT, None),
        "is_disabled": ("is_disabled", MappingType.DIRECT, None),
        "inbound_id": ("inbound_tag", MappingType.TRANSFORM, "inbound_id_to_tag"),
        "priority": ("priority", MappingType.DIRECT, None),
        # status is not in Marzneshin, will be set to '[]' (empty array) in converter
        "status": (None, MappingType.SKIP, None),
    },
    
    "groups": {
        # Map from Marzneshin "services" table
        "id": ("id", MappingType.DIRECT, None),
        "name": ("name", MappingType.DIRECT, None),
        "description": ("name", MappingType.DIRECT, None),  # Fallback if name is missing
    },
    
    "users_groups_association": {
        # Map from Marzneshin "users_services" table
        "user_id": ("user_id", MappingType.DIRECT, None),
        "service_id": ("groups_id", MappingType.DIRECT, None),
        "created_at": (None, MappingType.SKIP, None),
        "id": (None, MappingType.SKIP, None),
    },
    
    "inbounds_groups_association": {
        # Map from Marzneshin "inbounds_services" table
        "inbound_id": ("inbound_id", MappingType.DIRECT, None),
        "service_id": ("group_id", MappingType.DIRECT, None),
    },
    
    "user_templates": {
        "id": ("id", MappingType.DIRECT, None),
        "name": ("name", MappingType.DIRECT, None),
        "data_limit": ("data_limit", MappingType.DIRECT, None),
        "expire_duration": ("expire_duration", MappingType.DIRECT, None),
        "data_limit_reset_strategy": ("data_limit_reset_strategy", MappingType.DIRECT, None),
        "username_prefix": ("username_prefix", MappingType.DIRECT, None),
        "username_suffix": ("username_suffix", MappingType.DIRECT, None),
    },
    
    "next_plans": {
        "id": ("id", MappingType.DIRECT, None),
        "user_id": ("user_id", MappingType.DIRECT, None),
        "user_template_id": ("user_template_id", MappingType.DIRECT, None),
        "data_limit": ("data_limit", MappingType.DIRECT, None),
        "expire_duration": ("expire", MappingType.DIRECT, None),
        "created_at": (None, MappingType.SKIP, None),
    },
    
    "node_user_usages": {
        "id": ("id", MappingType.DIRECT, None),
        "created_at": ("created_at", MappingType.DIRECT, None),
        "user_id": ("user_id", MappingType.DIRECT, None),
        "node_id": ("node_id", MappingType.DIRECT, None),
        "used_traffic": ("used_traffic", MappingType.DIRECT, None),
    },
    
    "node_usages": {
        "id": ("id", MappingType.DIRECT, None),
        "created_at": ("created_at", MappingType.DIRECT, None),
        "node_id": ("node_id", MappingType.DIRECT, None),
        "uplink": ("uplink", MappingType.DIRECT, None),
        "downlink": ("downlink", MappingType.DIRECT, None),
        "used_traffic": ("uplink", MappingType.DIRECT, None),  # Fallback
    },
    
    "admin_usage_logs": {
        "id": ("id", MappingType.DIRECT, None),
        "created_at": ("created_at", MappingType.DIRECT, None),
        "admin_id": ("admin_id", MappingType.DIRECT, None),
        "used_traffic": ("used_traffic", MappingType.DIRECT, None),
        "used_traffic_at_reset": ("used_traffic_at_reset", MappingType.DIRECT, None),
    },
}


# Table name mappings: {marzneshin_table: pasarguard_table}
TABLE_MAPPINGS = {
    "services": "groups",
    "users_services": "users_groups_association",
    "inbounds_services": "inbounds_groups_association",
    "service_inbounds": "inbounds_groups_association",  # Alternative name
}


def get_target_column(table: str, source_column: str) -> Optional[str]:
    """
    Get the target column name for a source column.
    
    Args:
        table: Table name
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
        table: Table name
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

