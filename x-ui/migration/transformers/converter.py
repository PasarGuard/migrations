"""
Data converter for transforming x-ui data to Pasarguard format.
"""

import json
import logging
import uuid
import hashlib
import secrets
import string
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from ..models.mappings import get_mapping_info, get_target_table, MappingType

logger = logging.getLogger(__name__)


class DataConverter:
    """Convert x-ui data to Pasarguard format."""
    
    def __init__(self):
        """Initialize converter."""
        self.email_to_uuid_map: Dict[str, str] = {}
        self.email_to_password_map: Dict[str, str] = {}
        self._uuid_map_built = False
    
    def convert_table(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        target_columns: Dict[str, Any],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        target_table: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Convert table data from x-ui to Pasarguard format.
        
        Args:
            table: Source table name
            rows: Source rows
            target_columns: Target table column information
            all_data: All source data (for lookups)
            target_table: Target table name
            
        Returns:
            Converted rows
        """
        if target_table is None:
            target_table = get_target_table(table)
        
        # Build UUID map from inbound settings if converting users
        if not self._uuid_map_built and target_table == "users" and all_data:
            self._build_uuid_map_from_inbounds(all_data)
        
        # Special handling: create single core_config from all inbounds
        if target_table == "core_configs" and all_data and "inbounds" in all_data:
            inbound_count = len(all_data['inbounds'])
            logger.info(f"Creating single core_config containing all {inbound_count} inbounds")
            core_config = self._convert_all_inbounds_to_core_config(all_data['inbounds'], target_columns)
            if core_config:
                logger.info(f"✓ Created single core_config '{core_config.get('name', 'unknown')}' with {inbound_count} inbounds")
                return [core_config]
            return []
        
        logger.info(f"Converting {len(rows)} rows from {table} to {target_table}")
        
        converted_rows = []
        for idx, row in enumerate(rows, 1):
            try:
                converted_row = self._convert_row(table, row, target_columns, all_data, target_table)
                if converted_row:
                    converted_rows.append(converted_row)
            except Exception as e:
                logger.error(f"Error converting row {idx} in {table}: {e}")
                continue
        
        logger.info(f"Successfully converted {len(converted_rows)}/{len(rows)} rows")
        return converted_rows
    
    def _build_uuid_map_from_inbounds(self, all_data: Dict[str, List[Dict[str, Any]]]):
        """Build email to UUID and password mapping from inbound settings."""
        if 'inbounds' not in all_data:
            return
        
        for inbound in all_data['inbounds']:
            settings_str = inbound.get("settings", "{}")
            try:
                settings = json.loads(settings_str) if isinstance(settings_str, str) else settings_str
                clients = settings.get("clients", [])
                for client in clients:
                    email = client.get("email")
                    client_id = client.get("id")
                    password = client.get("password", "")
                    
                    if email and client_id:
                        self.email_to_uuid_map[email] = client_id
                    
                    # Store password if it exists and is not empty
                    if email and password:
                        self.email_to_password_map[email] = password
            except Exception as e:
                logger.debug(f"Failed to parse inbound settings for UUID/password mapping: {e}")
                continue
        
        self._uuid_map_built = True
        logger.info(f"Built UUID map with {len(self.email_to_uuid_map)} entries from inbound settings")
        logger.info(f"Found {len(self.email_to_password_map)} passwords from inbound settings")
    
    def _convert_row(
        self,
        source_table: str,
        row: Dict[str, Any],
        target_columns: Dict[str, Any],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]],
        target_table: str
    ) -> Optional[Dict[str, Any]]:
        """Convert a single row."""
        converted = {}
        
        # Special handling for core_configs from inbounds - handled separately
        if target_table == "core_configs":
            # This should not be called per-row for core_configs
            # core_configs are created from all inbounds together
            return None
        
        # Convert each column
        for source_col, source_value in row.items():
            target_col, mapping_type, transform_func = get_mapping_info(source_table, source_col)
            
            if mapping_type == MappingType.SKIP:
                continue
            
            if target_col:
                # Only include columns that exist in target table schema
                if target_col not in target_columns:
                    logger.debug(f"Skipping column {target_col} (not in target table {target_table})")
                    continue
                
                # Apply transformation if needed
                if mapping_type == MappingType.TRANSFORM and transform_func:
                    converted_value = self._apply_transform(transform_func, source_value, row)
                else:
                    converted_value = source_value
                
                # Set the value (including None - let _add_default_values handle None defaults if needed)
                converted[target_col] = converted_value
        
        # Add default values for required columns
        converted = self._add_default_values(target_table, converted, target_columns, row, all_data)
        
        return converted
    
    def _apply_transform(self, transform_func: str, value: Any, row: Dict[str, Any]) -> Any:
        """Apply transformation function."""
        if transform_func == "enable_to_status":
            enable = value
            expiry_time = row.get("expiry_time", 0)
            # x-ui stores expiry_time in milliseconds; convert to seconds
            if expiry_time and expiry_time > 1e10:
                expiry_time = expiry_time / 1000.0
            data_limit = row.get("total", 0)  # x-ui uses 'total' for data limit
            used_traffic = (row.get("up", 0) or 0) + (row.get("down", 0) or 0)
            
            # Don't set disabled - use expired or limited instead based on x-ui data
            now_ts = datetime.now(timezone.utc).timestamp()
            
            # Check expiry first
            if expiry_time and expiry_time > 0 and expiry_time < now_ts:
                return "expired"
            
            # Check data limit
            if data_limit and data_limit > 0 and used_traffic >= data_limit:
                return "limited"
            
            # If disabled in x-ui, check if expired or limited, otherwise set as expired
            if enable != 1:
                # If expired, return expired; if limited, return limited; otherwise expired
                if expiry_time and expiry_time > 0 and expiry_time < now_ts:
                    return "expired"
                elif data_limit and data_limit > 0 and used_traffic >= data_limit:
                    return "limited"
                else:
                    return "expired"  # Default to expired if disabled
            
            return "active"
        
        elif transform_func == "expiry_time_to_expire":
            if value and value > 0:
                try:
                    # x-ui stores expiry_time in milliseconds; convert to seconds
                    timestamp_seconds = value / 1000.0 if value > 1e10 else value
                    return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                except (ValueError, OSError):
                    return None
            return None
        
        elif transform_func == "last_online_to_online_at":
            if value is not None and value > 0:
                try:
                    # x-ui stores last_online in milliseconds, convert to seconds
                    timestamp_seconds = value / 1000.0 if value > 1e10 else value
                    result = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                    logger.debug(f"Converted last_online {value} (ms) to {result}")
                    return result
                except (ValueError, OSError) as e:
                    logger.warning(f"Failed to convert last_online timestamp {value}: {e}")
                    return None
            return None
        
        elif transform_func == "invert_boolean":
            return not bool(value) if value is not None else True
        
        return value
    
    def _add_default_values(
        self,
        target_table: str,
        converted: Dict[str, Any],
        target_columns: Dict[str, Any],
        source_row: Dict[str, Any],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]]
    ) -> Dict[str, Any]:
        """Add default values for required columns."""
        now = datetime.now(timezone.utc)
        
        if target_table == "admins":
            if "created_at" not in converted:
                converted["created_at"] = now
            if "is_sudo" not in converted:
                converted["is_sudo"] = True
            if "is_disabled" not in converted:
                converted["is_disabled"] = False
            if "used_traffic" not in converted:
                converted["used_traffic"] = 0
            if "password_reset_at" not in converted:
                converted["password_reset_at"] = None
            if "sub_domain" not in converted:
                converted["sub_domain"] = None
            if "telegram_id" not in converted:
                converted["telegram_id"] = None
            if "discord_id" not in converted:
                converted["discord_id"] = None
            if "discord_webhook" not in converted:
                converted["discord_webhook"] = None
            if "sub_template" not in converted:
                converted["sub_template"] = None
            if "profile_title" not in converted:
                converted["profile_title"] = None
            if "support_url" not in converted:
                converted["support_url"] = None
            if "notification_enable" not in converted:
                converted["notification_enable"] = "{}"
        
        elif target_table == "users":
            if "created_at" not in converted:
                converted["created_at"] = now
            if "used_traffic" not in converted:
                up = source_row.get("up", 0) or 0
                down = source_row.get("down", 0) or 0
                converted["used_traffic"] = int(up + down)
            if "status" not in converted:
                enable = source_row.get("enable", 1)
                expiry_time = source_row.get("expiry_time", 0)
                # x-ui stores expiry_time in milliseconds; convert to seconds
                if expiry_time and expiry_time > 1e10:
                    expiry_time = expiry_time / 1000.0
                data_limit = source_row.get("total", 0)
                used_traffic = converted.get("used_traffic", 0)
                
                # Don't set disabled - use expired or limited instead based on x-ui data
                now_ts = datetime.now(timezone.utc).timestamp()
                
                # Check expiry first
                if expiry_time and expiry_time > 0 and expiry_time < now_ts:
                    converted["status"] = "expired"
                # Check data limit
                elif data_limit and data_limit > 0 and used_traffic >= data_limit:
                    converted["status"] = "limited"
                # If disabled in x-ui, check if expired or limited, otherwise set as expired
                elif enable != 1:
                    # If expired, return expired; if limited, return limited; otherwise expired
                    if expiry_time and expiry_time > 0 and expiry_time < now_ts:
                        converted["status"] = "expired"
                    elif data_limit and data_limit > 0 and used_traffic >= data_limit:
                        converted["status"] = "limited"
                    else:
                        converted["status"] = "expired"  # Default to expired if disabled
                else:
                    converted["status"] = "active"
            if "data_limit" not in converted:
                total = source_row.get("total", 0)
                converted["data_limit"] = total if total and total > 0 else None
            if "admin_id" not in converted:
                if all_data and 'users' in all_data and all_data['users']:
                    first_admin_id = all_data['users'][0].get('id', 1)
                    converted["admin_id"] = first_admin_id
                else:
                    converted["admin_id"] = 1
            if "data_limit_reset_strategy" not in converted:
                converted["data_limit_reset_strategy"] = "no_reset"
            if "proxy_settings" not in converted:
                # Try to get UUID from inbound settings first (original x-ui UUID)
                email = source_row.get("email") or converted.get("username")
                original_uuid = None
                if email and email in self.email_to_uuid_map:
                    original_uuid = self.email_to_uuid_map[email]
                    logger.debug(f"Found original UUID for {email}: {original_uuid}")
                
                if original_uuid:
                    # Use original UUID from x-ui inbound settings
                    converted["proxy_settings"] = self._generate_proxy_settings_with_uuid(original_uuid, email)
                else:
                    # Fallback: Generate consistent UUID based on x-ui user ID
                    user_id = source_row.get("id")
                    if user_id:
                        converted["proxy_settings"] = self._generate_proxy_settings(f"xui_user_{user_id}")
                    else:
                        # Last resort: use username/email
                        username = converted.get("username") or source_row.get("email") or ""
                        converted["proxy_settings"] = self._generate_proxy_settings(str(username))
            if "edit_at" not in converted:
                converted["edit_at"] = None
            if "expire" not in converted:
                converted["expire"] = None
            if "sub_revoked_at" not in converted:
                converted["sub_revoked_at"] = None
            if "note" not in converted:
                converted["note"] = None
            if "online_at" not in converted:
                last_online = source_row.get("last_online", 0)
                if last_online and last_online > 0:
                    try:
                        # x-ui stores last_online in milliseconds, convert to seconds
                        timestamp_seconds = last_online / 1000.0 if last_online > 1e10 else last_online
                        converted["online_at"] = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                    except (ValueError, OSError):
                        converted["online_at"] = None
                else:
                    converted["online_at"] = None
            elif converted.get("online_at") is None:
                last_online = source_row.get("last_online", 0)
                if last_online and last_online > 0:
                    try:
                        # x-ui stores last_online in milliseconds, convert to seconds
                        timestamp_seconds = last_online / 1000.0 if last_online > 1e10 else last_online
                        converted["online_at"] = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                    except (ValueError, OSError):
                        pass
            if "on_hold_timeout" not in converted:
                converted["on_hold_timeout"] = None
            if "on_hold_expire_duration" not in converted:
                converted["on_hold_expire_duration"] = None
            if "auto_delete_in_days" not in converted:
                converted["auto_delete_in_days"] = None
            if "last_status_change" not in converted:
                converted["last_status_change"] = None
        
        elif target_table == "inbounds":
            pass
        
        elif target_table == "core_configs":
            pass
        
        return converted
    
    def _convert_all_inbounds_to_core_config(
        self,
        inbound_rows: List[Dict[str, Any]],
        target_columns: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Convert all x-ui inbounds into a single pasarguard core_config."""
        if not inbound_rows:
            return None
        
        # Build inbounds array from all inbound rows
        inbounds_array = []
        
        for inbound_row in inbound_rows:
            # Extract settings from inbound
            settings_str = inbound_row.get("settings", "{}")
            stream_settings_str = inbound_row.get("stream_settings", "{}")
            sniffing_str = inbound_row.get("sniffing", "{}")
            
            try:
                settings = json.loads(settings_str) if isinstance(settings_str, str) else settings_str
                stream_settings = json.loads(stream_settings_str) if isinstance(stream_settings_str, str) else stream_settings_str
                sniffing = json.loads(sniffing_str) if isinstance(sniffing_str, str) else sniffing_str
            except:
                settings = {}
                stream_settings = {}
                sniffing = {}
            
            # Ensure clients is set to empty array in settings
            if isinstance(settings, dict):
                settings["clients"] = []
            
            # Remove external proxy and TLS settings from streamSettings if present
            if isinstance(stream_settings, dict):
                # Remove proxy-related settings
                stream_settings.pop("proxySettings", None)
                stream_settings.pop("sockopt", None)
                # Remove external proxy
                if "externalProxy" in stream_settings:
                    stream_settings.pop("externalProxy")
                    logger.debug(f"Removed externalProxy from inbound {tag}")
                # Remove TLS settings (certificate files won't exist on new system)
                if "tlsSettings" in stream_settings:
                    stream_settings.pop("tlsSettings")
                    logger.debug(f"Removed tlsSettings from inbound {tag}")
                # Remove security field (TLS indicator)
                if "security" in stream_settings:
                    stream_settings.pop("security")
                    logger.debug(f"Removed security field from inbound {tag}")
            
            tag = inbound_row.get("tag", f"inbound-{inbound_row.get('id', 'unknown')}")
            protocol = inbound_row.get("protocol", "vless")
            port = inbound_row.get("port", 0)
            
            inbound_config = {
                "tag": tag,
                "listen": inbound_row.get("listen", "0.0.0.0"),
                "port": port,
                "protocol": protocol,
                "settings": settings,
                "streamSettings": stream_settings,
                "sniffing": sniffing
            }
            
            inbounds_array.append(inbound_config)
        
        # Build single Xray config with all inbounds
        xray_config = {
            "log": {"loglevel": "warning"},
            "inbounds": inbounds_array,
            "outbounds": [
                {
                    "protocol": "freedom",
                    "tag": "direct"
                },
                {
                    "protocol": "blackhole",
                    "tag": "BLOCK"
                }
            ],
            "routing": {
                "domainStrategy": "AsIs",
                "rules": [
                    {
                        "ip": ["geoip:private"],
                        "outboundTag": "BLOCK",
                        "type": "field"
                    }
                ]
            }
        }
        
        # Use a default name for the combined config
        core_config = {
            "name": "x-ui-migrated",
            "config": json.dumps(xray_config),
            "exclude_inbound_tags": None,
            "fallbacks_inbound_tags": None,
            "created_at": datetime.now(timezone.utc)
        }
        
        if len(core_config["name"]) > 256:
            core_config["name"] = core_config["name"][:256]
        
        return core_config
    
    def _generate_proxy_settings_with_uuid(self, user_uuid: str, email: Optional[str] = None) -> str:
        """
        Generate proxy settings JSON with a specific UUID (from x-ui).
        Uses original password from x-ui if available, otherwise generates random password.
        """
        # Try to get original password from x-ui if available
        user_password = None
        if email and email in self.email_to_password_map:
            user_password = self.email_to_password_map[email]
            logger.debug(f"Using original password for {email}")
        
        # If no password from x-ui, generate random password
        if not user_password:
            # Generate random 22-character password
            user_password = ''.join(secrets.choice(
                string.ascii_letters + string.digits
            ) for _ in range(22))
            logger.debug(f"Generated random password for {email or 'unknown'}")
        
        proxy_settings = {
            "vmess": {"id": user_uuid},
            "vless": {"id": user_uuid, "flow": ""},
            "trojan": {"password": user_password},
            "shadowsocks": {
                "password": user_password,
                "method": "chacha20-ietf-poly1305"
            }
        }
        
        return json.dumps(proxy_settings)
    
    def _generate_proxy_settings(self, user_key: str) -> str:
        """
        Generate proxy settings JSON with consistent UUID based on user key.
        Generates random passwords for trojan/shadowsocks (not deterministic).
        """
        if not user_key:
            user_key = str(uuid.uuid4())
        
        # Generate consistent UUID from user key using hash
        # Use MD5 hash to generate UUID (similar to how some systems do it)
        hash_obj = hashlib.md5(user_key.encode())
        hash_bytes = hash_obj.digest()
        # Convert to UUID format
        user_uuid = str(uuid.UUID(bytes=hash_bytes[:16]))
        
        # Generate random password (not deterministic) for trojan/shadowsocks
        user_password = ''.join(secrets.choice(
            string.ascii_letters + string.digits
        ) for _ in range(22))
        
        proxy_settings = {
            "vmess": {"id": user_uuid},
            "vless": {"id": user_uuid, "flow": ""},
            "trojan": {"password": user_password},
            "shadowsocks": {
                "password": user_password,
                "method": "chacha20-ietf-poly1305"
            }
        }
        
        return json.dumps(proxy_settings)

