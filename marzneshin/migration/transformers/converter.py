"""
Data converter for transforming Marzneshin data to Pasarguard format.
"""

import json
import logging
import uuid
import secrets
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import xxhash

from migration.models.mappings import get_mapping_info, get_target_table, MappingType

logger = logging.getLogger(__name__)


class DataConverter:
    """Convert Marzneshin data to Pasarguard format."""
    
    def __init__(self):
        """Initialize converter."""
        self.inbound_id_to_tag_map = {}
        self.used_tags = set()
        self.used_usernames = set()
        self.used_config_names = set()  # Track used core_config names
        self.inbound_id_to_final_tag_map = {}  # Map inbound_id to final unique tag
    
    def convert_table(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        target_columns: Dict[str, Any],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        target_table: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Convert table data from Marzneshin to Pasarguard format.
        
        Args:
            table: Table name
            rows: Source rows
            target_columns: Target table column information
            all_data: All source data (for lookups)
            target_table: Target table name (if different from computed target)
            
        Returns:
            Converted rows
        """
        if target_table is None:
            target_table = get_target_table(table)
        logger.info(f"Converting {len(rows)} rows from {table} to {target_table}")
        
        # Special handling for core_configs - create from inbounds grouped by node
        # Check both source and target table names
        if target_table == "core_configs":
            return self._convert_inbounds_to_core_configs(rows, all_data, target_columns)
        
        # Build inbound ID to tag mapping if needed
        if table == "hosts" and all_data:
            self._build_inbound_mapping(all_data)
        
        converted_rows = []
        for idx, row in enumerate(rows, 1):
            try:
                converted_row = self._convert_row(table, row, target_columns, all_data)
                if converted_row:
                    converted_rows.append(converted_row)
            except Exception as e:
                logger.error(f"Error converting row {idx} in {table}: {e}")
                import traceback
                logger.debug(f"Traceback: {traceback.format_exc()}")
                logger.debug(f"Failed row: {row}")
                continue
        
        logger.info(f"Successfully converted {len(converted_rows)}/{len(rows)} rows")
        return converted_rows
    
    def _build_inbound_mapping(self, all_data: Dict[str, List[Dict[str, Any]]]):
        """Build mapping from inbound_id to inbound_tag."""
        inbounds = all_data.get('inbounds', [])
        self.inbound_id_to_tag_map = {
            inbound['id']: inbound['tag'] 
            for inbound in inbounds 
            if 'id' in inbound and 'tag' in inbound
        }
    
    def _convert_inbounds_to_core_configs(
        self,
        inbounds: List[Dict[str, Any]],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        target_columns: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Convert Marzneshin inbounds to Pasarguard core_configs.
        Creates one core_config per inbound, using inbound tag as name.
        Adds counter suffix (starting from 2) only for duplicate tags.
        
        Args:
            inbounds: List of Marzneshin inbound rows
            all_data: All source data (for lookups)
            
        Returns:
            List of core_config rows
        """
        if not inbounds:
            return []
        
        # First pass: collect all tags to identify duplicates
        tag_counts = {}
        for inbound in inbounds:
            tag = inbound.get('tag', '').strip()
            if tag:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        core_configs = []
        tag_name_counters = {}  # Track counter for each tag base name
        
        for inbound in inbounds:
            try:
                inbound_id = inbound.get('id')
                node_id = inbound.get('node_id')
                tag = inbound.get('tag', '').strip()
                
                if not tag:
                    logger.warning(f"Skipping inbound {inbound_id}: missing tag")
                    continue
                
                # Ensure inbound tag is unique (for hosts foreign key reference)
                original_tag = tag
                tag = self._ensure_unique_tag(tag, inbound_id, node_id)
                
                # Store the final tag for this inbound_id so inbounds table conversion uses the same tag
                if inbound_id:
                    self.inbound_id_to_final_tag_map[inbound_id] = tag
                
                # Use inbound tag as base for core_config name
                base_name = original_tag  # Use original tag for naming (before uniqueness suffix)
                
                # If this tag appears multiple times, use counter for core_config name
                # Counter starts at 2 (first occurrence has no counter)
                if tag_counts.get(original_tag, 0) > 1:
                    # Track how many times we've seen this tag
                    if base_name not in tag_name_counters:
                        tag_name_counters[base_name] = 0  # First occurrence: no counter yet
                    
                    tag_name_counters[base_name] += 1
                    
                    # First occurrence (counter == 1): no suffix
                    # Second occurrence (counter == 2): add _2
                    # Third occurrence (counter == 3): add _3, etc.
                    if tag_name_counters[base_name] == 1:
                        core_config_name = base_name  # First occurrence: no counter
                    else:
                        core_config_name = f"{base_name}_{tag_name_counters[base_name]}"
                else:
                    # Single occurrence, use tag as-is (no counter)
                    core_config_name = base_name
                
                # Ensure core_config name is unique (in case original tag was already unique but name exists)
                original_core_config_name = core_config_name
                counter = 2  # Start from 2 for duplicates
                while core_config_name in self.used_config_names:
                    core_config_name = f"{original_core_config_name}_{counter}"
                    counter += 1
                
                # Ensure name is not empty
                if not core_config_name or not core_config_name.strip():
                    core_config_name = f"inbound_{inbound_id}" if inbound_id else f"inbound_{len(core_configs) + 1}"
                    logger.warning(f"Generated fallback name '{core_config_name}' for inbound {inbound_id}")
                
                self.used_config_names.add(core_config_name)
                
                # Parse inbound config
                config_str = inbound.get('config', '{}')
                try:
                    inbound_config = json.loads(config_str) if isinstance(config_str, str) else config_str
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse config for inbound {inbound_id}: {e}")
                    continue
                
                # Convert Marzneshin inbound config to Xray format
                xray_inbound = self._build_xray_inbound(inbound_config, tag, inbound_id, all_data)
                
                # Skip REALITY inbounds that don't have required settings
                if xray_inbound is None:
                    logger.warning(f"Skipping REALITY inbound {inbound_id} (tag: {tag}): missing required settings")
                    continue
                
                # Build Xray config with single inbound
                xray_config = {
                    "log": {
                        "loglevel": "info"
                    },
                    "inbounds": [xray_inbound],
                    "outbounds": [
                        {
                            "protocol": "freedom",
                            "tag": "DIRECT"
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
                
                # Create core_config entry
                core_config = {
                    "name": core_config_name,
                    "config": json.dumps(xray_config),  # Convert to JSON string
                    "exclude_inbound_tags": None,  # Will be converted to None for empty sets
                    "fallbacks_inbound_tags": None,  # Will be converted to None for empty sets
                    "created_at": datetime.now(timezone.utc),  # Add required created_at field
                    "_inbound_id": inbound_id,  # Temporary metadata for mapping
                    "_node_id": node_id  # Temporary metadata for mapping
                }
                
                core_configs.append(core_config)
                
            except Exception as e:
                logger.error(f"Error creating core_config for inbound {inbound.get('id')}: {e}")
                import traceback
                logger.debug(f"Traceback: {traceback.format_exc()}")
                continue
        
        logger.info(f"Created {len(core_configs)} core_configs from {len(inbounds)} inbounds")
        
        # Validate and convert types if target_columns provided
        if target_columns:
            validated_configs = []
            for config in core_configs:
                # Store name before validation to ensure it's preserved
                original_name = config.get('name')
                if not original_name:
                    logger.error(f"Core config missing name field before validation: {config}")
                    continue
                
                # Remove metadata fields before validation
                config_clean = {k: v for k, v in config.items() if not k.startswith('_')}
                
                # Ensure name is in config_clean (it should be, but double-check)
                if 'name' not in config_clean:
                    config_clean['name'] = original_name
                    logger.warning(f"Name field missing from config_clean, restoring: {original_name}")
                
                # Validate and convert types
                validated = self._validate_and_convert_types("core_configs", config_clean, target_columns)
                
                # Always ensure name is present and not None (required field)
                # Even if name is not in target_columns, we need to include it
                if 'name' not in validated:
                    validated['name'] = original_name
                    logger.warning(f"Name field missing after validation, restoring: {original_name}")
                elif not validated.get('name') or validated.get('name') is None:
                    validated['name'] = original_name
                    logger.warning(f"Name field is None/empty after validation, restoring: {original_name}")
                elif validated.get('name') != original_name:
                    # If name was modified during validation, restore original
                    validated['name'] = original_name
                    logger.debug(f"Restored original name field '{original_name}' (was '{validated.get('name')}')")
                
                # Ensure created_at is preserved (it was added in core_config creation)
                if 'created_at' in config_clean and 'created_at' not in validated:
                    validated['created_at'] = config_clean['created_at']
                
                # Final double-check before appending
                if 'name' not in validated or not validated.get('name'):
                    logger.error(f"CRITICAL: Name field still missing after all checks! Config: {validated}")
                    validated['name'] = original_name or f"inbound_{config.get('_inbound_id', len(validated_configs))}"
                
                validated_configs.append(validated)
            
            # Final check on all validated configs before returning
            for idx, cfg in enumerate(validated_configs):
                if 'name' not in cfg or not cfg.get('name'):
                    logger.error(f"CRITICAL: Config {idx} missing name field before return: {cfg}")
                    cfg['name'] = f"inbound_config_{idx + 1}"
            
            logger.info(f"Returning {len(validated_configs)} validated core_configs, all with name field")
            return validated_configs
        
        # If no target_columns, still ensure name is present
        for config in core_configs:
            if 'name' not in config or not config.get('name'):
                logger.warning(f"Core config missing name, adding fallback: {config}")
                config['name'] = f"inbound_{config.get('_inbound_id', len(core_configs))}"
        
        return core_configs
    
    def _build_xray_inbound(
        self,
        inbound_config: Dict[str, Any],
        tag: str,
        inbound_id: Optional[int],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Convert Marzneshin inbound config to Xray inbound format.
        
        Args:
            inbound_config: Marzneshin inbound config dict
            tag: Inbound tag
            inbound_id: Inbound ID (for lookups)
            all_data: All source data (for host lookups)
            
        Returns:
            Xray inbound dict or None if should be skipped (e.g., REALITY without settings)
        """
        protocol = inbound_config.get('protocol', '').lower()
        port = inbound_config.get('port')
        network = inbound_config.get('network', 'tcp').lower()
        tls = inbound_config.get('tls', 'none').lower()
        
        # Build base inbound structure
        xray_inbound = {
            "tag": tag,
            "listen": "0.0.0.0",
            "port": port,
            "protocol": protocol
        }
        
        # Build settings
        settings = {"clients": []}
        
        # Protocol-specific settings
        if protocol == "shadowsocks":
            # Shadowsocks needs network in settings
            network_str = inbound_config.get('network', 'tcp')
            if network_str:
                settings["network"] = network_str
        
        xray_inbound["settings"] = settings
        
        # Build streamSettings
        stream_settings = self._build_stream_settings(inbound_config, inbound_id, all_data)
        
        # Check for REALITY TLS - skip if missing required settings
        if stream_settings and stream_settings.get("security") == "reality":
            reality_settings = stream_settings.get("realitySettings")
            if not reality_settings or not reality_settings.get("privateKey"):
                # REALITY requires privateKey, but Marzneshin only stores reality_public_key in hosts
                # We can't create valid REALITY configs without privateKey
                return None
        
        if stream_settings:
            xray_inbound["streamSettings"] = stream_settings
        
        return xray_inbound
    
    def _build_stream_settings(
        self,
        inbound_config: Dict[str, Any],
        inbound_id: Optional[int],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Build Xray streamSettings from Marzneshin inbound config.
        
        Args:
            inbound_config: Marzneshin inbound config dict
            inbound_id: Inbound ID (for host lookups)
            all_data: All source data (for host lookups)
            
        Returns:
            streamSettings dict or None if not needed
        """
        network = inbound_config.get('network', 'tcp').lower()
        tls = inbound_config.get('tls', 'none').lower()
        header_type = inbound_config.get('header_type')
        
        # Check if we need streamSettings
        # We need it if: TLS is enabled, network is not tcp, or tcp has header config
        needs_stream_settings = (
            tls != 'none' or 
            network != 'tcp' or 
            (network == 'tcp' and header_type)
        )
        
        if not needs_stream_settings:
            return None
        
        stream_settings = {}
        
        # Network transport (ws, grpc, http, etc.)
        if network == 'ws':
            stream_settings["network"] = "ws"
            ws_settings = {}
            
            path = inbound_config.get('path')
            if path:
                ws_settings["path"] = path
            
            host = inbound_config.get('host')
            if host:
                # host can be string or list
                if isinstance(host, list):
                    ws_settings["headers"] = {"Host": host[0] if host else ""}
                elif isinstance(host, str) and host.strip():
                    ws_settings["headers"] = {"Host": host}
            
            if ws_settings:
                stream_settings["wsSettings"] = ws_settings
        
        elif network == 'grpc':
            stream_settings["network"] = "grpc"
            grpc_settings = {}
            
            # Check for serviceName in config
            service_name = inbound_config.get('serviceName') or inbound_config.get('path')
            if service_name:
                grpc_settings["serviceName"] = service_name
            
            if grpc_settings:
                stream_settings["grpcSettings"] = grpc_settings
        
        elif network == 'http':
            stream_settings["network"] = "http"
            http_settings = {}
            
            path = inbound_config.get('path')
            if path:
                http_settings["path"] = path
            
            host = inbound_config.get('host')
            if host:
                if isinstance(host, list):
                    http_settings["host"] = host
                elif isinstance(host, str) and host.strip():
                    http_settings["host"] = [host]
            
            if http_settings:
                stream_settings["httpSettings"] = http_settings
        
        elif network == 'tcp':
            stream_settings["network"] = "tcp"
            # TCP can have header config
            header_type = inbound_config.get('header_type')
            if header_type == 'http':
                tcp_settings = {
                    "header": {
                        "type": "http",
                        "request": {
                            "version": "1.1",
                            "method": "GET",
                            "path": [inbound_config.get('path', '/')],
                            "headers": {}
                        }
                    }
                }
                host = inbound_config.get('host')
                if host:
                    if isinstance(host, list) and host:
                        tcp_settings["header"]["request"]["headers"]["Host"] = host
                    elif isinstance(host, str) and host.strip():
                        tcp_settings["header"]["request"]["headers"]["Host"] = [host]
                
                stream_settings["tcpSettings"] = tcp_settings
        
        # TLS settings
        if tls != 'none':
            # Set security type
            if tls == 'tls':
                stream_settings["security"] = "tls"
            elif tls == 'reality':
                stream_settings["security"] = "reality"
            elif tls == 'xtls':
                stream_settings["security"] = "xtls"
            
            # TLS settings (for tls security)
            if tls == 'tls':
                tls_settings = {}
                
                # SNI
                sni = inbound_config.get('sni')
                if sni:
                    if isinstance(sni, list) and sni:
                        tls_settings["serverName"] = sni[0]
                    elif isinstance(sni, str) and sni.strip():
                        tls_settings["serverName"] = sni
                
                # ALPN
                alpn = inbound_config.get('alpn')
                if alpn:
                    if isinstance(alpn, list):
                        tls_settings["alpn"] = alpn
                    elif isinstance(alpn, str):
                        tls_settings["alpn"] = [alpn]
                
                # Allow insecure
                allow_insecure = inbound_config.get('allowinsecure', False)
                if allow_insecure:
                    tls_settings["allowInsecure"] = True
                
                if tls_settings:
                    stream_settings["tlsSettings"] = tls_settings
            
            # REALITY settings (for reality security)
            elif tls == 'reality':
                reality_settings = self._get_reality_settings_from_hosts(inbound_id, all_data)
                if reality_settings and reality_settings.get("privateKey"):
                    stream_settings["realitySettings"] = reality_settings
                # If no privateKey, will be skipped in _build_xray_inbound
        
        return stream_settings if stream_settings else None
    
    def _get_reality_settings_from_hosts(
        self,
        inbound_id: Optional[int],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get REALITY settings from associated hosts.
        
        Args:
            inbound_id: Inbound ID
            all_data: All source data
            
        Returns:
            REALITY settings dict or None
        """
        if not inbound_id or not all_data:
            return None
        
        hosts = all_data.get('hosts', [])
        for host in hosts:
            if host.get('inbound_id') == inbound_id:
                # Check for REALITY settings in host
                reality_public_key = host.get('reality_public_key') or host.get('realityPublicKey')
                reality_short_id = host.get('reality_short_id') or host.get('realityShortId')
                reality_server_name = host.get('reality_server_name') or host.get('realityServerName')
                
                if reality_public_key:
                    reality_settings = {
                        "publicKey": reality_public_key
                    }
                    
                    if reality_short_id:
                        reality_settings["shortId"] = reality_short_id
                    
                    if reality_server_name:
                        reality_settings["serverNames"] = [reality_server_name] if isinstance(reality_server_name, str) else reality_server_name
                    
                    # REALITY requires privateKey, but Marzneshin only stores public key
                    # Return None to indicate we can't create valid REALITY config
                    return None  # Will be skipped in _build_xray_inbound
        
        return None
    
    def _convert_row(
        self,
        table: str,
        row: Dict[str, Any],
        target_columns: Dict[str, Any],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> Optional[Dict[str, Any]]:
        """Convert a single row."""
        converted = {}
        
        # Special handling for services -> groups mapping
        if table == "services":
            table = "groups"
        elif table == "users_services":
            table = "users_groups_association"
        elif table == "inbounds_services":
            table = "inbounds_groups_association"
        
        for source_col, value in row.items():
            target_col, mapping_type, transform_func = get_mapping_info(table, source_col)
            
            # Skip if mapping says to skip
            if mapping_type == MappingType.SKIP:
                continue
            
            # Apply transformation if needed
            if transform_func:
                value = self._apply_transform(transform_func, value, row, table, source_col)
            
            # Set the value
            if target_col:
                converted[target_col] = value
        
        # Add computed fields
        converted = self._add_computed_fields(table, row, converted, all_data)
        
        # Validate and convert types
        converted = self._validate_and_convert_types(table, converted, target_columns)
        
        return converted
    
    def _apply_transform(
        self,
        transform_name: str,
        value: Any,
        row: Dict[str, Any],
        table: str,
        column: str
    ) -> Any:
        """Apply a named transformation."""
        transform_method = getattr(self, f"_transform_{transform_name}", None)
        
        if transform_method:
            try:
                return transform_method(value, row, table, column)
            except Exception as e:
                logger.warning(f"Transform {transform_name} failed for {table}.{column}: {e}")
                return value
        else:
            logger.warning(f"Unknown transform: {transform_name}")
            return value
    
    def _add_computed_fields(
        self,
        table: str,
        source_row: Dict[str, Any],
        converted_row: Dict[str, Any],
        all_data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> Dict[str, Any]:
        """Add computed fields based on table."""
        if table == "users":
            # Generate proxy_settings from user key
            if 'proxy_settings' not in converted_row:
                user_key = source_row.get('key')
                converted_row['proxy_settings'] = self._generate_proxy_settings(user_key)
            
            # Ensure username is unique
            if 'username' in converted_row:
                converted_row['username'] = self._ensure_unique_username(
                    converted_row['username'], 
                    source_row.get('id')
                )
        
        elif table == "inbounds":
            # Use the same tag that was used during core_config creation
            # This ensures hosts will reference the correct tag
            inbound_id = source_row.get('id')
            if inbound_id and inbound_id in self.inbound_id_to_final_tag_map:
                converted_row['tag'] = self.inbound_id_to_final_tag_map[inbound_id]
            elif 'tag' in converted_row:
                # Fallback: ensure tag is unique (shouldn't happen if core_configs were created first)
                original_tag = converted_row['tag']
                node_id = source_row.get('node_id')
                logger.debug(f"Ensuring unique tag for inbound {inbound_id}: '{original_tag}' (node_id={node_id})")
                try:
                    unique_tag = self._ensure_unique_tag(
                        original_tag,
                        inbound_id,
                        node_id
                    )
                    converted_row['tag'] = unique_tag
                    # Store it for consistency
                    if inbound_id:
                        self.inbound_id_to_final_tag_map[inbound_id] = unique_tag
                    logger.debug(f"  -> Unique tag: '{unique_tag}'")
                except Exception as e:
                    logger.error(f"Error ensuring unique tag for inbound {inbound_id}: {e}")
                    import traceback
                    logger.debug(f"Traceback: {traceback.format_exc()}")
                    # Fallback: use inbound_id to make it unique
                    converted_row['tag'] = f"{original_tag}_{inbound_id}" if inbound_id else f"{original_tag}_unknown"
        
        elif table == "nodes":
            # Ensure required fields have defaults
            if 'server_ca' not in converted_row or not converted_row['server_ca']:
                converted_row['server_ca'] = self._generate_default_ca()
            
            if 'status' not in converted_row:
                converted_row['status'] = 'connecting'
            
            # Pasarguard requires api_port, Marzneshin doesn't have it
            if 'api_port' not in converted_row or converted_row['api_port'] is None:
                # Use port + 1 as default, or 62051 if port not set
                converted_row['api_port'] = converted_row.get('port', 62050) + 1
        
        elif table == "admins":
            # Ensure used_traffic is set
            if 'used_traffic' not in converted_row:
                converted_row['used_traffic'] = 0
            
            # Ensure notification_enable is set with default value
            if 'notification_enable' not in converted_row or converted_row['notification_enable'] is None:
                converted_row['notification_enable'] = {
                    "create": False,
                    "modify": False,
                    "delete": False,
                    "status_change": False,
                    "reset_data_usage": False,
                    "data_reset_by_next": False,
                    "subscription_revoked": False
                }
        
        elif table == "hosts":
            # Add default priority if missing (Marzneshin doesn't have this field)
            if 'priority' not in converted_row or converted_row['priority'] is None:
                converted_row['priority'] = 1  # Default priority
            
            # Ensure status is set to empty array '[]' (EnumArray requires this format)
            # Pasarguard hosts.status is an EnumArray, stored as comma-separated string or '[]' for empty
            if 'status' not in converted_row or converted_row['status'] is None or converted_row['status'] == '':
                converted_row['status'] = '[]'  # Empty array string representation
            
            # Ensure inbound_tag uses the final unique tag from inbounds table
            # The transform function should handle this, but double-check here
            inbound_id = source_row.get('inbound_id')
            if inbound_id and inbound_id in self.inbound_id_to_final_tag_map:
                converted_row['inbound_tag'] = self.inbound_id_to_final_tag_map[inbound_id]
                logger.debug(f"Host {source_row.get('id')}: Using final tag '{converted_row['inbound_tag']}' for inbound_id {inbound_id}")
        
        elif table == "admin_usage_logs":
            # Ensure used_traffic_at_reset is set (default to 0 since we don't have historical reset data)
            if 'used_traffic_at_reset' not in converted_row or converted_row['used_traffic_at_reset'] is None:
                converted_row['used_traffic_at_reset'] = 0
            # Ensure reset_at is set (required field with default)
            if 'reset_at' not in converted_row or converted_row['reset_at'] is None:
                converted_row['reset_at'] = datetime.now(timezone.utc)
        
        elif table == "core_configs":
            # Ensure created_at is set (required field with default)
            if 'created_at' not in converted_row or converted_row['created_at'] is None:
                converted_row['created_at'] = datetime.now(timezone.utc)
        
        return converted_row
    
    def _validate_and_convert_types(
        self,
        table: str,
        row: Dict[str, Any],
        target_columns: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate and convert data types."""
        converted = {}
        
        # Special handling: preserve name field for core_configs even if conversion fails
        original_name = None
        if table == "core_configs" and "name" in row:
            original_name = row.get("name")
        
        for col, value in row.items():
            if col not in target_columns:
                # Column doesn't exist in target, skip it
                # BUT: for core_configs.name, we must preserve it even if not in target_columns
                if table == "core_configs" and col == "name":
                    converted[col] = value
                    continue
                continue
            
            col_info = target_columns[col]
            
            try:
                converted_value = self._convert_type(value, col_info)
                
                # Handle NOT NULL constraints
                if converted_value is None and not col_info['nullable']:
                    converted_value = self._get_default_value(col_info, table, col)
                
                # Special handling for hosts.path field - Pasarguard requires string, not None
                # Use '/' as default instead of empty string to avoid None conversion issues
                if table == "hosts" and col == "path" and (converted_value is None or (isinstance(converted_value, str) and not converted_value.strip())):
                    converted_value = "/"
                
                # Special handling for core_configs.name - ensure it's never None
                if table == "core_configs" and col == "name":
                    if converted_value is None or (isinstance(converted_value, str) and not converted_value.strip()):
                        converted_value = original_name or value or ""
                        logger.warning(f"Core config name was None/empty, using original: {converted_value}")
                
                # Truncate strings if needed
                if col_info.get('max_length') and isinstance(converted_value, str):
                    if len(converted_value) > col_info['max_length']:
                        logger.warning(
                            f"Truncating {table}.{col}: {len(converted_value)} -> {col_info['max_length']}"
                        )
                        converted_value = converted_value[:col_info['max_length']]
                
                converted[col] = converted_value
                
            except Exception as e:
                logger.error(f"Error converting {table}.{col}: {e}")
                # For core_configs.name, preserve original value even on error
                if table == "core_configs" and col == "name":
                    converted[col] = original_name or value or ""
                    logger.warning(f"Preserved core_configs.name after conversion error: {converted[col]}")
                else:
                    converted[col] = None
        
        # Final check: ensure core_configs.name is always present
        if table == "core_configs":
            if "name" not in converted or not converted.get("name"):
                converted["name"] = original_name or row.get("name") or ""
                logger.warning(f"Final check: restored core_configs.name: {converted['name']}")
        
        return converted
    
    def _convert_type(self, value: Any, col_info: Dict[str, Any]) -> Any:
        """Convert value to target type."""
        if value is None:
            return None
        
        col_type = col_info['type'].lower()
        
        # Boolean/TinyInt
        if col_type in ('bool', 'boolean', 'tinyint'):
            if isinstance(value, bool):
                return value
            if isinstance(value, int):
                return bool(value)
            return str(value).lower() in ('true', '1', 't', 'yes')
        
        # BigInteger
        if 'bigint' in col_type:
            try:
                return int(value)
            except (ValueError, TypeError):
                return 0
        
        # Integer
        if 'int' in col_type and 'bigint' not in col_type:
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        
        # Float/Double
        if any(t in col_type for t in ('float', 'double', 'decimal', 'numeric')):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        # DateTime/Timestamp
        if any(t in col_type for t in ('datetime', 'timestamp')):
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    if '.' in value:
                        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                    else:
                        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        return datetime.fromisoformat(value.replace("Z", "+00:00"))
                    except ValueError:
                        return None
        
        # JSON
        if 'json' in col_type:
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            if isinstance(value, str):
                if value.strip() == "":
                    return json.dumps({})
                try:
                    json.loads(value)
                    return value
                except json.JSONDecodeError:
                    return json.dumps({})
        
        # Enum
        if col_info.get('is_enum'):
            str_value = str(value).strip()
            enum_values = col_info.get('enum_values', [])
            if enum_values and str_value not in enum_values:
                # Return first enum value as default
                return enum_values[0] if enum_values else None
            return str_value
        
        # Text/String
        return str(value) if value is not None else None
    
    def _get_default_value(self, col_info: Dict[str, Any], table: str, column: str) -> Any:
        """Get default value for NOT NULL columns."""
        # Use database default if available
        if col_info.get('default') is not None:
            return col_info['default']
        
        col_type = col_info['type'].lower()
        
        if col_type in ('bool', 'boolean', 'tinyint'):
            return False
        
        if 'int' in col_type:
            return 0
        
        if any(t in col_type for t in ('float', 'double', 'decimal', 'numeric')):
            return 0.0
        
        if any(t in col_type for t in ('datetime', 'timestamp')):
            return datetime.now()
        
        if 'json' in col_type:
            return json.dumps({})
        
        if col_info.get('is_enum'):
            enum_values = col_info.get('enum_values', [])
            return enum_values[0] if enum_values else ""
        
        return ""
    
    # Transform functions
    
    def _transform_enabled_to_status(self, value: Any, row: Dict, table: str, col: str) -> str:
        """Transform Marzneshin 'enabled' to Pasarguard 'status'."""
        if value in (True, 1, '1', 'true', 'True'):
            return 'active'
        return 'disabled'
    
    def _transform_invert_boolean(self, value: Any, row: Dict, table: str, col: str) -> bool:
        """Invert boolean value."""
        if value in (True, 1, '1', 'true', 'True'):
            return False
        return True
    
    def _transform_node_status_transform(self, value: Any, row: Dict, table: str, col: str) -> str:
        """Transform node status to Pasarguard format."""
        # Always return connecting as safe default
        return 'connecting'
    
    def _transform_connection_backend_transform(self, value: Any, row: Dict, table: str, col: str) -> str:
        """Transform connection_backend to connection_type."""
        if value:
            backend = str(value).lower().strip()
            if backend in ('grpc', 'grpcio'):
                return 'grpc'
            elif backend in ('rest', 'http'):
                return 'rest'
        return 'grpc'
    
    def _transform_alpn_transform(self, value: Any, row: Dict, table: str, col: str) -> Optional[str]:
        """Transform ALPN to valid enum value."""
        if not value:
            return None
        
        valid_alpn = ['h3', 'h3,h2', 'h3,h2,http/1.1', 'none', 'h2', 'http/1.1', 'h2,http/1.1']
        str_value = str(value).strip()
        
        if str_value in valid_alpn:
            return str_value
        
        return 'none'
    
    def _transform_alpn_fix_none(self, value: Any, row: Dict, table: str, col: str) -> Optional[str]:
        """Transform ALPN value and fix invalid 'none' values for PasarGuard."""
        # First apply standard transform
        transformed = self._transform_alpn_transform(value, row, table, col)
        
        # Replace 'none' or any value containing 'none' with 'h2' since PasarGuard doesn't support 'none'
        if transformed and ('none' in transformed.lower() or transformed == 'none'):
            return 'h2'
        
        return transformed
    
    def _transform_fingerprint_transform(self, value: Any, row: Dict, table: str, col: str) -> str:
        """Transform fingerprint to valid enum value."""
        if not value:
            return 'none'
        
        valid_fingerprints = [
            'none', 'chrome', 'firefox', 'safari', 'ios', 'android', 
            'edge', '360', 'qq', 'random', 'randomized', 'randomizednoalpn', 'unsafe'
        ]
        
        str_value = str(value).lower().strip()
        if str_value in valid_fingerprints:
            return str_value
        
        return 'none'
    
    def _transform_inbound_id_to_tag(self, value: Any, row: Dict, table: str, col: str) -> Optional[str]:
        """Transform inbound_id to inbound_tag."""
        if value is None:
            return None
        
        # Use the final unique tag that was created during core_config conversion
        # This ensures hosts reference the same tags as inbounds table
        if value in self.inbound_id_to_final_tag_map:
            return self.inbound_id_to_final_tag_map[value]
        
        # Fallback to original tag mapping (shouldn't happen if core_configs were created first)
        return self.inbound_id_to_tag_map.get(value)
    
    def _transform_key_to_proxy_settings(self, value: Any, row: Dict, table: str, col: str) -> str:
        """Generate proxy_settings from user key (handled in _generate_proxy_settings)."""
        # This is a marker - actual generation happens in _generate_proxy_settings
        return value
    
    def _generate_proxy_settings(self, user_key: Optional[str]) -> str:
        """Generate proxy settings JSON from user key."""
        if user_key:
            # Use Marzneshin's UUID generation algorithm for consistency
            user_uuid = str(uuid.UUID(bytes=xxhash.xxh128(user_key.encode()).digest()))
            user_password = xxhash.xxh128(user_key.encode()).hexdigest()[:22]
        else:
            # Generate random credentials
            user_uuid = str(uuid.uuid4())
            user_password = ''.join(secrets.choice(
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
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
    
    def _ensure_unique_username(self, username: str, user_id: Optional[int] = None) -> str:
        """Ensure username is unique."""
        if not username or username.strip() == "":
            username = f"user_{user_id}" if user_id else "user"
        
        original = username
        counter = 1
        
        while username in self.used_usernames:
            username = f"{original}_{counter}"
            counter += 1
        
        self.used_usernames.add(username)
        return username
    
    def _ensure_unique_tag(
        self,
        tag: str,
        inbound_id: Optional[int] = None,
        node_id: Optional[int] = None
    ) -> str:
        """
        Ensure inbound tag is unique.
        First occurrence: no counter
        Duplicates: counter starts from 2 (e.g., tag_2, tag_3, etc.)
        """
        if not tag:
            tag = f"inbound_{inbound_id}" if inbound_id else "inbound_unknown"
        
        original = tag
        
        # First check if tag is already unique
        if tag not in self.used_tags:
            self.used_tags.add(tag)
            return tag
        
        # Tag is already used, need to add counter
        # First duplicate gets counter 2, then 3, 4, etc.
        counter = 2
        
        # Generate first variant with counter 2
        if node_id:
            tag = f"{original}_node{node_id}_{counter}"
        elif inbound_id:
            tag = f"{original}_{inbound_id}_{counter}"
        else:
            tag = f"{original}_{counter}"
        
        # Keep trying until we find a unique tag
        while tag in self.used_tags:
            counter += 1
            if node_id:
                tag = f"{original}_node{node_id}_{counter}"
            elif inbound_id:
                tag = f"{original}_{inbound_id}_{counter}"
            else:
                tag = f"{original}_{counter}"
            
            # Safety check to prevent infinite loop
            if counter > 10000:
                # Fallback to using inbound_id or a random suffix
                import random
                tag = f"{original}_{inbound_id or random.randint(10000, 99999)}"
                break
        
        self.used_tags.add(tag)
        return tag
    
    def _generate_default_ca(self) -> str:
        """Generate default CA certificate."""
        return "LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF0OGl2SzVUOFQ4UGYxT1FWbVk3awpMaG1HNHFpWXh0SE9rUkcyOEpMTHZq"

