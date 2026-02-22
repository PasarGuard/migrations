"""
Data converter for transforming v2board dump data to Pasarguard format.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class DataConverter:
    """Convert extracted v2board rows into Pasarguard target-table rows."""

    def __init__(self):
        self._now = datetime.now(timezone.utc)
        self._now_ts = int(self._now.timestamp())

    def convert_all(self, source_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """Convert all source tables into target Pasarguard table payloads."""

        groups, group_ids = self._build_groups(source_data)
        admins = self._build_admins(source_data.get("v2_user", []))
        admin_ids = {row["id"] for row in admins}
        default_admin_id = min(admin_ids) if admin_ids else None

        users, users_groups_association = self._build_users(
            source_data.get("v2_user", []),
            group_ids,
            default_admin_id,
        )

        inbounds, inbounds_groups_association, inbound_configs = self._build_inbounds(
            source_data,
            group_ids,
        )

        core_configs = self._build_core_configs(inbound_configs)

        return {
            "admins": admins,
            "groups": groups,
            "users": users,
            "inbounds": inbounds,
            "core_configs": core_configs,
            "users_groups_association": users_groups_association,
            "inbounds_groups_association": inbounds_groups_association,
        }

    def _build_groups(self, source_data: Dict[str, List[Dict[str, Any]]]) -> Tuple[List[Dict[str, Any]], Set[int]]:
        source_groups = source_data.get("v2_server_group", [])
        users = source_data.get("v2_user", [])

        raw_group_ids: Set[int] = set()
        for row in users:
            raw_group_ids.update(self._parse_group_ids(row.get("group_id")))

        for table in ("v2_server_vmess", "v2_server_trojan", "v2_server_shadowsocks"):
            for row in source_data.get(table, []):
                raw_group_ids.update(self._parse_group_ids(row.get("group_id")))

        groups_by_id: Dict[int, Dict[str, Any]] = {}
        used_names: Set[str] = set()

        for row in source_groups:
            group_id = self._to_int(row.get("id"))
            if group_id is None:
                continue

            group_name = self._to_text(row.get("name"), fallback=f"group-{group_id}")
            group_name = self._unique_name(group_name, used_names, max_len=64)

            groups_by_id[group_id] = {
                "id": group_id,
                "name": group_name,
                "is_disabled": False,
            }

        for group_id in sorted(raw_group_ids):
            if group_id in groups_by_id:
                continue
            placeholder = self._unique_name(f"group-{group_id}", used_names, max_len=64)
            groups_by_id[group_id] = {
                "id": group_id,
                "name": placeholder,
                "is_disabled": False,
            }

        groups = [groups_by_id[group_id] for group_id in sorted(groups_by_id.keys())]
        return groups, set(groups_by_id.keys())

    def _build_admins(self, users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        admins: List[Dict[str, Any]] = []
        seen_ids: Set[int] = set()
        used_usernames: Set[str] = set()

        for row in users:
            if not self._is_truthy(row.get("is_admin")):
                continue

            admin_id = self._to_int(row.get("id"))
            if admin_id is None or admin_id in seen_ids:
                continue

            email = self._to_text(row.get("email"), fallback=f"admin-{admin_id}")
            username_base = self._derive_admin_username(email, admin_id)
            username = self._unique_name(username_base, used_usernames, max_len=34)

            hashed_password = self._to_text(row.get("password"), fallback="change-me")
            telegram_id = self._to_int(row.get("telegram_id"))

            traffic_up = self._to_int(row.get("u"), 0) or 0
            traffic_down = self._to_int(row.get("d"), 0) or 0

            admins.append(
                {
                    "id": admin_id,
                    "created_at": self._ts_to_datetime(row.get("created_at")) or self._now,
                    "username": username,
                    "hashed_password": hashed_password,
                    "is_sudo": False,
                    "password_reset_at": None,
                    "telegram_id": telegram_id,
                    "discord_webhook": None,
                    "discord_id": None,
                    "used_traffic": traffic_up + traffic_down,
                    "is_disabled": self._is_truthy(row.get("banned")),
                    "sub_template": None,
                    "sub_domain": None,
                    "profile_title": None,
                    "support_url": None,
                    "notification_enable": {},
                }
            )
            seen_ids.add(admin_id)

        admins.sort(key=lambda item: item["id"])

        if not admins:
            logger.warning("No admin user found in v2_user; creating fallback admin with id=1")
            admins.append(
                {
                    "id": 1,
                    "created_at": self._now,
                    "username": "admin",
                    "hashed_password": "change-me",
                    "is_sudo": True,
                    "password_reset_at": None,
                    "telegram_id": None,
                    "discord_webhook": None,
                    "discord_id": None,
                    "used_traffic": 0,
                    "is_disabled": False,
                    "sub_template": None,
                    "sub_domain": None,
                    "profile_title": None,
                    "support_url": None,
                    "notification_enable": {},
                }
            )
        else:
            admins[0]["is_sudo"] = True

        return admins

    def _build_users(
        self,
        source_users: List[Dict[str, Any]],
        valid_group_ids: Set[int],
        default_admin_id: Optional[int],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        users: List[Dict[str, Any]] = []
        association_rows: List[Dict[str, Any]] = []

        seen_user_ids: Set[int] = set()
        used_usernames: Set[str] = set()
        seen_associations: Set[Tuple[int, int]] = set()

        for row in source_users:
            if self._is_truthy(row.get("is_admin")):
                continue

            user_id = self._to_int(row.get("id"))
            if user_id is None or user_id in seen_user_ids:
                continue

            email = self._to_text(row.get("email"), fallback=f"user-{user_id}")
            username = self._unique_name(email, used_usernames, max_len=128)

            used_traffic = (self._to_int(row.get("u"), 0) or 0) + (self._to_int(row.get("d"), 0) or 0)
            data_limit_raw = self._to_int(row.get("transfer_enable"))
            data_limit = data_limit_raw if data_limit_raw and data_limit_raw > 0 else None
            expired_at = self._normalize_timestamp(self._to_int(row.get("expired_at")))

            status = self._build_user_status(
                banned=self._is_truthy(row.get("banned")),
                expired_at=expired_at,
                used_traffic=used_traffic,
                data_limit=data_limit,
            )

            user_uuid = self._to_text(row.get("uuid")) or str(uuid.uuid4())
            token = self._to_text(row.get("token")) or uuid.uuid4().hex

            users.append(
                {
                    "id": user_id,
                    "created_at": self._ts_to_datetime(row.get("created_at")) or self._now,
                    "username": username,
                    "status": status,
                    "used_traffic": used_traffic,
                    "data_limit": data_limit,
                    "data_limit_reset_strategy": "no_reset",
                    "expire": self._ts_to_datetime(expired_at),
                    "admin_id": default_admin_id,
                    "sub_revoked_at": None,
                    "note": self._to_text(row.get("remarks"), fallback=None),
                    "online_at": self._ts_to_datetime(row.get("last_login_at")),
                    "on_hold_expire_duration": None,
                    "on_hold_timeout": None,
                    "auto_delete_in_days": None,
                    "edit_at": self._ts_to_datetime(row.get("updated_at")),
                    "last_status_change": self._ts_to_datetime(row.get("updated_at")),
                    "proxy_settings": {
                        "vmess": {"id": user_uuid},
                        "vless": {"id": user_uuid, "flow": ""},
                        "trojan": {"password": token},
                        "shadowsocks": {
                            "password": token,
                            "method": "chacha20-ietf-poly1305",
                        },
                    },
                }
            )

            seen_user_ids.add(user_id)

            for group_id in self._parse_group_ids(row.get("group_id")):
                if group_id not in valid_group_ids:
                    continue
                assoc_key = (user_id, group_id)
                if assoc_key in seen_associations:
                    continue
                seen_associations.add(assoc_key)
                association_rows.append({"user_id": user_id, "groups_id": group_id})

        users.sort(key=lambda item: item["id"])
        association_rows.sort(key=lambda item: (item["user_id"], item["groups_id"]))
        return users, association_rows

    def _build_inbounds(
        self,
        source_data: Dict[str, List[Dict[str, Any]]],
        valid_group_ids: Set[int],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        inbounds: List[Dict[str, Any]] = []
        associations: List[Dict[str, Any]] = []
        xray_inbounds: List[Dict[str, Any]] = []

        table_protocol_pairs = [
            ("v2_server_vmess", "vmess"),
            ("v2_server_trojan", "trojan"),
            ("v2_server_shadowsocks", "shadowsocks"),
        ]

        seen_tags: Set[str] = set()
        seen_associations: Set[Tuple[int, int]] = set()
        inbound_id = 1

        for table, protocol in table_protocol_pairs:
            rows = source_data.get(table, [])
            for row in rows:
                if not self._is_truthy(row.get("show"), truthy_if_missing=True):
                    continue

                source_id = self._to_int(row.get("id"))
                if source_id is None:
                    continue

                listen_port = self._to_int(row.get("server_port")) or self._to_int(row.get("port"))
                if listen_port is None or listen_port <= 0:
                    logger.debug("Skipping server row %s:%s due to invalid port", table, source_id)
                    continue

                tag = self._unique_tag(f"{protocol}-{source_id}", seen_tags)
                inbounds.append({"id": inbound_id, "tag": tag})

                for group_id in self._parse_group_ids(row.get("group_id")):
                    if group_id not in valid_group_ids:
                        continue
                    assoc_key = (inbound_id, group_id)
                    if assoc_key in seen_associations:
                        continue
                    seen_associations.add(assoc_key)
                    associations.append({"inbound_id": inbound_id, "group_id": group_id})

                config = self._build_inbound_config(protocol, row, tag, listen_port)
                if config is not None:
                    xray_inbounds.append(config)

                inbound_id += 1

        inbounds.sort(key=lambda item: item["id"])
        associations.sort(key=lambda item: (item["inbound_id"], item["group_id"]))
        return inbounds, associations, xray_inbounds

    def _build_inbound_config(
        self,
        protocol: str,
        row: Dict[str, Any],
        tag: str,
        port: int,
    ) -> Optional[Dict[str, Any]]:
        host = self._to_text(row.get("host"), fallback="")

        if protocol == "vmess":
            network = self._to_text(row.get("network"), fallback="tcp").lower()
            if network not in {"tcp", "ws", "grpc", "httpupgrade", "xhttp", "kcp", "http"}:
                network = "tcp"

            tls_enabled = self._is_truthy(row.get("tls"))
            stream_settings: Dict[str, Any] = {
                "network": network,
                "security": "tls" if tls_enabled else "none",
            }

            network_settings = self._parse_json_object(row.get("networkSettings"))
            if network == "ws":
                ws_settings: Dict[str, Any] = {
                    "path": self._to_text(network_settings.get("path"), fallback="/") if network_settings else "/"
                }
                headers = network_settings.get("headers") if isinstance(network_settings, dict) else None
                if not isinstance(headers, dict):
                    headers = {}
                if host and "Host" not in headers:
                    headers["Host"] = host
                if headers:
                    ws_settings["headers"] = headers
                stream_settings["wsSettings"] = ws_settings

            if tls_enabled:
                tls_settings = self._parse_json_object(row.get("tlsSettings"))
                if host and "serverName" not in tls_settings:
                    tls_settings["serverName"] = host
                stream_settings["tlsSettings"] = tls_settings

            return {
                "tag": tag,
                "listen": "0.0.0.0",
                "port": port,
                "protocol": "vmess",
                "settings": {"clients": []},
                "streamSettings": stream_settings,
                "sniffing": {"enabled": True, "destOverride": ["http", "tls"]},
            }

        if protocol == "trojan":
            tls_settings: Dict[str, Any] = {
                "allowInsecure": self._is_truthy(row.get("allow_insecure")),
            }
            server_name = self._to_text(row.get("server_name"), fallback=None)
            if server_name:
                tls_settings["serverName"] = server_name
            elif host:
                tls_settings["serverName"] = host

            return {
                "tag": tag,
                "listen": "0.0.0.0",
                "port": port,
                "protocol": "trojan",
                "settings": {"clients": []},
                "streamSettings": {
                    "network": "tcp",
                    "security": "tls",
                    "tlsSettings": tls_settings,
                },
                "sniffing": {"enabled": True, "destOverride": ["http", "tls"]},
            }

        if protocol == "shadowsocks":
            method = self._to_text(row.get("cipher"), fallback="chacha20-ietf-poly1305")
            return {
                "tag": tag,
                "listen": "0.0.0.0",
                "port": port,
                "protocol": "shadowsocks",
                "settings": {
                    "method": method,
                    "password": "change-me",
                    "network": "tcp,udp",
                },
                "streamSettings": {"network": "tcp", "security": "none"},
                "sniffing": {"enabled": True, "destOverride": ["http", "tls"]},
            }

        return None

    def _build_core_configs(self, xray_inbounds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not xray_inbounds:
            return []

        config = {
            "log": {"loglevel": "warning"},
            "inbounds": xray_inbounds,
            "outbounds": [
                {"protocol": "freedom", "tag": "direct"},
                {"protocol": "blackhole", "tag": "BLOCK"},
            ],
            "routing": {
                "domainStrategy": "AsIs",
                "rules": [
                    {
                        "type": "field",
                        "ip": ["geoip:private"],
                        "outboundTag": "BLOCK",
                    }
                ],
            },
        }

        return [
            {
                "id": 1,
                "created_at": self._now,
                "name": "v2board-migrated",
                "config": config,
                "exclude_inbound_tags": None,
                "fallbacks_inbound_tags": None,
            }
        ]

    def _build_user_status(
        self,
        banned: bool,
        expired_at: Optional[int],
        used_traffic: int,
        data_limit: Optional[int],
    ) -> str:
        if banned:
            return "disabled"

        if expired_at and expired_at > 0 and expired_at <= self._now_ts:
            return "expired"

        if data_limit and data_limit > 0 and used_traffic >= data_limit:
            return "limited"

        return "active"

    def _derive_admin_username(self, email: str, admin_id: int) -> str:
        base = email.split("@", 1)[0].strip() if email else ""
        if not base:
            base = f"admin{admin_id}"

        # Keep usernames simple and portable.
        cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", base)
        cleaned = cleaned.strip("._-") or f"admin{admin_id}"
        return cleaned

    def _unique_tag(self, base: str, used: Set[str]) -> str:
        candidate = base
        counter = 2
        while candidate in used:
            candidate = f"{base}-{counter}"
            counter += 1
        used.add(candidate)
        return candidate

    def _unique_name(self, base: str, used: Set[str], max_len: int) -> str:
        raw = (base or "value").strip()
        if not raw:
            raw = "value"

        if len(raw) > max_len:
            raw = raw[:max_len]

        candidate = raw
        counter = 2
        while candidate in used:
            suffix = f"-{counter}"
            trimmed = raw[: max_len - len(suffix)] if len(raw) + len(suffix) > max_len else raw
            candidate = f"{trimmed}{suffix}"
            counter += 1

        used.add(candidate)
        return candidate

    def _parse_group_ids(self, value: Any) -> List[int]:
        if value is None:
            return []

        if isinstance(value, (int, float)):
            numeric = self._to_int(value)
            return [numeric] if numeric is not None else []

        text = str(value).strip()
        if not text:
            return []

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                out: List[int] = []
                for item in parsed:
                    numeric = self._to_int(item)
                    if numeric is not None:
                        out.append(numeric)
                return sorted(set(out))
            except Exception:
                pass

        out = []
        for chunk in re.split(r"[,|]", text):
            numeric = self._to_int(chunk)
            if numeric is not None:
                out.append(numeric)

        return sorted(set(out))

    def _parse_json_object(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {}

        if isinstance(value, dict):
            return value

        text = str(value).strip()
        if not text:
            return {}

        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _to_text(self, value: Any, fallback: Optional[str] = "") -> Optional[str]:
        if value is None:
            return fallback

        text = str(value)
        if text == "":
            return fallback

        return text

    def _is_truthy(self, value: Any, truthy_if_missing: bool = False) -> bool:
        if value is None:
            return truthy_if_missing

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return value != 0

        text = str(value).strip().lower()
        if text in {"", "none", "null"}:
            return truthy_if_missing

        return text in {"1", "true", "yes", "on"}

    def _to_int(self, value: Any, fallback: Optional[int] = None) -> Optional[int]:
        if value is None:
            return fallback

        if isinstance(value, bool):
            return int(value)

        if isinstance(value, int):
            return value

        if isinstance(value, float):
            return int(value)

        text = str(value).strip()
        if not text:
            return fallback

        try:
            if "." in text:
                return int(float(text))
            return int(text)
        except ValueError:
            return fallback

    def _ts_to_datetime(self, timestamp: Any) -> Optional[datetime]:
        ts = self._normalize_timestamp(self._to_int(timestamp))
        if ts is None or ts <= 0:
            return None

        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None

    def _normalize_timestamp(self, timestamp: Optional[int]) -> Optional[int]:
        if timestamp is None:
            return None

        # Some panels store unix timestamps in milliseconds.
        if timestamp > 10_000_000_000:
            return int(timestamp / 1000)

        return timestamp
