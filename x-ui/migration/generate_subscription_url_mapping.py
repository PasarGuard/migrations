"""
Generate mapping of old x-ui subscription URLs to new Pasarguard subscription URLs.
"""

import json
import secrets
import time
import logging
import sys
import sqlite3
from pathlib import Path
from math import ceil
from base64 import b64encode
from hashlib import sha256
from typing import Dict, Any, Optional

if Path(__file__).parent.name == 'migration':
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

from migration.config import XUI_CONFIG, PASARGUARD_CONFIG
from migration.utils import setup_logging

logger = logging.getLogger(__name__)


def get_pasarguard_subscription_url_prefix(admin_id: Optional[int], pasarguard_conn) -> str:
    """Get subscription URL prefix from Pasarguard admin or settings."""
    try:
        if admin_id:
            cursor = pasarguard_conn.cursor()
            cursor.execute("SELECT sub_domain FROM admins WHERE id = ?", (admin_id,))
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
        
        cursor = pasarguard_conn.cursor()
        cursor.execute("SELECT subscription FROM settings WHERE id = 0")
        result = cursor.fetchone()
        if result and result[0]:
            subscription_settings = json.loads(result[0])
            if subscription_settings and subscription_settings.get('url_prefix'):
                return subscription_settings['url_prefix']
        
        return ""
    except Exception as e:
        logger.warning(f"Failed to get Pasarguard subscription URL prefix: {e}")
        return ""


def create_pasarguard_subscription_token(username: str, secret_key: str) -> str:
    """Create Pasarguard subscription token (synchronous version)."""
    data = username + "," + str(ceil(time.time()))
    data_b64_str = b64encode(data.encode("utf-8"), altchars=b"-_").decode("utf-8").rstrip("=")
    data_b64_sign = b64encode(
        sha256((data_b64_str + secret_key).encode("utf-8")).digest(), altchars=b"-_"
    ).decode("utf-8")[:10]
    data_final = data_b64_str + data_b64_sign
    return data_final


def get_pasarguard_jwt_secret(pasarguard_conn) -> str:
    """Get JWT secret key from Pasarguard database."""
    try:
        cursor = pasarguard_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jwt'")
        if not cursor.fetchone():
            logger.warning("JWT table not found in Pasarguard database. Using default secret.")
            logger.warning("Note: Generated subscription URLs may not work until JWT secret is configured.")
            return "default_secret_key_change_in_production"
        
        cursor.execute("SELECT secret_key FROM jwt WHERE id = 0")
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        
        cursor.execute("SELECT secret_key FROM jwt WHERE id = 1")
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        
        cursor.execute("SELECT secret_key FROM jwt LIMIT 1")
        result = cursor.fetchone()
        if result and result[0]:
            logger.info("Found JWT secret_key (not at id=0)")
            return result[0]
        
        logger.warning("JWT secret_key not found in jwt table. Using default secret.")
        logger.warning("Note: Generated subscription URLs may not work until JWT secret is configured in Pasarguard.")
        return "default_secret_key_change_in_production"
    except Exception as e:
        logger.warning(f"Failed to get JWT secret: {e}")
        logger.warning("Using default secret. Generated subscription URLs may not work until JWT secret is configured.")
        return "default_secret_key_change_in_production"


def extract_subscription_token_from_inbound(inbound_settings: str, email: str) -> Optional[str]:
    """Extract subscription token (subId) from inbound client settings."""
    try:
        if not inbound_settings:
            return None
        
        settings = json.loads(inbound_settings) if isinstance(inbound_settings, str) else inbound_settings
        clients = settings.get("clients", [])
        
        for client in clients:
            if client.get("email") == email:
                sub_id = client.get("subId") or client.get("sub_id")
                if sub_id:
                    return sub_id
                
                sub_token = client.get("sub_token") or client.get("subscription_token") or client.get("token") or client.get("sub")
                if sub_token:
                    return sub_token
        return None
    except Exception as e:
        logger.debug(f"Failed to extract subscription token from inbound: {e}")
        return None


def generate_subscription_url_mapping(
    output_file: str = "subscription_url_mapping.json",
    xui_subscription_path: str = "sub",
    pasarguard_subscription_path: str = "sub",
    xui_db_path: Optional[str] = None,
    pasarguard_db_path: Optional[str] = None,
    xui_subscription_domain: Optional[str] = None,
    xui_subscription_port: Optional[int] = None
) -> Dict[str, Any]:
    """
    Generate mapping of old x-ui subscription URLs to new Pasarguard subscription URLs.
    
    Args:
        output_file: Output JSON file path
        xui_subscription_path: Subscription path for x-ui (default: "sub")
        pasarguard_subscription_path: Subscription path for Pasarguard (default: "sub")
        xui_db_path: Path to x-ui SQLite database (default: from config)
        pasarguard_db_path: Path to Pasarguard SQLite database (default: from config)
        xui_subscription_domain: x-ui subscription service domain (e.g., gost.fastnet-iran.sbs).
                                 If provided, generates full URLs: https://domain:port/path/token?name=token
        xui_subscription_port: x-ui subscription service port (e.g., 2096). Required if domain is provided.
    
    Returns:
        Dictionary with mapping data
    """
    logger.info("Generating subscription URL mapping for x-ui...")
    
    xui_db = xui_db_path or XUI_CONFIG.db_path
    pasarguard_db = pasarguard_db_path or PASARGUARD_CONFIG.db_path
    
    xui_db_path_obj = Path(xui_db)
    pasarguard_db_path_obj = Path(pasarguard_db)
    
    if not xui_db_path_obj.exists():
        raise FileNotFoundError(f"x-ui database not found: {xui_db}")
    
    if not pasarguard_db_path_obj.exists():
        raise FileNotFoundError(
            f"Pasarguard database not found: {pasarguard_db}\n"
            f"Please run the migration first: uv run migrate.py"
        )
    
    logger.info(f"Connecting to x-ui database: {xui_db}")
    xui_conn = sqlite3.connect(xui_db)
    xui_conn.row_factory = sqlite3.Row
    
    logger.info(f"Connecting to Pasarguard database: {pasarguard_db}")
    pasarguard_conn = sqlite3.connect(pasarguard_db)
    pasarguard_conn.row_factory = sqlite3.Row
    
    try:
        def table_exists(conn, table_name: str) -> bool:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            return cursor.fetchone() is not None
        
        def get_all_tables(conn) -> list:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return [row[0] for row in cursor.fetchall()]
        
        if not table_exists(xui_conn, 'client_traffics'):
            if table_exists(xui_conn, 'clients'):
                logger.warning("Table 'client_traffics' not found, trying 'clients' table...")
                xui_user_table = 'clients'
            else:
                available_tables = get_all_tables(xui_conn)
                tables_msg = ', '.join(available_tables) if available_tables else '(no tables found)'
                raise ValueError(
                    f"Required table 'client_traffics' not found in x-ui database.\n"
                    f"Available tables: {tables_msg}"
                )
        else:
            xui_user_table = 'client_traffics'
        
        if not table_exists(pasarguard_conn, 'users'):
            available_tables = get_all_tables(pasarguard_conn)
            tables_msg = ', '.join(available_tables) if available_tables else '(database is empty - no tables found)'
            
            if not available_tables:
                raise ValueError(
                    f"Pasarguard database exists but is empty (no tables found).\n"
                    f"Database path: {pasarguard_db}\n\n"
                    f"Please run the migration first to populate the database:\n"
                    f"  cd x-ui\n"
                    f"  uv run migrate.py\n\n"
                    f"This will create the Pasarguard database with all required tables in: output-db/db.sqlite3"
                )
            else:
                raise ValueError(
                    f"Required table 'users' not found in Pasarguard database.\n"
                    f"Available tables: {tables_msg}\n\n"
                    f"Database path: {pasarguard_db}\n\n"
                    f"Please run the migration first:\n"
                    f"  cd x-ui\n"
                    f"  uv run migrate.py"
                )
        
        jwt_secret = get_pasarguard_jwt_secret(pasarguard_conn)
        
        logger.info(f"Fetching users from x-ui (table: {xui_user_table})...")
        cursor = xui_conn.cursor()
        if xui_user_table not in ['client_traffics', 'clients']:
            raise ValueError(f"Invalid table name: {xui_user_table}")

        logger.info("Pre-loading subIds from inbounds...")
        cursor.execute("SELECT id, settings FROM inbounds")
        inbound_subid_map: dict[tuple, str] = {}
        inbound_count = 0
        for ib_row in cursor:
            inbound_count += 1
            raw = ib_row['settings']
            if not raw:
                logger.debug(f"Inbound {ib_row['id']} has empty settings, skipping")
                continue
            try:
                ib_settings = json.loads(raw) if isinstance(raw, str) else raw
                for client in ib_settings.get('clients', []):
                    client_email = client.get('email', '').strip()
                    sub_id = (
                        client.get('subId') or client.get('sub_id') or
                        client.get('sub_token') or client.get('subscription_token') or
                        client.get('token') or client.get('sub')
                    )
                    if client_email and sub_id:
                        inbound_subid_map[(ib_row['id'], client_email)] = sub_id
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"Failed to parse settings for inbound {ib_row['id']}: {e}")
        logger.info(f"  Built subId map: {len(inbound_subid_map)} entries across {inbound_count} inbounds")

        cursor.execute(f"PRAGMA table_info(`{xui_user_table}`)")
        columns = [row[1] for row in cursor.fetchall()]
        logger.debug(f"Available columns in {xui_user_table}: {columns}")

        select_fields = ["ct.id", "ct.email", "ct.inbound_id"]
        if 'sub_token' in columns:
            select_fields.append("ct.sub_token")
        elif 'subscription_token' in columns:
            select_fields.append("ct.subscription_token")
        elif 'token' in columns:
            select_fields.append("ct.token")
        elif 'sub' in columns:
            select_fields.append("ct.sub")

        cursor.execute(f"""
            SELECT {', '.join(select_fields)}
            FROM `{xui_user_table}` ct
            ORDER BY ct.id
        """)
        xui_users = cursor.fetchall()
        
        logger.info("Fetching users from Pasarguard...")
        cursor = pasarguard_conn.cursor()
        cursor.execute("""
            SELECT u.id, u.username, u.admin_id, a.sub_domain as admin_sub_domain
            FROM users u
            LEFT JOIN admins a ON u.admin_id = a.id
            ORDER BY u.id
        """)
        pasarguard_users = cursor.fetchall()
        
        # Create email -> user mapping for Pasarguard (x-ui uses email as identifier)
        pasarguard_user_map_by_email = {}
        pasarguard_user_map_by_id = {}
        for user in pasarguard_users:
            # Try to match by email (username in pasarguard might be email)
            pasarguard_user_map_by_email[user['username']] = user
            pasarguard_user_map_by_id[user['id']] = user
        
        logger.info(f"Pasarguard users: {len(pasarguard_users)} (unique emails: {len(pasarguard_user_map_by_email)}, unique IDs: {len(pasarguard_user_map_by_id)})")
        
        # Cache admin prefixes to avoid repeated database queries
        logger.info("Caching admin subscription URL prefixes...")
        admin_prefix_cache = {}
        unique_admin_ids = set()
        for user in pasarguard_users:
            admin_id = user['admin_id']  # sqlite3.Row supports dict-style access
            if admin_id is not None:
                unique_admin_ids.add(admin_id)
        
        # Pre-fetch all admin prefixes
        for admin_id in unique_admin_ids:
            admin_prefix_cache[admin_id] = get_pasarguard_subscription_url_prefix(admin_id, pasarguard_conn)
        
        # Also cache the default (None) prefix
        admin_prefix_cache[None] = get_pasarguard_subscription_url_prefix(None, pasarguard_conn)
        
        logger.info(f"Cached prefixes for {len(admin_prefix_cache)} admin IDs")
        
        # Generate mappings
        mappings = {}
        not_found = {}
        matched_by_email = 0
        matched_by_id = 0
        
        logger.info(f"Processing {len(xui_users)} users...")
        for idx, xui_user in enumerate(xui_users, 1):
            if idx % 100 == 0 or idx == len(xui_users):
                logger.info(f"  Processed {idx}/{len(xui_users)} users...")
            
            email = xui_user['email']
            xui_user_id = xui_user['id']
            inbound_id = xui_user['inbound_id']

            old_url = None
            subscription_token = None
            for token_field in ['sub_token', 'subscription_token', 'token', 'sub']:
                try:
                    token_value = xui_user[token_field]
                    if token_value:
                        subscription_token = token_value
                        break
                except (KeyError, IndexError):
                    continue

            if not subscription_token and email and inbound_id is not None:
                subscription_token = inbound_subid_map.get((inbound_id, email))

            if not subscription_token:
                logger.debug(f"No subId for user '{email}' (ID: {xui_user_id}, inbound: {inbound_id})")
                not_found[email if email else f"user_{xui_user_id}"] = {
                    "user_id": xui_user_id,
                    "reason": "No subscription token (subId) found in inbound settings"
                }
                continue
            
            if xui_subscription_domain:
                port_part = f":{xui_subscription_port}" if xui_subscription_port else ""
                protocol = "https" if (xui_subscription_port == 443 or xui_subscription_port == 2096) else "http"
                old_url = f"{protocol}://{xui_subscription_domain}{port_part}/{xui_subscription_path}/{subscription_token}?name={subscription_token}"
            else:
                old_url = f"/{xui_subscription_path}/{subscription_token}?name={subscription_token}"
            
            pg_user = None
            match_method = None
            
            if email and email in pasarguard_user_map_by_email:
                pg_user = pasarguard_user_map_by_email[email]
                match_method = "email"
                matched_by_email += 1
            elif xui_user_id in pasarguard_user_map_by_id:
                pg_user = pasarguard_user_map_by_id[xui_user_id]
                match_method = "id"
                matched_by_id += 1
                logger.debug(f"Matched user ID {xui_user_id} by ID (email: '{email}')")
            
            if pg_user:
                pg_admin_id = pg_user['admin_id']
                pg_username = pg_user['username'] if pg_user['username'] else email
                
                pg_prefix = admin_prefix_cache.get(pg_admin_id) or admin_prefix_cache.get(None) or ""
                pg_salt = secrets.token_hex(8)
                pasarguard_prefix = pg_prefix.replace("*", pg_salt) if pg_prefix else ""
                
                token = create_pasarguard_subscription_token(pg_username, jwt_secret)
                
                new_url = f"{pasarguard_prefix}/{pasarguard_subscription_path}/{token}".strip("/")
                if not new_url.startswith("http"):
                    new_url = f"/{pasarguard_subscription_path}/{token}"
                
                mapping_key = email if email else f"user_{xui_user_id}"
                mapping_entry = {
                    "user_id": xui_user_id,
                    "old_subscription_url": old_url,
                    "new_subscription_url": new_url,
                    "inbound_id": inbound_id
                }

                if pg_username != email:
                    mapping_entry["username_pasarguard"] = pg_username

                if match_method != "email":
                    mapping_entry["matched_by"] = match_method

                mappings[mapping_key] = mapping_entry
            else:
                mapping_key = email if email else f"user_{xui_user_id}"
                not_found[mapping_key] = {
                    "user_id": xui_user_id,
                    "old_subscription_url": old_url,
                    "email": email
                }
        
        logger.info(f"Matched {matched_by_email} users by email, {matched_by_id} users by ID")
        
        result = {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_users": len(xui_users),
            "mapped_users": len(mappings),
            "not_found_users": len(not_found),
            "panel": "x-ui",
            "url_formats": {
                "old_format": f"/{xui_subscription_path}/{{email}}/{{key}}",
                "new_format": f"/{pasarguard_subscription_path}/{{token}}"
            },
            "mappings": mappings
        }
        
        # Only include not_found if there are any
        if not_found:
            result["not_found"] = not_found
        
        # Save to file
        logger.info(f"Saving mapping to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ Generated mapping for {len(mappings)} users")
        logger.info(f"✓ {len(not_found)} users not found in Pasarguard")
        logger.info(f"✓ Mapping saved to {output_file}")
        
        return result
        
    finally:
        xui_conn.close()
        pasarguard_conn.close()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate subscription URL mapping from x-ui to Pasarguard'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='subscription_url_mapping.json',
        help='Output JSON file path (default: subscription_url_mapping.json)'
    )
    parser.add_argument(
        '--xui-path',
        type=str,
        default='sub',
        help='x-ui subscription path (default: sub)'
    )
    parser.add_argument(
        '--pasarguard-path',
        type=str,
        default='sub',
        help='Pasarguard subscription path (default: sub)'
    )
    parser.add_argument(
        '--xui-db',
        type=str,
        help='Path to x-ui SQLite database (default: from config)'
    )
    parser.add_argument(
        '--pasarguard-db',
        type=str,
        help='Path to Pasarguard SQLite database (default: from config)'
    )
    parser.add_argument(
        '--xui-domain',
        type=str,
        help='x-ui subscription service domain (e.g., gost.fastnet-iran.sbs). If provided, generates full URLs: https://domain:port/path/token?name=token'
    )
    parser.add_argument(
        '--xui-port',
        type=int,
        help='x-ui subscription service port (e.g., 2096). Required if --xui-domain is provided'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    
    args = parser.parse_args()
    
    setup_logging(level=args.log_level)
    
    try:
        generate_subscription_url_mapping(
            output_file=args.output,
            xui_subscription_path=args.xui_path,
            pasarguard_subscription_path=args.pasarguard_path,
            xui_db_path=args.xui_db,
            pasarguard_db_path=args.pasarguard_db,
            xui_subscription_domain=args.xui_domain,
            xui_subscription_port=args.xui_port
        )
    except Exception as e:
        logger.error(f"Failed to generate mapping: {e}")
        raise


if __name__ == '__main__':
    main()

