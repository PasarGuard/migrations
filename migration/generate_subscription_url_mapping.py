"""
Generate mapping of old Marzneshin subscription URLs to new Pasarguard subscription URLs.
"""

import json
import secrets
import time
import logging
import sys
from pathlib import Path
from math import ceil
from base64 import b64encode
from hashlib import sha256
from typing import Dict, List, Any, Optional
import pymysql
from pymysql.cursors import DictCursor

# Add project root to path if running from migration directory
if Path(__file__).parent.name == 'migration':
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

from migration.config import MARZNESHIN_CONFIG, PASARGUARD_CONFIG
from migration.utils import setup_logging

logger = logging.getLogger(__name__)


def get_marzneshin_subscription_url_prefix(admin_id: Optional[int], marzneshin_conn) -> str:
    """Get subscription URL prefix from Marzneshin admin or settings."""
    try:
        if admin_id:
            with marzneshin_conn.cursor(DictCursor) as cursor:
                cursor.execute(
                    "SELECT subscription_url_prefix FROM admins WHERE id = %s",
                    (admin_id,)
                )
                result = cursor.fetchone()
                if result and result.get('subscription_url_prefix'):
                    return result['subscription_url_prefix']
        
        # Try to get from environment variable (SUBSCRIPTION_URL_PREFIX)
        # For now, return empty string as default
        return ""
    except Exception as e:
        logger.warning(f"Failed to get Marzneshin subscription URL prefix: {e}")
        return ""


def get_pasarguard_subscription_url_prefix(admin_id: Optional[int], pasarguard_conn) -> str:
    """Get subscription URL prefix from Pasarguard admin or settings."""
    try:
        if admin_id:
            with pasarguard_conn.cursor(DictCursor) as cursor:
                cursor.execute(
                    "SELECT sub_domain FROM admins WHERE id = %s",
                    (admin_id,)
                )
                result = cursor.fetchone()
                if result and result.get('sub_domain'):
                    return result['sub_domain']
        
        # Get from settings
        with pasarguard_conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT subscription FROM settings WHERE id = 0")
            result = cursor.fetchone()
            if result and result.get('subscription'):
                import json as json_lib
                subscription_settings = json_lib.loads(result['subscription'])
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
        # Check if jwt table exists
        with pasarguard_conn.cursor(DictCursor) as cursor:
            cursor.execute("SHOW TABLES LIKE 'jwt'")
            if not cursor.fetchone():
                logger.warning("JWT table not found in Pasarguard database. Using default secret.")
                logger.warning("Note: Generated subscription URLs may not work until JWT secret is configured.")
                return "default_secret_key_change_in_production"
            
            # Try id = 0 first (common default), then try id = 1, then any row
            cursor.execute("SELECT secret_key FROM jwt WHERE id = 0")
            result = cursor.fetchone()
            if result and result.get('secret_key'):
                return result['secret_key']
            
            # Try id = 1
            cursor.execute("SELECT secret_key FROM jwt WHERE id = 1")
            result = cursor.fetchone()
            if result and result.get('secret_key'):
                return result['secret_key']
            
            # Try any row
            cursor.execute("SELECT secret_key FROM jwt LIMIT 1")
            result = cursor.fetchone()
            if result and result.get('secret_key'):
                logger.info("Found JWT secret_key (not at id=0)")
                return result['secret_key']
        
        # JWT table exists but no secret_key found
        logger.warning("JWT secret_key not found in jwt table. Using default secret.")
        logger.warning("Note: Generated subscription URLs may not work until JWT secret is configured in Pasarguard.")
        return "default_secret_key_change_in_production"
    except Exception as e:
        logger.warning(f"Failed to get JWT secret: {e}")
        logger.warning("Using default secret. Generated subscription URLs may not work until JWT secret is configured.")
        return "default_secret_key_change_in_production"


def generate_subscription_url_mapping(
    output_file: str = "subscription_url_mapping.json",
    marzneshin_subscription_path: str = "sub",
    pasarguard_subscription_path: str = "sub"
) -> Dict[str, Any]:
    """
    Generate mapping of old Marzneshin subscription URLs to new Pasarguard subscription URLs.
    
    Args:
        output_file: Output JSON file path
        marzneshin_subscription_path: Subscription path for Marzneshin (default: "sub")
        pasarguard_subscription_path: Subscription path for Pasarguard (default: "sub")
    
    Returns:
        Dictionary with mapping data
    """
    logger.info("Generating subscription URL mapping...")
    
    # Connect to databases
    logger.info("Connecting to Marzneshin database...")
    marzneshin_conn = pymysql.connect(
        host=MARZNESHIN_CONFIG.host,
        port=MARZNESHIN_CONFIG.port,
        user=MARZNESHIN_CONFIG.user,
        password=MARZNESHIN_CONFIG.password,
        database=MARZNESHIN_CONFIG.database,
        charset=MARZNESHIN_CONFIG.charset,
        cursorclass=DictCursor
    )
    
    logger.info("Connecting to Pasarguard database...")
    pasarguard_conn = pymysql.connect(
        host=PASARGUARD_CONFIG.host,
        port=PASARGUARD_CONFIG.port,
        user=PASARGUARD_CONFIG.user,
        password=PASARGUARD_CONFIG.password,
        database=PASARGUARD_CONFIG.database,
        charset=PASARGUARD_CONFIG.charset,
        cursorclass=DictCursor
    )
    
    try:
        # Get Pasarguard JWT secret for token generation
        jwt_secret = get_pasarguard_jwt_secret(pasarguard_conn)
        
        # Get all users from both databases
        logger.info("Fetching users from Marzneshin...")
        with marzneshin_conn.cursor(DictCursor) as cursor:
            cursor.execute("""
                SELECT u.id, u.username, u.key, u.admin_id, a.subscription_url_prefix as admin_subscription_url_prefix
                FROM users u
                LEFT JOIN admins a ON u.admin_id = a.id
                ORDER BY u.id
            """)
            marzneshin_users = cursor.fetchall()
        
        logger.info("Fetching users from Pasarguard...")
        with pasarguard_conn.cursor(DictCursor) as cursor:
            cursor.execute("""
                SELECT u.id, u.username, u.admin_id, a.sub_domain as admin_sub_domain
                FROM users u
                LEFT JOIN admins a ON u.admin_id = a.id
                ORDER BY u.id
            """)
            pasarguard_users = cursor.fetchall()
        
        # Create username -> user mapping for Pasarguard
        pasarguard_user_map_by_username = {user['username']: user for user in pasarguard_users}
        # Also create ID -> user mapping as fallback
        pasarguard_user_map_by_id = {user['id']: user for user in pasarguard_users}
        
        logger.info(f"Pasarguard users: {len(pasarguard_users)} (unique usernames: {len(pasarguard_user_map_by_username)}, unique IDs: {len(pasarguard_user_map_by_id)})")
        
        # Cache admin prefixes to avoid repeated database queries
        logger.info("Caching admin subscription URL prefixes...")
        admin_prefix_cache = {}
        unique_admin_ids = set()
        for user in pasarguard_users:
            admin_id = user.get('admin_id')
            if admin_id is not None:
                unique_admin_ids.add(admin_id)
        
        # Pre-fetch all admin prefixes
        for admin_id in unique_admin_ids:
            admin_prefix_cache[admin_id] = get_pasarguard_subscription_url_prefix(admin_id, pasarguard_conn)
        
        # Also cache the default (None) prefix
        admin_prefix_cache[None] = get_pasarguard_subscription_url_prefix(None, pasarguard_conn)
        
        logger.info(f"Cached prefixes for {len(admin_prefix_cache)} admin IDs")
        
        # Generate mappings
        mappings = []
        not_found = []
        matched_by_id = 0
        matched_by_username = 0
        
        logger.info(f"Processing {len(marzneshin_users)} users...")
        for idx, marz_user in enumerate(marzneshin_users, 1):
            if idx % 100 == 0 or idx == len(marzneshin_users):
                logger.info(f"  Processed {idx}/{len(marzneshin_users)} users...")
            
            username = marz_user['username']
            user_key = marz_user['key']
            admin_id = marz_user.get('admin_id')
            marz_user_id = marz_user['id']
            
            # Generate old Marzneshin URL
            prefix = marz_user.get('admin_subscription_url_prefix') or ""
            # Replace * with random hex (for consistency, we'll use a fixed salt per user)
            salt = secrets.token_hex(8)
            marzneshin_prefix = prefix.replace("*", salt) if prefix else ""
            old_url = f"{marzneshin_prefix}/{marzneshin_subscription_path}/{username}/{user_key}".strip("/")
            if not old_url.startswith("http"):
                # If no prefix, just the path
                old_url = f"/{marzneshin_subscription_path}/{username}/{user_key}"
            
            # Try to find user in Pasarguard - first by username, then by ID
            pg_user = None
            match_method = None
            
            if username in pasarguard_user_map_by_username:
                pg_user = pasarguard_user_map_by_username[username]
                match_method = "username"
                matched_by_username += 1
            elif marz_user_id in pasarguard_user_map_by_id:
                pg_user = pasarguard_user_map_by_id[marz_user_id]
                match_method = "id"
                matched_by_id += 1
                logger.debug(f"Matched user ID {marz_user_id} by ID (username differs: '{username}' vs '{pg_user.get('username')}')")
            
            # Generate new Pasarguard URL
            if pg_user:
                pg_admin_id = pg_user.get('admin_id')
                pg_username = pg_user.get('username', username)  # Use Pasarguard username for token generation
                
                # Get URL prefix from cache
                pg_prefix = admin_prefix_cache.get(pg_admin_id, admin_prefix_cache.get(None, ""))
                pg_salt = secrets.token_hex(8)
                pasarguard_prefix = pg_prefix.replace("*", pg_salt) if pg_prefix else ""
                
                # Generate token using Pasarguard username
                token = create_pasarguard_subscription_token(pg_username, jwt_secret)
                
                new_url = f"{pasarguard_prefix}/{pasarguard_subscription_path}/{token}".strip("/")
                if not new_url.startswith("http"):
                    new_url = f"/{pasarguard_subscription_path}/{token}"
                
                # Build mapping entry - only include pasarguard username if different
                mapping_entry = {
                    "username": username,
                    "user_id": marz_user_id,  # Single user_id since both IDs are always the same
                    "old_subscription_url": old_url,
                    "new_subscription_url": new_url
                }
                
                # Only include pasarguard username if it differs from marzneshin username
                if pg_username != username:
                    mapping_entry["username_pasarguard"] = pg_username
                
                # Only include matched_by if not matched by username (to save space)
                if match_method != "username":
                    mapping_entry["matched_by"] = match_method
                
                mappings.append(mapping_entry)
            else:
                not_found.append({
                    "username": username,
                    "user_id": marz_user_id,
                    "old_subscription_url": old_url
                })
        
        logger.info(f"Matched {matched_by_username} users by username, {matched_by_id} users by ID")
        
        result = {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_users": len(marzneshin_users),
            "mapped_users": len(mappings),
            "not_found_users": len(not_found),
            "url_formats": {
                "old_format": f"/{marzneshin_subscription_path}/{{username}}/{{key}}",
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
        marzneshin_conn.close()
        pasarguard_conn.close()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate subscription URL mapping from Marzneshin to Pasarguard'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='subscription_url_mapping.json',
        help='Output JSON file path (default: subscription_url_mapping.json)'
    )
    parser.add_argument(
        '--marzneshin-path',
        type=str,
        default='sub',
        help='Marzneshin subscription path (default: sub)'
    )
    parser.add_argument(
        '--pasarguard-path',
        type=str,
        default='sub',
        help='Pasarguard subscription path (default: sub)'
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
            marzneshin_subscription_path=args.marzneshin_path,
            pasarguard_subscription_path=args.pasarguard_path
        )
    except Exception as e:
        logger.error(f"Failed to generate mapping: {e}")
        raise


if __name__ == '__main__':
    main()

