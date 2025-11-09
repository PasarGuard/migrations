"""
Database loader for Pasarguard database.
"""

import json
import logging
from typing import Dict, List, Any, Optional
import pymysql
from pymysql.cursors import DictCursor

from migration.config import DatabaseConfig, MIGRATION_CONFIG

logger = logging.getLogger(__name__)


class PasarguardLoader:
    """Load data into Pasarguard database."""
    
    def __init__(self, config: DatabaseConfig):
        """
        Initialize loader.
        
        Args:
            config: Database configuration
        """
        self.config = config
        self.conn: Optional[pymysql.Connection] = None
        self.batch_size = MIGRATION_CONFIG.batch_size
    
    def connect(self):
        """Connect to Pasarguard database."""
        try:
            logger.info(f"Connecting to Pasarguard at {self.config.host}:{self.config.port}...")
            self.conn = pymysql.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                charset=self.config.charset,
                cursorclass=DictCursor,
                connect_timeout=10,  # 10 second timeout
                read_timeout=300,  # 5 minute read timeout (for large operations)
                write_timeout=300  # 5 minute write timeout (for ALTER TABLE operations)
            )
            logger.info(f"✓ Connected to Pasarguard database at {self.config.host}")
        except pymysql.err.OperationalError as e:
            logger.error(f"✗ Cannot connect to Pasarguard database:")
            logger.error(f"  Host: {self.config.host}:{self.config.port}")
            logger.error(f"  Database: {self.config.database}")
            logger.error(f"  User: {self.config.user}")
            logger.error(f"  Error: {e}")
            raise ConnectionError(f"Failed to connect to Pasarguard: {e}")
        except Exception as e:
            logger.error(f"✗ Unexpected error connecting to Pasarguard: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Disconnected from Pasarguard database")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def clear_table(self, table: str):
        """
        Clear all data from a table.
        
        Args:
            table: Table name
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE `{table}`")
            self.conn.commit()
            logger.info(f"Cleared table {table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to clear table {table}: {e}")
            raise
    
    def clear_all_tables(self, tables: List[str]):
        """
        Clear all tables in reverse order (to respect foreign keys).
        
        Args:
            tables: List of table names in dependency order
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        # Disable foreign key checks
        with self.conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        try:
            for table in reversed(tables):
                try:
                    self.clear_table(table)
                except Exception as e:
                    logger.warning(f"Could not clear {table}: {e}")
        finally:
            # Re-enable foreign key checks
            with self.conn.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    
    def load_table(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        ignore_duplicates: bool = False
    ) -> tuple[int, int]:
        """
        Load data into a table.
        
        Args:
            table: Table name
            rows: List of row dictionaries
            ignore_duplicates: Whether to ignore duplicate key errors
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        if not rows:
            logger.info(f"No data to load for {table}")
            return (0, 0)
        
        logger.info(f"Loading {len(rows)} rows into {table}")
        
        success_count = 0
        fail_count = 0
        
        # Process in batches
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i:i + self.batch_size]
            batch_success, batch_fail = self._load_batch(
                table, batch, ignore_duplicates
            )
            success_count += batch_success
            fail_count += batch_fail
        
        logger.info(f"Loaded {success_count}/{len(rows)} rows into {table}")
        if fail_count > 0:
            logger.warning(f"Failed to load {fail_count} rows into {table}")
        
        return (success_count, fail_count)
    
    def _load_batch(
        self,
        table: str,
        batch: List[Dict[str, Any]],
        ignore_duplicates: bool = False
    ) -> tuple[int, int]:
        """Load a batch of rows."""
        if not batch:
            return (0, 0)
        
        # Get columns from first row
        columns = list(batch[0].keys())
        
        # Build INSERT query
        sql = self._build_insert_query(table, columns, ignore_duplicates)
        
        # Convert rows to tuples, handling special types
        values = []
        for row in batch:
            row_values = []
            for col in columns:
                value = row.get(col)
                # Special handling for hosts.path - Pasarguard requires string, not None
                # Use '/' as default instead of empty string to avoid None conversion issues
                if table == "hosts" and col == "path" and (value is None or (isinstance(value, str) and not value.strip())):
                    value = "/"
                # Special handling for hosts.status - EnumArray requires '[]' for empty, not empty string
                elif table == "hosts" and col == "status":
                    if value is None or value == '':
                        value = '[]'  # Empty array string representation for EnumArray
                    # Ensure it's a string (EnumArray is stored as comma-separated string)
                    elif not isinstance(value, str):
                        value = '[]'  # Default to empty array if not a string
                # Handle JSON fields
                elif isinstance(value, dict):
                    value = json.dumps(value)
                # Handle set fields (for StringArray columns)
                elif isinstance(value, set):
                    value = ','.join(sorted(value)) if value else None
                row_values.append(value)
            values.append(tuple(row_values))
        
        try:
            with self.conn.cursor() as cursor:
                cursor.executemany(sql, values)
            self.conn.commit()
            return (len(batch), 0)
            
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"Batch insert failed for {table}: {e}")
            
            # Retry row by row
            return self._retry_batch_row_by_row(table, batch, sql)
    
    def _retry_batch_row_by_row(
        self,
        table: str,
        batch: List[Dict[str, Any]],
        sql: str
    ) -> tuple[int, int]:
        """Retry failed batch row by row."""
        success_count = 0
        fail_count = 0
        
        columns = list(batch[0].keys())
        
        for row in batch:
            # Handle special types (JSON, sets)
            row_values = []
            for col in columns:
                value = row.get(col)
                # Special handling for hosts.path - Pasarguard requires string, not None
                # Use '/' as default instead of empty string to avoid None conversion issues
                if table == "hosts" and col == "path" and (value is None or (isinstance(value, str) and not value.strip())):
                    value = "/"
                # Special handling for hosts.status - EnumArray requires '[]' for empty, not empty string
                elif table == "hosts" and col == "status":
                    if value is None or value == '':
                        value = '[]'  # Empty array string representation for EnumArray
                    # Ensure it's a string (EnumArray is stored as comma-separated string)
                    elif not isinstance(value, str):
                        value = '[]'  # Default to empty array if not a string
                # Handle JSON fields
                elif isinstance(value, dict):
                    value = json.dumps(value)
                # Handle set fields (for StringArray columns)
                elif isinstance(value, set):
                    value = ','.join(sorted(value)) if value else None
                row_values.append(value)
            values = tuple(row_values)
            
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, values)
                self.conn.commit()
                success_count += 1
                
            except Exception as e:
                self.conn.rollback()
                fail_count += 1
                
                if fail_count <= 3:  # Log first 3 errors
                    logger.error(f"Failed to insert row in {table}: {e}")
                    logger.debug(f"Failed row: {row}")
        
        return (success_count, fail_count)
    
    def _build_insert_query(
        self,
        table: str,
        columns: List[str],
        ignore_duplicates: bool = False
    ) -> str:
        """Build INSERT query."""
        escaped_table = f"`{table}`"
        escaped_columns = [f"`{col}`" for col in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        
        if ignore_duplicates:
            return (
                f"INSERT IGNORE INTO {escaped_table} "
                f"({', '.join(escaped_columns)}) "
                f"VALUES ({placeholders})"
            )
        else:
            return (
                f"INSERT INTO {escaped_table} "
                f"({', '.join(escaped_columns)}) "
                f"VALUES ({placeholders})"
            )
    
    def get_max_id(self, table: str, id_column: str = 'id') -> int:
        """
        Get maximum ID from a table.
        
        Args:
            table: Table name
            id_column: ID column name
            
        Returns:
            Maximum ID value
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT MAX(`{id_column}`) as max_id FROM `{table}`")
            result = cursor.fetchone()
            return result['max_id'] if result and result['max_id'] else 0
    
    def reset_auto_increment(self, table: str, id_column: str = 'id'):
        """
        Reset auto-increment value for a table.
        
        Args:
            table: Table name
            id_column: ID column name
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        max_id = self.get_max_id(table, id_column)
        if max_id > 0:
            next_id = max_id + 1
            
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(f"ALTER TABLE `{table}` AUTO_INCREMENT = {next_id}")
                self.conn.commit()
                logger.info(f"Reset auto-increment for {table}.{id_column} to {next_id}")
            except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
                # Connection lost - try to reconnect and continue
                try:
                    if self.conn:
                        self.conn.rollback()
                except:
                    pass  # Connection already lost, can't rollback
                logger.warning(f"Failed to reset auto-increment for {table}: {e}")
                # Try to reconnect
                try:
                    self.conn.ping(reconnect=True)
                except:
                    logger.warning(f"Could not reconnect for {table}, skipping...")
            except Exception as e:
                try:
                    self.conn.rollback()
                except:
                    pass
                logger.warning(f"Failed to reset auto-increment for {table}: {e}")
    
    def reset_all_auto_increments(self):
        """Reset auto-increment for all tables with auto-increment columns."""
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT TABLE_NAME, COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND EXTRA LIKE '%auto_increment%'
            """)
            
            tables = cursor.fetchall()
        
        for row in tables:
            table = row['TABLE_NAME']
            column = row['COLUMN_NAME']
            self.reset_auto_increment(table, column)
    
    def table_exists(self, table: str) -> bool:
        """
        Check if table exists.
        
        Args:
            table: Table name
            
        Returns:
            True if exists
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = %s
            """, (table,))
            
            result = cursor.fetchone()
            return result['count'] > 0 if result else False
    
    def get_alembic_version(self) -> Optional[str]:
        """
        Get current Alembic version from database.
        
        Returns:
            Current Alembic version string or None if not set
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        if not self.table_exists('alembic_version'):
            logger.warning("alembic_version table does not exist")
            return None
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT version_num FROM alembic_version LIMIT 1")
                result = cursor.fetchone()
                return result['version_num'] if result else None
        except Exception as e:
            logger.warning(f"Failed to get Alembic version: {e}")
            return None
    
    def update_alembic_version(self, version: str):
        """
        Update Alembic version in database.
        
        This sets the alembic_version to indicate which migrations have been applied.
        Call this after successful migration to mark the database as up-to-date.
        
        Args:
            version: Alembic version string (e.g., '5943013d0e49')
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        if not self.table_exists('alembic_version'):
            logger.warning("alembic_version table does not exist, skipping version update")
            return
        
        try:
            with self.conn.cursor() as cursor:
                # Delete existing version
                cursor.execute("DELETE FROM alembic_version")
                # Insert new version
                cursor.execute("INSERT INTO alembic_version (version_num) VALUES (%s)", (version,))
            self.conn.commit()
            logger.info(f"Updated Alembic version to {version}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to update Alembic version: {e}")
            raise
    
    def get_all_tables(self) -> list[str]:
        """
        Get list of all tables in database.
        
        Returns:
            List of table names
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_TYPE = 'BASE TABLE'
            """)
            
            results = cursor.fetchall()
            return [row['TABLE_NAME'] for row in results]
    
    def drop_table(self, table: str):
        """
        Drop a table from the database.
        
        Args:
            table: Table name to drop
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
            self.conn.commit()
            logger.info(f"Dropped table: {table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to drop table {table}: {e}")
            raise
    
    def cleanup_extra_tables(self, valid_tables: set):
        """
        Drop all tables that are not in the valid tables list.
        
        This ensures the database only contains tables that match PasarGuard's schema.
        
        Args:
            valid_tables: Set of valid table names to keep
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        logger.info("Checking for extra tables to clean up...")
        
        # Get all tables in database
        all_tables = self.get_all_tables()
        
        # Find tables to drop
        tables_to_drop = [table for table in all_tables if table not in valid_tables]
        
        if not tables_to_drop:
            logger.info("✓ No extra tables found - database schema is clean")
            return
        
        logger.warning(f"Found {len(tables_to_drop)} extra tables that will be dropped:")
        for table in tables_to_drop:
            logger.warning(f"  - {table}")
        
        # Disable foreign key checks to allow dropping tables with dependencies
        with self.conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        try:
            # Drop each extra table
            dropped_count = 0
            for table in tables_to_drop:
                try:
                    self.drop_table(table)
                    dropped_count += 1
                except Exception as e:
                    logger.error(f"Failed to drop {table}: {e}")
            
            logger.info(f"✓ Dropped {dropped_count}/{len(tables_to_drop)} extra tables")
            
        finally:
            # Re-enable foreign key checks
            with self.conn.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    
    def insert_default_settings(self):
        """
        Insert default settings row if it doesn't exist.
        This is required for PasarGuard to start properly.
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            # Check if settings table exists, create if not
            if not self.table_exists('settings'):
                logger.info("Settings table doesn't exist, creating it...")
                with self.conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE `settings` (
                            `id` INT NOT NULL AUTO_INCREMENT,
                            `telegram` JSON NOT NULL,
                            `discord` JSON NOT NULL,
                            `webhook` JSON NOT NULL,
                            `notification_settings` JSON NOT NULL,
                            `notification_enable` JSON NOT NULL,
                            `subscription` JSON NOT NULL,
                            `general` JSON NOT NULL,
                            PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """)
                self.conn.commit()
                logger.info("✓ Created settings table")
            
            with self.conn.cursor() as cursor:
                # Check if settings row already exists
                cursor.execute("SELECT COUNT(*) as count FROM settings")
                result = cursor.fetchone()
                
                if result['count'] > 0:
                    logger.info(f"Settings table already has {result['count']} row(s), skipping default insertion")
                    return
                
                # Insert default settings
                default_settings = {
                    'telegram': '{"enable": false, "token": null, "webhook_url": null, "webhook_secret": null, "proxy_url": null, "method": "webhook", "mini_app_login": true, "mini_app_web_url": "", "for_admins_only": true}',
                    'discord': '{"enable": false, "token": null, "proxy_url": null}',
                    'webhook': '{"enable": false, "webhooks": [], "days_left": [], "usage_percent": [], "timeout": 10, "recurrent": 1, "proxy_url": null}',
                    'notification_settings': '{"notify_telegram": false, "notify_discord": false, "telegram_api_token": null, "telegram_admin_id": null, "telegram_channel_id": null, "telegram_topic_id": null, "discord_webhook_url": null, "proxy_url": null, "max_retries": 3}',
                    'notification_enable': '{"admin": {"create": true, "modify": true, "delete": true, "reset_usage": true, "login": true}, "core": {"create": true, "modify": true, "delete": true}, "group": {"create": true, "modify": true, "delete": true}, "host": {"create": true, "modify": true, "delete": true, "modify_hosts": true}, "node": {"create": true, "modify": true, "delete": true, "connect": true, "error": true}, "user": {"create": true, "modify": true, "delete": true, "status_change": true, "reset_data_usage": true, "data_reset_by_next": true, "subscription_revoked": true}, "user_template": {"create": true, "modify": true, "delete": true}, "days_left": true, "percentage_reached": true}',
                    'subscription': '{"url_prefix": "", "update_interval": 12, "support_url": "https://t.me/", "profile_title": "Subscription", "host_status_filter": true, "rules": [], "manual_sub_request": {"links": true, "links_base64": true, "xray": true, "sing_box": true, "clash": true, "clash_meta": true, "outline": true}, "applications": []}',
                    'general': '{"default_flow": "", "default_method": "chacha20-ietf-poly1305"}'
                }
                
                cursor.execute(
                    """
                    INSERT INTO settings 
                    (telegram, discord, webhook, notification_settings, notification_enable, subscription, `general`)
                    VALUES (%(telegram)s, %(discord)s, %(webhook)s, %(notification_settings)s, 
                            %(notification_enable)s, %(subscription)s, %(general)s)
                    """,
                    default_settings
                )
            
            self.conn.commit()
            logger.info("✓ Inserted default settings row")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert default settings: {e}")
            raise
    
    def fix_settings_default_flow(self):
        """
        Fix invalid default_flow value in settings table.
        Changes 'none' to '' (empty string) to match PasarGuard's enum.
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            if not self.table_exists('settings'):
                logger.info("Settings table doesn't exist, skipping default_flow fix")
                return
            
            with self.conn.cursor() as cursor:
                # Check if any settings have invalid default_flow
                cursor.execute("SELECT id, general FROM settings")
                rows = cursor.fetchall()
                
                if not rows:
                    logger.info("No settings found, skipping default_flow fix")
                    return
                
                import json
                fixed_count = 0
                
                for row in rows:
                    general = row['general']
                    if isinstance(general, str):
                        general = json.loads(general)
                    
                    # Check if default_flow is 'none' (invalid)
                    if general.get('default_flow') == 'none':
                        logger.info(f"Fixing invalid default_flow in settings row {row['id']}...")
                        general['default_flow'] = ''  # Change to empty string
                        
                        # Update the row
                        cursor.execute(
                            "UPDATE settings SET general = %s WHERE id = %s",
                            (json.dumps(general), row['id'])
                        )
                        fixed_count += 1
                        logger.info(f"✓ Fixed default_flow in settings row {row['id']}")
                
                if fixed_count > 0:
                    self.conn.commit()
                    logger.info(f"✓ Fixed default_flow in {fixed_count} settings row(s)")
                else:
                    logger.info("Settings default_flow values are already correct")
                    
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to fix settings default_flow: {e}")
            raise
    
    def add_missing_node_columns(self):
        """
        Add missing columns to nodes table that exist in PasarGuard but not in Marzneshin.
        This is needed because we're setting the Alembic version without running all migrations.
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            if not self.table_exists('nodes'):
                logger.info("Nodes table doesn't exist, skipping missing columns")
                return
            
            with self.conn.cursor() as cursor:
                # Check which columns exist
                cursor.execute("DESCRIBE nodes")
                existing_columns = {row['Field'] for row in cursor.fetchall()}
                
                columns_added = []
                
                # Add api_key column if missing
                if 'api_key' not in existing_columns:
                    logger.info("Adding missing 'api_key' column to nodes table...")
                    cursor.execute("""
                        ALTER TABLE nodes 
                        ADD COLUMN `api_key` VARCHAR(36) NULL
                    """)
                    columns_added.append('api_key')
                    logger.info("✓ Added api_key column to nodes table")
                
                # Add core_config_id column if missing
                if 'core_config_id' not in existing_columns:
                    logger.info("Adding missing 'core_config_id' column to nodes table...")
                    # First check if core_configs table exists
                    if self.table_exists('core_configs'):
                        cursor.execute("""
                            ALTER TABLE nodes 
                            ADD COLUMN `core_config_id` INT NULL,
                            ADD CONSTRAINT `fk_nodes_core_config_id` 
                            FOREIGN KEY (`core_config_id`) 
                            REFERENCES `core_configs`(`id`) 
                            ON DELETE SET NULL
                        """)
                    else:
                        # Add without foreign key constraint if core_configs doesn't exist
                        cursor.execute("""
                            ALTER TABLE nodes 
                            ADD COLUMN `core_config_id` INT NULL
                        """)
                    columns_added.append('core_config_id')
                    logger.info("✓ Added core_config_id column to nodes table")
                
                # Add max_logs column if missing
                if 'max_logs' not in existing_columns:
                    logger.info("Adding missing 'max_logs' column to nodes table...")
                    cursor.execute("""
                        ALTER TABLE nodes 
                        ADD COLUMN `max_logs` BIGINT NOT NULL DEFAULT 1000
                    """)
                    columns_added.append('max_logs')
                    logger.info("✓ Added max_logs column to nodes table")
                
                # Add gather_logs column if missing
                if 'gather_logs' not in existing_columns:
                    logger.info("Adding missing 'gather_logs' column to nodes table...")
                    cursor.execute("""
                        ALTER TABLE nodes 
                        ADD COLUMN `gather_logs` TINYINT(1) NOT NULL DEFAULT 1
                    """)
                    columns_added.append('gather_logs')
                    logger.info("✓ Added gather_logs column to nodes table")
                
                # Log summary
                if columns_added:
                    logger.info(f"✓ Applied missing schema changes to nodes table: {', '.join(columns_added)}")
                else:
                    logger.info("Nodes table schema is up to date")
            
            self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to add missing node columns: {e}")
            raise
    
    def add_missing_host_columns(self):
        """
        Add missing columns to hosts table that are expected by the latest PasarGuard version.
        This is needed because we're setting the Alembic version without running all migrations.
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            if not self.table_exists('hosts'):
                logger.info("Hosts table doesn't exist, skipping missing columns")
                return
            
            with self.conn.cursor() as cursor:
                # Check which columns exist (with metadata to validate types)
                cursor.execute("DESCRIBE hosts")
                column_details = cursor.fetchall()
                existing_columns = {row['Field'] for row in column_details}
                column_info = {row['Field']: row for row in column_details}
                
                schema_changes = []
                
                def _is_enum_like(col_type: str) -> bool:
                    lowered = col_type.lower()
                    return lowered.startswith("enum(") or lowered.startswith("set(")
                
                # Add status column if missing
                if 'status' not in existing_columns:
                    logger.info("Adding missing 'status' column to hosts table...")
                    # Add as nullable first, update rows, then make non-nullable
                    cursor.execute("""
                        ALTER TABLE hosts 
                        ADD COLUMN `status` VARCHAR(60) NULL DEFAULT ''
                    """)
                    cursor.execute("UPDATE hosts SET status = '' WHERE status IS NULL")
                    schema_changes.append("status (added)")
                    logger.info("✓ Added status column to hosts table")
                    column_info['status'] = {'Type': 'varchar(60)', 'Null': 'YES', 'Default': ''}
                else:
                    status_col = column_info['status']
                    status_type = status_col['Type'].lower()
                    status_null = status_col['Null'] == 'YES'
                    status_default = status_col['Default']
                    needs_status_fix = (
                        _is_enum_like(status_type)
                        or not status_type.startswith('varchar(60)')
                        or not status_null
                        or status_default not in ('', None)
                    )
                    if needs_status_fix:
                        logger.info("Fixing 'status' column definition on hosts table...")
                        cursor.execute("""
                            ALTER TABLE hosts 
                            MODIFY COLUMN `status` VARCHAR(60) NULL DEFAULT ''
                        """)
                        schema_changes.append("status (type fixed)")
                    # Clean up legacy empty array values
                    cursor.execute("""
                        UPDATE hosts 
                        SET status = '' 
                        WHERE status IS NULL OR status IN ('[]', '{}')
                    """)
                
                # Add ech_config_list column if missing  
                if 'ech_config_list' not in existing_columns:
                    logger.info("Adding missing 'ech_config_list' column to hosts table...")
                    cursor.execute("""
                        ALTER TABLE hosts 
                        ADD COLUMN `ech_config_list` VARCHAR(512) DEFAULT NULL
                    """)
                    schema_changes.append('ech_config_list')
                    logger.info("✓ Added ech_config_list column to hosts table")
                
                # Ensure ALPN column matches PasarGuard expectations
                if 'alpn' not in existing_columns:
                    logger.info("Adding missing 'alpn' column to hosts table...")
                    cursor.execute("""
                        ALTER TABLE hosts 
                        ADD COLUMN `alpn` VARCHAR(14) NULL DEFAULT NULL
                    """)
                    schema_changes.append("alpn (added)")
                else:
                    alpn_col = column_info['alpn']
                    alpn_type = alpn_col['Type'].lower()
                    alpn_null = alpn_col['Null'] == 'YES'
                    alpn_default = alpn_col['Default']
                    needs_alpn_fix = (
                        _is_enum_like(alpn_type)
                        or not alpn_type.startswith('varchar(14)')
                        or not alpn_null
                        or alpn_default is not None
                    )
                    if needs_alpn_fix:
                        logger.info("Fixing 'alpn' column definition on hosts table...")
                        cursor.execute("""
                            ALTER TABLE hosts 
                            MODIFY COLUMN `alpn` VARCHAR(14) NULL DEFAULT NULL
                        """)
                        schema_changes.append("alpn (type fixed)")
                    cursor.execute("""
                        UPDATE hosts 
                        SET alpn = NULL 
                        WHERE alpn IN ('none', '', '[]')
                    """)
                
                # Log summary
                if schema_changes:
                    logger.info(f"✓ Applied hosts schema changes: {', '.join(schema_changes)}")
                else:
                    logger.info("Hosts table schema is up to date")
            
            self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to add missing host columns: {e}")
            raise
    
    def fix_hosts_null_paths(self):
        """
        Fix NULL path values in hosts table.
        Pasarguard requires path to be a string, not None.
        Also fixes empty strings which might be converted to None by Pasarguard.
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            if not self.table_exists('hosts'):
                logger.info("Hosts table doesn't exist, skipping path fix")
                return
            
            with self.conn.cursor() as cursor:
                # Update NULL paths to '/' (Pasarguard may convert empty strings to None)
                cursor.execute("UPDATE hosts SET path = '/' WHERE path IS NULL OR path = ''")
                rows_updated = cursor.rowcount
                
                if rows_updated > 0:
                    logger.info(f"✓ Fixed {rows_updated} hosts with NULL or empty path values (set to '/')")
                else:
                    logger.info("No hosts with NULL or empty path values found")
            
            self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to fix NULL paths in hosts table: {e}")
            raise
    
    def add_missing_user_columns(self):
        """
        Add missing columns to users table that are expected by the latest PasarGuard version.
        This is needed because we're setting the Alembic version without running all migrations.
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            if not self.table_exists('users'):
                logger.info("Users table doesn't exist, skipping missing columns")
                return
            
            with self.conn.cursor() as cursor:
                # Check which columns exist
                cursor.execute("DESCRIBE users")
                existing_columns = {row['Field'] for row in cursor.fetchall()}
                
                columns_added = []
                
                # Add proxy_settings column if missing
                if 'proxy_settings' not in existing_columns:
                    logger.info("Adding missing 'proxy_settings' column to users table...")
                    cursor.execute("""
                        ALTER TABLE users 
                        ADD COLUMN `proxy_settings` JSON NOT NULL DEFAULT ('{}')
                    """)
                    columns_added.append('proxy_settings')
                    logger.info("✓ Added proxy_settings column to users table")
                
                # Log summary
                if columns_added:
                    logger.info(f"✓ Applied missing schema changes to users table: {', '.join(columns_added)}")
                else:
                    logger.info("Users table schema is up to date")
            
            self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to add missing user columns: {e}")
            raise
    
    def add_missing_admin_columns(self):
        """
        Add missing columns to admins table that are expected by the latest PasarGuard version.
        This is needed because we're setting the Alembic version without running all migrations.
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            if not self.table_exists('admins'):
                logger.info("Admins table doesn't exist, skipping missing columns")
                return
            
            with self.conn.cursor() as cursor:
                # Check which columns exist
                cursor.execute("DESCRIBE admins")
                existing_columns = {row['Field'] for row in cursor.fetchall()}
                
                columns_added = []
                
                # Add discord_id column if missing
                if 'discord_id' not in existing_columns:
                    logger.info("Adding missing 'discord_id' column to admins table...")
                    cursor.execute("""
                        ALTER TABLE admins 
                        ADD COLUMN `discord_id` BIGINT DEFAULT NULL
                    """)
                    columns_added.append('discord_id')
                    logger.info("✓ Added discord_id column to admins table")
                
                # Add discord_webhook column if missing
                if 'discord_webhook' not in existing_columns:
                    logger.info("Adding missing 'discord_webhook' column to admins table...")
                    cursor.execute("""
                        ALTER TABLE admins 
                        ADD COLUMN `discord_webhook` VARCHAR(1024) DEFAULT NULL
                    """)
                    columns_added.append('discord_webhook')
                    logger.info("✓ Added discord_webhook column to admins table")
                
                # Add sub_template column if missing
                if 'sub_template' not in existing_columns:
                    logger.info("Adding missing 'sub_template' column to admins table...")
                    cursor.execute("""
                        ALTER TABLE admins 
                        ADD COLUMN `sub_template` VARCHAR(1024) DEFAULT NULL
                    """)
                    columns_added.append('sub_template')
                    logger.info("✓ Added sub_template column to admins table")
                
                # Add sub_domain column if missing
                if 'sub_domain' not in existing_columns:
                    logger.info("Adding missing 'sub_domain' column to admins table...")
                    cursor.execute("""
                        ALTER TABLE admins 
                        ADD COLUMN `sub_domain` VARCHAR(256) DEFAULT NULL
                    """)
                    columns_added.append('sub_domain')
                    logger.info("✓ Added sub_domain column to admins table")
                
                # Add profile_title column if missing
                if 'profile_title' not in existing_columns:
                    logger.info("Adding missing 'profile_title' column to admins table...")
                    cursor.execute("""
                        ALTER TABLE admins 
                        ADD COLUMN `profile_title` VARCHAR(512) DEFAULT NULL
                    """)
                    columns_added.append('profile_title')
                    logger.info("✓ Added profile_title column to admins table")
                
                # Add support_url column if missing
                if 'support_url' not in existing_columns:
                    logger.info("Adding missing 'support_url' column to admins table...")
                    cursor.execute("""
                        ALTER TABLE admins 
                        ADD COLUMN `support_url` VARCHAR(1024) DEFAULT NULL
                    """)
                    columns_added.append('support_url')
                    logger.info("✓ Added support_url column to admins table")
                
                # Handle used_traffic column (might be named users_usage in older versions)
                if 'used_traffic' not in existing_columns:
                    if 'users_usage' in existing_columns:
                        # Rename users_usage to used_traffic
                        logger.info("Renaming 'users_usage' to 'used_traffic' in admins table...")
                        cursor.execute("""
                            ALTER TABLE admins 
                            CHANGE COLUMN `users_usage` `used_traffic` BIGINT NOT NULL DEFAULT 0
                        """)
                        columns_added.append('used_traffic (renamed from users_usage)')
                        logger.info("✓ Renamed users_usage to used_traffic in admins table")
                    else:
                        # Add used_traffic column
                        logger.info("Adding missing 'used_traffic' column to admins table...")
                        cursor.execute("""
                            ALTER TABLE admins 
                            ADD COLUMN `used_traffic` BIGINT NOT NULL DEFAULT 0
                        """)
                        columns_added.append('used_traffic')
                        logger.info("✓ Added used_traffic column to admins table")
                
                # Add is_disabled column if missing
                if 'is_disabled' not in existing_columns:
                    logger.info("Adding missing 'is_disabled' column to admins table...")
                    cursor.execute("""
                        ALTER TABLE admins 
                        ADD COLUMN `is_disabled` TINYINT(1) NOT NULL DEFAULT 0
                    """)
                    columns_added.append('is_disabled')
                    logger.info("✓ Added is_disabled column to admins table")
                
                # Log summary
                if columns_added:
                    logger.info(f"✓ Applied missing schema changes to admins table: {', '.join(columns_added)}")
                else:
                    logger.info("Admins table schema is up to date")
            
            self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to add missing admin columns: {e}")
            raise
    
    def add_missing_user_template_columns(self):
        """
        Add missing columns to user_templates table that are expected by the latest PasarGuard version.
        This is needed because we're setting the Alembic version without running all migrations.
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            if not self.table_exists('user_templates'):
                logger.info("User templates table doesn't exist, skipping missing columns")
                return
            
            with self.conn.cursor() as cursor:
                # Check which columns exist
                cursor.execute("DESCRIBE user_templates")
                existing_columns = {row['Field'] for row in cursor.fetchall()}
                
                columns_added = []
                
                # Add extra_settings column if missing
                if 'extra_settings' not in existing_columns:
                    logger.info("Adding missing 'extra_settings' column to user_templates table...")
                    cursor.execute("""
                        ALTER TABLE user_templates 
                        ADD COLUMN `extra_settings` JSON DEFAULT NULL
                    """)
                    columns_added.append('extra_settings')
                    logger.info("✓ Added extra_settings column to user_templates table")
                
                # Add on_hold_timeout column if missing
                if 'on_hold_timeout' not in existing_columns:
                    logger.info("Adding missing 'on_hold_timeout' column to user_templates table...")
                    cursor.execute("""
                        ALTER TABLE user_templates 
                        ADD COLUMN `on_hold_timeout` INT DEFAULT NULL
                    """)
                    columns_added.append('on_hold_timeout')
                    logger.info("✓ Added on_hold_timeout column to user_templates table")
                
                # Add status column if missing
                if 'status' not in existing_columns:
                    logger.info("Adding missing 'status' column to user_templates table...")
                    cursor.execute("""
                        ALTER TABLE user_templates 
                        ADD COLUMN `status` ENUM('active', 'on_hold') NOT NULL DEFAULT 'active'
                    """)
                    columns_added.append('status')
                    logger.info("✓ Added status column to user_templates table")
                
                # Add reset_usages column if missing
                if 'reset_usages' not in existing_columns:
                    logger.info("Adding missing 'reset_usages' column to user_templates table...")
                    cursor.execute("""
                        ALTER TABLE user_templates 
                        ADD COLUMN `reset_usages` TINYINT(1) NOT NULL DEFAULT 0
                    """)
                    columns_added.append('reset_usages')
                    logger.info("✓ Added reset_usages column to user_templates table")
                
                # Add data_limit_reset_strategy column if missing
                if 'data_limit_reset_strategy' not in existing_columns:
                    logger.info("Adding missing 'data_limit_reset_strategy' column to user_templates table...")
                    cursor.execute("""
                        ALTER TABLE user_templates 
                        ADD COLUMN `data_limit_reset_strategy` ENUM('no_reset', 'day', 'week', 'month', 'year') 
                        NOT NULL DEFAULT 'no_reset'
                    """)
                    columns_added.append('data_limit_reset_strategy')
                    logger.info("✓ Added data_limit_reset_strategy column to user_templates table")
                
                # Add is_disabled column if missing
                if 'is_disabled' not in existing_columns:
                    logger.info("Adding missing 'is_disabled' column to user_templates table...")
                    cursor.execute("""
                        ALTER TABLE user_templates 
                        ADD COLUMN `is_disabled` TINYINT(1) NOT NULL DEFAULT 0
                    """)
                    columns_added.append('is_disabled')
                    logger.info("✓ Added is_disabled column to user_templates table")
                
                # Log summary
                if columns_added:
                    logger.info(f"✓ Applied missing schema changes to user_templates table: {', '.join(columns_added)}")
                else:
                    logger.info("User templates table schema is up to date")
            
            self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to add missing user_template columns: {e}")
            raise
