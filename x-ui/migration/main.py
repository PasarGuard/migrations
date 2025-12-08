"""
Main migration orchestration script for x-ui to Pasarguard.
"""

import logging
import sqlite3
import time
from typing import Dict, List, Any, Optional
from pathlib import Path

from .config import (
    XUI_CONFIG,
    PASARGUARD_CONFIG,
    MIGRATION_CONFIG,
    TABLE_ORDER,
    EXCLUDE_TABLES,
    SQLiteConfig,
)
from . import config as config_module
from .extractors import XUIExtractor
from .transformers import DataConverter, DataValidator
from .loaders import PasarguardLoader
from .models.schemas import get_pasarguard_schema, get_column_info, table_exists
from .models.mappings import get_target_table
from .utils import setup_logging, confirm_action, print_statistics, format_duration

logger = logging.getLogger(__name__)


class MigrationOrchestrator:
    """Main migration orchestrator."""
    
    def __init__(self, schema_db_path: Optional[str] = None):
        """
        Initialize orchestrator.
        
        Args:
            schema_db_path: Optional path to schema reference database (read-only)
        """
        self.extractor: Optional[XUIExtractor] = None
        self.loader: Optional[PasarguardLoader] = None
        self.schema_conn: Optional[sqlite3.Connection] = None
        self.schema_db_path = schema_db_path
        self.converter = DataConverter()
        self.validator = DataValidator()
        self.source_data: Dict[str, List[Dict[str, Any]]] = {}
        self.statistics = {
            'start_time': None,
            'end_time': None,
            'duration': 0,
            'tables_migrated': 0,
            'total_rows_migrated': 0,
            'total_rows_failed': 0,
            'table_stats': {}
        }
    
    def run(self):
        """
        Run the complete migration process.
        """
        setup_logging(
            level=MIGRATION_CONFIG.log_level,
            log_file=MIGRATION_CONFIG.log_file
        )
        
        logger.info("=" * 70)
        logger.info("X-UI TO PASARGUARD MIGRATION")
        logger.info("=" * 70)
        
        self.statistics['start_time'] = time.time()
        
        try:
            # Step 1: Extract source data from x-ui.db
            logger.info("\n[STEP 1] Extracting source data from x-ui.db...")
            self._extract_from_database()
            
            # Step 2: Validate references
            logger.info("\n[STEP 2] Building reference maps for validation...")
            self.validator.build_reference_sets(self.source_data)
            
            # Step 3: Connect to Pasarguard output database
            logger.info("\n[STEP 3] Connecting to Pasarguard output database...")
            self.loader = PasarguardLoader(config_module.PASARGUARD_CONFIG)
            self.loader.connect()
            
            # Step 4: Copy schema from reference database if needed
            logger.info("\n[STEP 4] Checking and copying schema...")
            if self.schema_db_path:
                schema_db_path = Path(self.schema_db_path)
                if schema_db_path.exists():
                    logger.info(f"Schema reference database found: {schema_db_path}")
                    # Check if output DB has tables
                    output_tables = self._get_table_list(self.loader.conn)
                    if not output_tables:
                        logger.info("Output database is empty, copying schema from reference database...")
                        self._copy_schema_from_reference(str(schema_db_path), self.loader.conn)
                        logger.info("✓ Schema copied successfully")
                    else:
                        logger.info(f"Output database already has {len(output_tables)} tables, skipping schema copy")
                    # Connect to schema DB for reading schema
                    self.schema_conn = sqlite3.connect(str(schema_db_path))
                    self.schema_conn.row_factory = sqlite3.Row
                    target_schema = get_pasarguard_schema(self.schema_conn)
                else:
                    logger.warning(f"Schema reference database not found: {schema_db_path}, using output database schema")
                    target_schema = get_pasarguard_schema(self.loader.conn)
            else:
                target_schema = get_pasarguard_schema(self.loader.conn)
            logger.info(f"Found {len(target_schema)} tables in target database")
            
            # Step 5: Migrate tables
            logger.info("\n[STEP 5] Migrating tables (inserting columns only)...")
            logger.info("NOTE: This migration only inserts columns - tables must already exist!")
            self._migrate_tables(target_schema)
            
            # Step 6: Print summary
            self.statistics['end_time'] = time.time()
            self.statistics['duration'] = self.statistics['end_time'] - self.statistics['start_time']
            self._print_summary()
            
            logger.info("\n✓ Migration completed successfully!")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}", exc_info=True)
            raise
        
        finally:
            # Cleanup
            if self.loader:
                self.loader.disconnect()
            if self.extractor:
                self.extractor.disconnect()
            if self.schema_conn:
                self.schema_conn.close()
                self.schema_conn = None
    
    def _get_table_list(self, conn) -> List[str]:
        """Get list of tables in database."""
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]
    
    def _copy_schema_from_reference(self, source_db_path: str, target_conn: sqlite3.Connection):
        """Copy schema and initial data from reference database to target database."""
        source_conn = sqlite3.connect(source_db_path)
        source_conn.row_factory = sqlite3.Row
        
        try:
            # Get all CREATE TABLE statements from source
            source_cursor = source_conn.cursor()
            source_cursor.execute("""
                SELECT sql FROM sqlite_master 
                WHERE type='table' AND sql IS NOT NULL
                ORDER BY name
            """)
            
            target_cursor = target_conn.cursor()
            
            for row in source_cursor.fetchall():
                create_sql = row[0]
                if create_sql:
                    try:
                        target_cursor.execute(create_sql)
                        logger.debug(f"Created table from schema")
                    except sqlite3.OperationalError as e:
                        # Table might already exist, skip
                        logger.debug(f"Skipping table creation (may already exist): {e}")
            
            # Copy indexes
            source_cursor.execute("""
                SELECT sql FROM sqlite_master 
                WHERE type='index' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
            """)
            
            for row in source_cursor.fetchall():
                create_sql = row[0]
                if create_sql:
                    try:
                        target_cursor.execute(create_sql)
                        logger.debug(f"Created index from schema")
                    except sqlite3.OperationalError as e:
                        logger.debug(f"Skipping index creation: {e}")
            
            # Copy initial data from system tables that need it
            # These tables need at least one row for Pasarguard to work
            system_tables = ['settings', 'system', 'jwt', 'alembic_version']
            
            for table_name in system_tables:
                try:
                    # Check if table exists in source
                    source_cursor.execute(f"SELECT COUNT(*) as count FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    if source_cursor.fetchone()['count'] == 0:
                        continue
                    
                    # Get columns from source table
                    source_cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                    columns = [col[1] for col in source_cursor.fetchall()]
                    if not columns:
                        continue
                    
                    # Check if target table is empty
                    target_cursor.execute(f"SELECT COUNT(*) as count FROM `{table_name}`")
                    result = target_cursor.fetchone()
                    target_count = result[0] if isinstance(result, tuple) else result['count']
                    if target_count > 0:
                        logger.debug(f"Table {table_name} already has data, skipping")
                        continue
                    
                    # Copy all rows from source to target
                    source_cursor.execute(f"SELECT * FROM `{table_name}`")
                    rows = source_cursor.fetchall()
                    
                    if rows:
                        col_names = ', '.join([f'`{col}`' for col in columns])
                        placeholders = ', '.join(['?' for _ in columns])
                        
                        for row in rows:
                            values = [row[col] for col in columns]
                            target_cursor.execute(
                                f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})",
                                values
                            )
                        logger.info(f"Copied {len(rows)} row(s) from {table_name}")
                except Exception as e:
                    logger.warning(f"Failed to copy data from {table_name}: {e}")
            
            target_conn.commit()
        finally:
            source_conn.close()
    
    def _extract_from_database(self):
        """Extract data from x-ui SQLite database."""
        self.extractor = XUIExtractor(config_module.XUI_CONFIG)
        self.extractor.connect()
        
        # Get all table data
        self.source_data = self.extractor.extract_all_tables()
        
        # Log statistics
        stats = {table: len(rows) for table, rows in self.source_data.items()}
        logger.info(f"Extracted data from {len(self.source_data)} tables")
        for table, count in sorted(stats.items()):
            logger.info(f"  {table}: {count} rows")
    
    def _migrate_tables(self, target_schema: Dict[str, Dict[str, Any]]):
        """Migrate all tables in correct order."""
        table_mapping = {
            'inbounds': 'inbounds',
            'client_traffics': 'users',
        }
        
        for xui_table, pasarguard_table in table_mapping.items():
            if xui_table not in self.source_data:
                logger.info(f"[SKIP] {xui_table} (no source data)")
                continue
            
            # Check if target table exists
            if not self.loader.table_exists(pasarguard_table):
                logger.warning(f"[SKIP] {pasarguard_table} (table not found in target)")
                continue
            
            source_rows = self.source_data.get(xui_table, [])
            if not source_rows:
                logger.info(f"[SKIP] {xui_table} -> {pasarguard_table} (no source data)")
                continue
            
            # Migrate this table
            self._migrate_table(
                xui_table,
                pasarguard_table,
                source_rows,
                target_schema.get(pasarguard_table, {})
            )
            
            if xui_table == 'inbounds':
                if self.loader.table_exists('core_configs'):
                    logger.info(f"\n[MIGRATE] {xui_table} -> core_configs (creating single core_config with {len(source_rows)} inbounds)")
                    # Delete any existing core_configs first to ensure only one exists
                    cursor = self.loader.conn.cursor()
                    cursor.execute("DELETE FROM core_configs")
                    deleted_count = cursor.rowcount
                    if deleted_count > 0:
                        logger.info(f"  Deleted {deleted_count} existing core_config(s)")
                    self.loader.conn.commit()
                    
                    self._migrate_table(
                        xui_table,
                        'core_configs',
                        source_rows,
                        target_schema.get('core_configs', {}),
                        skip_fk_validation=True
                    )
        
        # Create default group and associations
        self._create_groups_and_associations(target_schema)
    
    def _migrate_table(
        self,
        source_table: str,
        target_table: str,
        source_rows: List[Dict[str, Any]],
        target_columns: Dict[str, Any],
        skip_fk_validation: bool = False
    ):
        """Migrate a single table."""
        logger.info(f"\n[MIGRATE] {source_table} -> {target_table} ({len(source_rows)} rows)")
        
        table_start = time.time()
        
        try:
            if skip_fk_validation:
                validated_rows = source_rows
                logger.info("  Skipping foreign key validation")
            else:
                logger.info("  Validating foreign keys...")
                validated_rows = self.validator.validate_foreign_keys(target_table, source_rows)
                if len(validated_rows) < len(source_rows):
                    logger.info(f"  Filtered {len(source_rows) - len(validated_rows)} rows with invalid foreign keys")
            
            logger.info("  Converting data...")
            converted_rows = self.converter.convert_table(
                source_table,
                validated_rows,
                target_columns,
                self.source_data,
                target_table
            )
            
            logger.info("  Validating required fields...")
            final_rows = self.validator.validate_required_fields(
                target_table,
                converted_rows,
                target_columns
            )
            
            logger.info(f"  Loading {len(final_rows)} rows...")
            ignore_duplicates = target_table in ['inbounds']
            replace_existing = target_table in ['admins', 'users', 'core_configs']
            
            success, failed = self.loader.load_table(
                target_table,
                final_rows,
                ignore_duplicates=ignore_duplicates,
                replace_existing=replace_existing
            )
            
            # Update statistics
            self.statistics['tables_migrated'] += 1
            self.statistics['total_rows_migrated'] += success
            self.statistics['total_rows_failed'] += failed
            self.statistics['table_stats'][target_table] = {
                'source_rows': len(source_rows),
                'migrated': success,
                'failed': failed,
                'duration': time.time() - table_start
            }
            
            logger.info(f"  ✓ Loaded {success}/{len(final_rows)} rows")
            if failed > 0:
                logger.warning(f"  ✗ Failed to load {failed} rows")
        
        except Exception as e:
            logger.error(f"  ✗ Failed to migrate {target_table}: {e}")
            self.statistics['table_stats'][target_table] = {
                'source_rows': len(source_rows),
                'error': str(e)
            }
    
    def _create_groups_and_associations(self, target_schema: Dict[str, Dict[str, Any]]):
        """Create a group for each inbound and associations for inbounds and users."""
        logger.info("\n[CREATE] Creating groups and associations...")
        
        # Check if groups table exists
        if not self.loader.table_exists('groups'):
            logger.warning("Groups table does not exist, skipping group creation")
            return
        
        cursor = self.loader.conn.cursor()
        
        try:
            # Get all inbound IDs that were migrated
            inbound_ids = []
            inbound_data = {}
            if 'inbounds' in self.source_data:
                for row in self.source_data['inbounds']:
                    if 'id' in row:
                        inbound_id = row['id']
                        inbound_ids.append(inbound_id)
                        inbound_data[inbound_id] = row
            
            if not inbound_ids:
                logger.info("No inbounds found, skipping group creation")
                return
            
            logger.info(f"Creating groups for {len(inbound_ids)} inbounds...")
            
            # Get columns from groups table
            groups_columns = target_schema.get('groups', {})
            
            # Create a group for each inbound
            inbound_to_group_map = {}
            for inbound_id in inbound_ids:
                inbound_info = inbound_data.get(inbound_id, {})
                tag = inbound_info.get('tag', f'inbound-{inbound_id}')
                remark = inbound_info.get('remark', '')
                
                # Create group name from inbound tag or remark
                group_name = tag if tag else f'inbound-{inbound_id}'
                group_description = f'Group for inbound {inbound_id}' + (f' ({remark})' if remark else '')
                
                # Check if group already exists
                cursor.execute("SELECT id FROM groups WHERE name = ?", (group_name,))
                existing_group = cursor.fetchone()
                
                if existing_group:
                    group_id = existing_group[0] if isinstance(existing_group, tuple) else existing_group['id']
                    logger.debug(f"Group '{group_name}' already exists with ID {group_id}")
                else:
                    # Create new group
                    col_names = [col for col in groups_columns.keys() if col in ['name', 'description', 'created_at']]
                    
                    # Build INSERT statement
                    insert_cols = [col for col in col_names if col not in ['id']]
                    col_str = ', '.join([f'`{col}`' for col in insert_cols])
                    placeholders = ', '.join(['?' for _ in insert_cols])
                    values = []
                    
                    for col in insert_cols:
                        if col == 'created_at':
                            from datetime import datetime, timezone
                            values.append(datetime.now(timezone.utc))
                        elif col == 'name':
                            values.append(group_name)
                        elif col == 'description':
                            values.append(group_description)
                        else:
                            values.append(None)
                    
                    cursor.execute(f"INSERT INTO groups ({col_str}) VALUES ({placeholders})", values)
                    group_id = cursor.lastrowid
                    logger.info(f"Created group '{group_name}' (ID: {group_id}) for inbound {inbound_id}")
                
                inbound_to_group_map[inbound_id] = group_id
            
            self.loader.conn.commit()
            
            # Create inbound-group associations
            if self.loader.table_exists('inbounds_groups_association'):
                # Check existing associations
                cursor.execute("SELECT inbound_id, group_id FROM inbounds_groups_association")
                existing_associations = {(row[0], row[1]) for row in cursor.fetchall()}
                
                new_associations = []
                for inbound_id, group_id in inbound_to_group_map.items():
                    if (inbound_id, group_id) not in existing_associations:
                        new_associations.append((inbound_id, group_id))
                
                if new_associations:
                    cursor.executemany(
                        "INSERT INTO inbounds_groups_association (inbound_id, group_id) VALUES (?, ?)",
                        new_associations
                    )
                    logger.info(f"Created {len(new_associations)} inbound-group associations")
            
            # Create user-group associations
            # Users should be associated with the group of their inbound
            if self.loader.table_exists('users_groups_association') and 'client_traffics' in self.source_data:
                # Map users to their inbound groups
                user_inbound_map = {}
                for row in self.source_data['client_traffics']:
                    if 'id' in row and 'inbound_id' in row:
                        user_id = row['id']
                        inbound_id = row['inbound_id']
                        if inbound_id in inbound_to_group_map:
                            user_inbound_map[user_id] = inbound_to_group_map[inbound_id]
                
                if user_inbound_map:
                    # Check existing associations
                    cursor.execute("SELECT user_id, groups_id FROM users_groups_association")
                    existing_user_associations = {(row[0], row[1]) for row in cursor.fetchall()}
                    
                    new_user_associations = []
                    for user_id, group_id in user_inbound_map.items():
                        if (user_id, group_id) not in existing_user_associations:
                            new_user_associations.append((user_id, group_id))
                    
                    if new_user_associations:
                        cursor.executemany(
                            "INSERT INTO users_groups_association (user_id, groups_id) VALUES (?, ?)",
                            new_user_associations
                        )
                        logger.info(f"Created {len(new_user_associations)} user-group associations")
            
            self.loader.conn.commit()
            logger.info(f"✓ Created {len(inbound_to_group_map)} groups and associations successfully")
            
        except Exception as e:
            logger.error(f"Failed to create groups and associations: {e}")
            self.loader.conn.rollback()
            raise
    
    def _print_summary(self):
        """Print migration summary."""
        duration_str = format_duration(self.statistics['duration'])
        
        summary = {
            'Duration': duration_str,
            'Tables Migrated': self.statistics['tables_migrated'],
            'Total Rows Migrated': self.statistics['total_rows_migrated'],
            'Total Rows Failed': self.statistics['total_rows_failed'],
        }
        
        print_statistics(summary, "MIGRATION SUMMARY")
        
        # Print per-table statistics
        if self.statistics['table_stats']:
            print("\nPer-Table Statistics:")
            print("-" * 70)
            for table, stats in self.statistics['table_stats'].items():
                if 'error' in stats:
                    print(f"  {table}: ERROR - {stats['error']}")
                else:
                    print(
                        f"  {table}: {stats['source_rows']} -> "
                        f"{stats['migrated']} migrated, {stats['failed']} failed "
                        f"({format_duration(stats['duration'])})"
                    )
            print("-" * 70)


def main():
    """Main entry point."""
    import argparse
    
    # Get default paths relative to script location
    script_dir = Path(__file__).parent.parent
    default_input_db = script_dir / 'x-ui.db'
    default_schema_db = script_dir / 'input-db-pg' / 'db.sqlite3'
    default_output_folder = script_dir / 'output-db'
    
    parser = argparse.ArgumentParser(
        description='Migrate x-ui data to Pasarguard'
    )
    parser.add_argument(
        '--input-db',
        type=str,
        default=str(default_input_db),
        help=f'Path to input x-ui database (default: {default_input_db})'
    )
    parser.add_argument(
        '--schema-db',
        type=str,
        default=str(default_schema_db),
        help=f'Path to Pasarguard schema reference database (read-only, default: {default_schema_db})'
    )
    parser.add_argument(
        '--output-folder',
        type=str,
        default=str(default_output_folder),
        help=f'Path to output folder for Pasarguard database (default: {default_output_folder})'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log to file'
    )
    
    args = parser.parse_args()
    
    # Validate input database exists
    input_db_path = Path(args.input_db)
    if not input_db_path.exists():
        raise FileNotFoundError(f"Input database not found: {input_db_path}")
    
    # Create output folder if it doesn't exist
    output_folder = Path(args.output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    
    # Set output database path
    output_db_path = output_folder / 'db.sqlite3'
    
    # Update configuration from args
    config_module.XUI_CONFIG = SQLiteConfig(db_path=str(input_db_path))
    config_module.PASARGUARD_CONFIG = SQLiteConfig(db_path=str(output_db_path))
    
    if args.log_level:
        MIGRATION_CONFIG.log_level = args.log_level
    if args.log_file:
        MIGRATION_CONFIG.log_file = args.log_file
    
    # Pass schema database path to orchestrator
    orchestrator = MigrationOrchestrator(schema_db_path=args.schema_db)
    
    try:
        orchestrator.run()
    except KeyboardInterrupt:
        logger.info("\nMigration interrupted by user")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


if __name__ == '__main__':
    main()

