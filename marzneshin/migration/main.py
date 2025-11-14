"""
Main migration orchestration script.
"""

import logging
import time
from typing import Dict, List, Any, Optional

from migration.config import (
    MARZNESHIN_CONFIG,
    PASARGUARD_CONFIG,
    MIGRATION_CONFIG,
    TABLE_ORDER,
    EXCLUDE_TABLES,
    PASARGUARD_TABLES
)
from migration.extractors import MarzneshinExtractor
from migration.transformers import DataConverter, DataValidator
from migration.loaders import PasarguardLoader
from migration.models.schemas import get_pasarguard_schema, get_column_info, table_exists
from migration.models.mappings import get_target_table
from migration.utils import setup_logging, confirm_action, print_statistics, format_duration
from migration.generate_subscription_url_mapping import generate_subscription_url_mapping

logger = logging.getLogger(__name__)


class MigrationOrchestrator:
    """Main migration orchestrator."""
    
    def __init__(self):
        """Initialize orchestrator."""
        self.extractor: Optional[MarzneshinExtractor] = None
        self.loader: Optional[PasarguardLoader] = None
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
        logger.info("MARZNESHIN TO PASARGUARD MIGRATION")
        logger.info("=" * 70)
        
        self.statistics['start_time'] = time.time()
        
        try:
            # Step 1: Extract source data
            logger.info("\n[STEP 1] Extracting source data...")
            self._extract_from_database()
            
            # Step 2: Validate references
            logger.info("\n[STEP 2] Building reference maps for validation...")
            self.validator.build_reference_sets(self.source_data)
            
            # Step 3: Connect to Pasarguard
            logger.info("\n[STEP 3] Connecting to Pasarguard database...")
            self.loader = PasarguardLoader(PASARGUARD_CONFIG)
            self.loader.connect()
            
            # Step 4: Get target schema
            logger.info("\n[STEP 4] Analyzing target schema...")
            target_schema = get_pasarguard_schema(self.loader.conn)
            logger.info(f"Found {len(target_schema)} tables in target database")
            
            # Step 5: Clear existing data (with confirmation)
            logger.info("\n[STEP 5] Clearing existing data...")
            if confirm_action("This will delete ALL data in Pasarguard. Continue?"):
                self._clear_target_data()
            else:
                logger.info("Migration cancelled by user")
                return
            
            # Step 6: Migrate tables
            logger.info("\n[STEP 6] Migrating tables...")
            self._migrate_tables(target_schema)
            
            # Step 7: Reset auto-increments
            logger.info("\n[STEP 7] Resetting auto-increment values...")
            self.loader.reset_all_auto_increments()
            logger.info("✓ Auto-increment values reset")
            
            # Step 8: Clean up extra tables
            logger.info("\n[STEP 8] Cleaning up extra tables...")
            self.loader.cleanup_extra_tables(PASARGUARD_TABLES)
            logger.info("✓ Database schema cleanup complete")
            
            # Step 9: Set Alembic version
            if MIGRATION_CONFIG.set_alembic_version:
                logger.info("\n[STEP 9] Setting Alembic version...")
                current_version = self.loader.get_alembic_version()
                if current_version:
                    logger.info(f"  Current version: {current_version}")
                self.loader.update_alembic_version(MIGRATION_CONFIG.alembic_version)
                logger.info(f"✓ Alembic version set to {MIGRATION_CONFIG.alembic_version}")
            else:
                logger.info("\n[STEP 9] Skipping Alembic version update (disabled in config)")
            
            # Step 10: Insert default settings
            logger.info("\n[STEP 10] Inserting default settings...")
            self.loader.insert_default_settings()
            
            # Step 11: Apply missing schema changes
            logger.info("\n[STEP 11] Applying missing schema changes...")
            self.loader.add_missing_admin_columns()
            self.loader.add_missing_user_columns()
            self.loader.add_missing_user_template_columns()
            self.loader.add_missing_node_columns()
            self.loader.add_missing_host_columns()
            self.loader.fix_hosts_null_paths()
            self.loader.fix_settings_default_flow()
            
            # Step 12: Generate subscription URL mapping
            logger.info("\n[STEP 12] Generating subscription URL mapping...")
            try:
                output_file = getattr(MIGRATION_CONFIG, 'url_mapping_output_file', 'subscription_url_mapping.json')
                marzneshin_path = getattr(MIGRATION_CONFIG, 'marzneshin_subscription_path', 'sub')
                pasarguard_path = getattr(MIGRATION_CONFIG, 'pasarguard_subscription_path', 'sub')
                generate_subscription_url_mapping(
                    output_file=output_file,
                    marzneshin_subscription_path=marzneshin_path,
                    pasarguard_subscription_path=pasarguard_path
                )
                logger.info("✓ Subscription URL mapping generated successfully")
            except Exception as e:
                logger.error(f"Failed to generate subscription URL mapping: {e}")
                logger.warning("Continuing despite URL mapping generation failure...")
            
            # Step 13: Print summary
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
    
    def _extract_from_database(self):
        """Extract data from Marzneshin database."""
        self.extractor = MarzneshinExtractor(MARZNESHIN_CONFIG)
        self.extractor.connect()
        
        # Get all table data
        self.source_data = self.extractor.extract_all_tables()
        
        # Special extraction for admin_usage_logs (doesn't exist in Marzneshin, computed from node_user_usages)
        if 'admin_usage_logs' not in EXCLUDE_TABLES:
            logger.info("Extracting admin_usage_logs from node_user_usages...")
            admin_usage_logs = self.extractor.extract_admin_usage_logs()
            self.source_data['admin_usage_logs'] = admin_usage_logs
        
        # Log statistics
        stats = {table: len(rows) for table, rows in self.source_data.items()}
        logger.info(f"Extracted data from {len(self.source_data)} tables")
        for table, count in sorted(stats.items()):
            logger.info(f"  {table}: {count} rows")
    
    def _clear_target_data(self):
        """Clear all target tables."""
        # Get tables that exist in target
        existing_tables = [
            table for table in TABLE_ORDER
            if self.loader.table_exists(table) and table not in EXCLUDE_TABLES
        ]
        
        logger.info(f"Clearing {len(existing_tables)} tables...")
        self.loader.clear_all_tables(existing_tables)
        logger.info("✓ All tables cleared")
    
    def _migrate_tables(self, target_schema: Dict[str, Dict[str, Any]]):
        """Migrate all tables in correct order."""
        for table in TABLE_ORDER:
            if table in EXCLUDE_TABLES:
                logger.info(f"[SKIP] {table} (excluded)")
                continue
            
            # Get target table name
            target_table = get_target_table(table)
            
            # Check if target table exists
            if not self.loader.table_exists(target_table):
                logger.warning(f"[SKIP] {target_table} (table not found in target)")
                continue
            
            # Get source data - check both the table name and any mapped source names
            source_rows = self.source_data.get(table, [])
            if not source_rows:
                # Try mapped table name
                if table != target_table:
                    source_rows = self.source_data.get(target_table, [])
                
                # Special case: core_configs comes from inbounds
                if not source_rows and target_table == "core_configs":
                    source_rows = self.source_data.get("inbounds", [])
                    table = "inbounds"  # Use inbounds for conversion
                
                # Special case: groups comes from services
                if not source_rows and target_table == "groups":
                    source_rows = self.source_data.get("services", [])
                    table = "services"  # Update table name for proper conversion
                
                # Special case: users_groups_association comes from users_services
                if not source_rows and target_table == "users_groups_association":
                    source_rows = self.source_data.get("users_services", [])
                    table = "users_services"
                
                # Special case: inbounds_groups_association comes from inbounds_services
                if not source_rows and target_table == "inbounds_groups_association":
                    source_rows = self.source_data.get("inbounds_services", [])
                    table = "inbounds_services"
                
                # Special case: admin_usage_logs is computed from node_user_usages (already extracted)
                if not source_rows and target_table == "admin_usage_logs":
                    source_rows = self.source_data.get("admin_usage_logs", [])
                    table = "admin_usage_logs"
                
                if not source_rows:
                    logger.info(f"[SKIP] {table} -> {target_table} (no source data)")
                    continue
            
            # Migrate this table
            self._migrate_table(
                table,
                target_table,
                source_rows,
                target_schema.get(target_table, {})
            )
    
    def _migrate_table(
        self,
        source_table: str,
        target_table: str,
        source_rows: List[Dict[str, Any]],
        target_columns: Dict[str, Any]
    ):
        """Migrate a single table."""
        logger.info(f"\n[MIGRATE] {source_table} -> {target_table} ({len(source_rows)} rows)")
        
        table_start = time.time()
        
        try:
            # Step 1: Validate foreign keys
            logger.info("  Validating foreign keys...")
            validated_rows = self.validator.validate_foreign_keys(target_table, source_rows)
            if len(validated_rows) < len(source_rows):
                logger.info(f"  Filtered {len(source_rows) - len(validated_rows)} rows with invalid foreign keys")
            
            # Step 2: Convert data
            logger.info("  Converting data...")
            converted_rows = self.converter.convert_table(
                source_table,
                validated_rows,
                target_columns,
                self.source_data,
                target_table
            )
            
            # Step 3: Validate required fields
            logger.info("  Validating required fields...")
            final_rows = self.validator.validate_required_fields(
                target_table,
                converted_rows,
                target_columns
            )
            
            # Step 4: Load into target
            logger.info(f"  Loading {len(final_rows)} rows...")
            
            # Use INSERT IGNORE for tables that might have duplicates
            ignore_duplicates = target_table in [
                'inbounds',  # Can have duplicates if migration is re-run
                'node_usages', 'node_user_usages',
                'admin_usage_logs', 'user_usage_logs', 'node_stats'
            ]
            
            success, failed = self.loader.load_table(
                target_table,
                final_rows,
                ignore_duplicates=ignore_duplicates
            )
            
            # Update validator with actual IDs from database for tables that use INSERT IGNORE
            # This ensures foreign key validation uses actual database IDs, not just source data
            if ignore_duplicates and target_table == "inbounds":
                logger.info("  Updating validator with actual inbound IDs from database...")
                self.validator.update_inbound_ids_from_database(self.loader.conn)
            
            # Update admin IDs after admins are loaded so users can reference them
            if target_table == "admins":
                logger.info("  Updating validator with actual admin IDs from database...")
                self.validator.update_admin_ids_from_database(self.loader.conn)
            
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
    
    parser = argparse.ArgumentParser(
        description='Migrate Marzneshin data to Pasarguard'
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
    parser.add_argument(
        '--exclude-tables',
        type=str,
        help='Comma-separated list of table names to exclude from migration (e.g., "node_user_usages,node_usages")'
    )
    parser.add_argument(
        '--max-usage-rows',
        type=int,
        help='Maximum rows to extract from usage tables (default: 100000, 0 = no limit)'
    )
    parser.add_argument(
        '--url-mapping-output',
        type=str,
        default='subscription_url_mapping.json',
        help='Output file for subscription URL mapping (default: subscription_url_mapping.json)'
    )
    parser.add_argument(
        '--marzneshin-subscription-path',
        type=str,
        default='sub',
        help='Marzneshin subscription path (default: sub)'
    )
    parser.add_argument(
        '--pasarguard-subscription-path',
        type=str,
        default='sub',
        help='Pasarguard subscription path (default: sub)'
    )
    
    args = parser.parse_args()
    
    # Update configuration from args
    if args.log_level:
        MIGRATION_CONFIG.log_level = args.log_level
    if args.log_file:
        MIGRATION_CONFIG.log_file = args.log_file
    if args.max_usage_rows is not None:
        MIGRATION_CONFIG.max_usage_table_rows = args.max_usage_rows
    # Always set URL mapping config (generation is now automatic)
    MIGRATION_CONFIG.url_mapping_output_file = args.url_mapping_output
    MIGRATION_CONFIG.marzneshin_subscription_path = args.marzneshin_subscription_path
    MIGRATION_CONFIG.pasarguard_subscription_path = args.pasarguard_subscription_path
    
    # Add excluded tables from command line
    if args.exclude_tables:
        excluded = [t.strip() for t in args.exclude_tables.split(',') if t.strip()]
        EXCLUDE_TABLES.update(excluded)
        # Initialize logging early to show excluded tables
        setup_logging(
            level=MIGRATION_CONFIG.log_level,
            log_file=MIGRATION_CONFIG.log_file
        )
        logger.info(f"Excluding tables from command line: {', '.join(excluded)}")
    
    orchestrator = MigrationOrchestrator()
    
    try:
        # Full migration
        orchestrator.run()
    except KeyboardInterrupt:
        logger.info("\nMigration interrupted by user")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


if __name__ == '__main__':
    main()

