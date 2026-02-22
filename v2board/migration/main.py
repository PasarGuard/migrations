"""
Main migration orchestration script for v2board SQL dump -> Pasarguard.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import MIGRATION_CONFIG, PASARGUARD_CONFIG, V2BOARD_CONFIG, SQLiteConfig, V2BoardConfig
from .extractors import V2BoardExtractor
from .loaders import PasarguardLoader
from .models.schemas import get_pasarguard_schema
from .transformers import DataConverter, DataValidator
from .utils import format_duration, print_statistics, setup_logging

logger = logging.getLogger(__name__)


class MigrationOrchestrator:
    """Main migration orchestrator."""

    def __init__(self, schema_db_path: Optional[str] = None):
        self.extractor: Optional[V2BoardExtractor] = None
        self.loader: Optional[PasarguardLoader] = None
        self.schema_conn: Optional[sqlite3.Connection] = None

        self.schema_db_path = schema_db_path
        self.converter = DataConverter()
        self.validator = DataValidator()

        self.source_data: Dict[str, List[Dict[str, Any]]] = {}
        self.converted_data: Dict[str, List[Dict[str, Any]]] = {}

        self.statistics = {
            "start_time": None,
            "end_time": None,
            "duration": 0,
            "tables_migrated": 0,
            "total_rows_migrated": 0,
            "total_rows_failed": 0,
            "table_stats": {},
        }

    def run(self):
        setup_logging(level=MIGRATION_CONFIG.log_level, log_file=MIGRATION_CONFIG.log_file)

        logger.info("=" * 70)
        logger.info("V2BOARD TO PASARGUARD MIGRATION")
        logger.info("=" * 70)

        self.statistics["start_time"] = time.time()

        try:
            logger.info("\n[STEP 1] Extracting source data from SQL dump...")
            self._extract_source_data()

            logger.info("\n[STEP 2] Connecting to output database...")
            self.loader = PasarguardLoader(PASARGUARD_CONFIG)
            self.loader.connect()

            logger.info("\n[STEP 3] Ensuring target schema exists...")
            target_schema = self._prepare_target_schema()
            logger.info("Found %d table(s) in target schema", len(target_schema))

            logger.info("\n[STEP 4] Converting source data...")
            self.converted_data = self.converter.convert_all(self.source_data)
            self._log_converted_statistics()

            logger.info("\n[STEP 5] Clearing destination migration tables...")
            self._clear_target_tables()

            logger.info("\n[STEP 6] Loading converted data...")
            self._load_converted_data(target_schema)

            self.statistics["end_time"] = time.time()
            self.statistics["duration"] = self.statistics["end_time"] - self.statistics["start_time"]

            self._print_summary()
            logger.info("\n✓ Migration completed successfully")

        except Exception as exc:
            logger.error("Migration failed: %s", exc, exc_info=True)
            raise

        finally:
            if self.loader:
                self.loader.disconnect()
            if self.extractor:
                self.extractor.disconnect()
            if self.schema_conn:
                self.schema_conn.close()
                self.schema_conn = None

    def _extract_source_data(self):
        self.extractor = V2BoardExtractor(V2BOARD_CONFIG)
        self.source_data = self.extractor.extract_all_tables()

    def _prepare_target_schema(self) -> Dict[str, Dict[str, Any]]:
        if not self.loader:
            raise RuntimeError("Loader is not initialized")

        target_tables = self._get_table_list(self.loader.conn)

        schema_db_path = Path(self.schema_db_path) if self.schema_db_path else None
        if schema_db_path and schema_db_path.exists() and not target_tables:
            logger.info("Output database is empty. Copying schema from %s", schema_db_path)
            self._copy_schema_from_reference(str(schema_db_path), self.loader.conn)
            target_tables = self._get_table_list(self.loader.conn)

        if schema_db_path and schema_db_path.exists():
            self.schema_conn = sqlite3.connect(str(schema_db_path))
            self.schema_conn.row_factory = sqlite3.Row
            return get_pasarguard_schema(self.schema_conn)

        return get_pasarguard_schema(self.loader.conn)

    def _copy_schema_from_reference(self, source_db_path: str, target_conn: sqlite3.Connection):
        source_conn = sqlite3.connect(source_db_path)
        source_conn.row_factory = sqlite3.Row

        try:
            src_cursor = source_conn.cursor()
            dst_cursor = target_conn.cursor()

            src_cursor.execute(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type='table' AND sql IS NOT NULL
                ORDER BY name
                """
            )

            for row in src_cursor.fetchall():
                create_sql = row[0]
                if not create_sql:
                    continue
                try:
                    dst_cursor.execute(create_sql)
                except sqlite3.OperationalError:
                    # Table may already exist.
                    pass

            src_cursor.execute(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type='index' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
                """
            )

            for row in src_cursor.fetchall():
                create_sql = row[0]
                if not create_sql:
                    continue
                try:
                    dst_cursor.execute(create_sql)
                except sqlite3.OperationalError:
                    pass

            # Copy seed/system tables so output DB is directly runnable.
            for table_name in ["settings", "system", "jwt", "alembic_version"]:
                if not self._table_exists_sqlite(source_conn, table_name):
                    continue
                if not self._table_exists_sqlite(target_conn, table_name):
                    continue

                dst_cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                if dst_cursor.fetchone()[0] > 0:
                    continue

                src_cursor.execute(f"SELECT * FROM `{table_name}`")
                rows = src_cursor.fetchall()
                if not rows:
                    continue

                src_cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                columns = [col[1] for col in src_cursor.fetchall()]
                column_sql = ", ".join(f"`{col}`" for col in columns)
                placeholders = ", ".join("?" for _ in columns)

                for row in rows:
                    values = [row[col] for col in columns]
                    dst_cursor.execute(
                        f"INSERT INTO `{table_name}` ({column_sql}) VALUES ({placeholders})",
                        values,
                    )

            target_conn.commit()
            logger.info("Schema copy completed")

        finally:
            source_conn.close()

    def _clear_target_tables(self):
        if not self.loader:
            raise RuntimeError("Loader is not initialized")

        delete_order = [
            "users_groups_association",
            "inbounds_groups_association",
            "users",
            "admins",
            "inbounds",
            "groups",
            "core_configs",
        ]

        cursor = self.loader.conn.cursor()
        cursor.execute("PRAGMA foreign_keys = OFF")
        try:
            for table in delete_order:
                if not self.loader.table_exists(table):
                    continue
                cursor.execute(f"DELETE FROM `{table}`")
                logger.info("  cleared table %s", table)
            self.loader.conn.commit()
        finally:
            cursor.execute("PRAGMA foreign_keys = ON")

    def _load_converted_data(self, target_schema: Dict[str, Dict[str, Any]]):
        if not self.loader:
            raise RuntimeError("Loader is not initialized")

        table_order = [
            "admins",
            "groups",
            "users",
            "inbounds",
            "core_configs",
            "users_groups_association",
            "inbounds_groups_association",
        ]

        for table in table_order:
            rows = self.converted_data.get(table, [])
            if not rows:
                logger.info("[SKIP] %s (no converted rows)", table)
                continue

            if not self.loader.table_exists(table):
                logger.warning("[SKIP] %s (table not found in target)", table)
                continue

            target_columns = target_schema.get(table, {})
            final_rows = self.validator.validate_required_fields(table, rows, target_columns)

            ignore_duplicates = table in {"users_groups_association", "inbounds_groups_association"}
            replace_existing = table in {"admins", "users", "core_configs", "groups", "inbounds"}

            logger.info("[LOAD] %s (%d row(s))", table, len(final_rows))
            table_start = time.time()

            success, failed = self.loader.load_table(
                table,
                final_rows,
                ignore_duplicates=ignore_duplicates,
                replace_existing=replace_existing,
            )

            self.statistics["tables_migrated"] += 1
            self.statistics["total_rows_migrated"] += success
            self.statistics["total_rows_failed"] += failed
            self.statistics["table_stats"][table] = {
                "source_rows": len(rows),
                "migrated": success,
                "failed": failed,
                "duration": time.time() - table_start,
            }

    def _log_converted_statistics(self):
        logger.info("Converted table statistics:")
        for table, rows in sorted(self.converted_data.items()):
            logger.info("  %s: %d row(s)", table, len(rows))

    def _print_summary(self):
        duration_str = format_duration(self.statistics["duration"])
        summary = {
            "Duration": duration_str,
            "Tables Migrated": self.statistics["tables_migrated"],
            "Total Rows Migrated": self.statistics["total_rows_migrated"],
            "Total Rows Failed": self.statistics["total_rows_failed"],
        }
        print_statistics(summary, "MIGRATION SUMMARY")

        if self.statistics["table_stats"]:
            print("\nPer-Table Statistics:")
            print("-" * 70)
            for table, stats in self.statistics["table_stats"].items():
                print(
                    f"  {table}: {stats['source_rows']} -> "
                    f"{stats['migrated']} migrated, {stats['failed']} failed "
                    f"({format_duration(stats['duration'])})"
                )
            print("-" * 70)

    def _get_table_list(self, conn: sqlite3.Connection) -> List[str]:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]

    def _table_exists_sqlite(self, conn: sqlite3.Connection, table: str) -> bool:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        return cursor.fetchone() is not None


def main():
    import argparse

    script_dir = Path(__file__).parent.parent
    default_input_sql = script_dir.parent / "sql_multiserver_.sql"
    default_schema_db = script_dir / "input-db-pg" / "db.sqlite3"
    default_output_folder = script_dir / "output-db"

    parser = argparse.ArgumentParser(description="Migrate v2board SQL dump to Pasarguard")
    parser.add_argument(
        "--input-sql",
        type=str,
        default=str(default_input_sql),
        help=f"Path to input v2board SQL dump (default: {default_input_sql})",
    )
    parser.add_argument(
        "--schema-db",
        type=str,
        default=str(default_schema_db),
        help=f"Path to Pasarguard schema reference database (default: {default_schema_db})",
    )
    parser.add_argument(
        "--output-folder",
        type=str,
        default=str(default_output_folder),
        help=f"Path to output folder for Pasarguard database (default: {default_output_folder})",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    parser.add_argument("--log-file", type=str, help="Optional path to a log file")

    args = parser.parse_args()

    input_sql_path = Path(args.input_sql)
    if not input_sql_path.exists():
        raise FileNotFoundError(f"Input SQL dump not found: {input_sql_path}")

    output_folder = Path(args.output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    output_db_path = output_folder / "db.sqlite3"

    # Update runtime config objects.
    import migration.config as config_module

    config_module.V2BOARD_CONFIG = V2BoardConfig(sql_dump_path=str(input_sql_path))
    config_module.PASARGUARD_CONFIG = SQLiteConfig(db_path=str(output_db_path))

    if args.log_level:
        MIGRATION_CONFIG.log_level = args.log_level
    if args.log_file:
        MIGRATION_CONFIG.log_file = args.log_file

    orchestrator = MigrationOrchestrator(schema_db_path=args.schema_db)

    try:
        orchestrator.run()
    except KeyboardInterrupt:
        logger.info("\nMigration interrupted by user")
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
