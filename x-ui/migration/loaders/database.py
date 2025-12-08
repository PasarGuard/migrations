"""
Database loader for Pasarguard database.
Only inserts columns - does NOT create tables.
"""

import json
import logging
from typing import Dict, List, Any, Optional
import sqlite3
from pathlib import Path

from ..config import SQLiteConfig, MIGRATION_CONFIG

logger = logging.getLogger(__name__)


class PasarguardLoader:
    """Load data into Pasarguard SQLite database - inserts columns only."""
    
    def __init__(self, config: SQLiteConfig):
        """
        Initialize loader.
        
        Args:
            config: SQLite database configuration
        """
        self.config = config
        self.conn: Optional[sqlite3.Connection] = None
        self.batch_size = MIGRATION_CONFIG.batch_size
    
    def connect(self):
        """Connect to Pasarguard SQLite database."""
        try:
            db_path = Path(self.config.db_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Connecting to Pasarguard database at {db_path}...")
            self.conn = sqlite3.connect(str(db_path))
            self.conn.row_factory = sqlite3.Row
            logger.info(f"✓ Connected to Pasarguard database")
        except Exception as e:
            logger.error(f"✗ Cannot connect to Pasarguard database:")
            logger.error(f"  Path: {self.config.db_path}")
            logger.error(f"  Error: {e}")
            raise ConnectionError(f"Failed to connect to Pasarguard: {e}")
    
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
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists."""
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (table,))
        result = cursor.fetchone()
        return result is not None
    
    def load_table(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        ignore_duplicates: bool = False,
        replace_existing: bool = False
    ) -> tuple[int, int]:
        """
        Load data into a table (insert columns only, table must exist).
        
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
        
        # Check if table exists
        if not self.table_exists(table):
            logger.error(f"Table {table} does not exist. Skipping insertion.")
            logger.error("This migration only inserts columns - tables must already exist.")
            return (0, len(rows))
        
        logger.info(f"Loading {len(rows)} rows into {table}")
        
        success_count = 0
        fail_count = 0
        
        # Process in batches
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i:i + self.batch_size]
            batch_success, batch_fail = self._load_batch(
                table, batch, ignore_duplicates, replace_existing
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
        ignore_duplicates: bool = False,
        replace_existing: bool = False
    ) -> tuple[int, int]:
        """Load a batch of rows."""
        if not batch:
            return (0, 0)
        
        # Get columns from first row
        columns = list(batch[0].keys())
        
        # Build INSERT query
        sql = self._build_insert_query(table, columns, ignore_duplicates, replace_existing)
        
        # Convert rows to tuples
        values = []
        for row in batch:
            row_values = []
            for col in columns:
                value = row.get(col)
                # Handle None values
                if value is None:
                    row_values.append(None)
                elif isinstance(value, (dict, list)):
                    # Convert dict/list to JSON string
                    row_values.append(json.dumps(value))
                else:
                    row_values.append(value)
            values.append(tuple(row_values))
        
        # Execute batch insert
        success_count = 0
        fail_count = 0
        
        try:
            cursor = self.conn.cursor()
            cursor.executemany(sql, values)
            self.conn.commit()
            success_count = len(batch)
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading batch into {table}: {e}")
            fail_count = len(batch)
        
        return (success_count, fail_count)
    
    def _build_insert_query(
        self,
        table: str,
        columns: List[str],
        ignore_duplicates: bool = False,
        replace_existing: bool = False
    ) -> str:
        """Build INSERT query for SQLite."""
        escaped_columns = [f"`{col}`" for col in columns]
        placeholders = ", ".join(["?"] * len(columns))  # SQLite uses ? placeholders
        
        if replace_existing:
            return f"INSERT OR REPLACE INTO `{table}` ({', '.join(escaped_columns)}) VALUES ({placeholders})"
        elif ignore_duplicates:
            return f"INSERT OR IGNORE INTO `{table}` ({', '.join(escaped_columns)}) VALUES ({placeholders})"
        else:
            return f"INSERT INTO `{table}` ({', '.join(escaped_columns)}) VALUES ({placeholders})"

