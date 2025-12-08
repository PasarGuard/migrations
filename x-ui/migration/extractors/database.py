"""
Database extractor for x-ui SQLite database.
"""

import logging
import sqlite3
import time
from typing import Dict, List, Any, Optional
from pathlib import Path

from ..config import SQLiteConfig, EXCLUDE_TABLES, MIGRATION_CONFIG

logger = logging.getLogger(__name__)


class XUIExtractor:
    """Extract data from x-ui SQLite database."""
    
    def __init__(self, config: SQLiteConfig):
        """
        Initialize extractor.
        
        Args:
            config: SQLite database configuration
        """
        self.config = config
        self.conn: Optional[sqlite3.Connection] = None
    
    def connect(self):
        """Connect to x-ui SQLite database."""
        try:
            db_path = Path(self.config.db_path)
            if not db_path.exists():
                raise FileNotFoundError(f"x-ui database not found at: {db_path}")
            
            logger.info(f"Connecting to x-ui database at {db_path}...")
            self.conn = sqlite3.connect(str(db_path))
            self.conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            logger.info(f"✓ Connected to x-ui database")
        except Exception as e:
            logger.error(f"✗ Cannot connect to x-ui database:")
            logger.error(f"  Path: {self.config.db_path}")
            logger.error(f"  Error: {e}")
            raise ConnectionError(f"Failed to connect to x-ui database: {e}")
    
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Disconnected from x-ui database")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def discover_tables(self) -> List[str]:
        """
        Discover all tables in the database.
        
        Returns:
            List of table names
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Filter out excluded tables
        tables = [t for t in tables if t not in EXCLUDE_TABLES]
        logger.info(f"Discovered {len(tables)} tables: {', '.join(tables)}")
        
        return tables
    
    def get_table_columns(self, table: str) -> List[str]:
        """
        Get column names for a table.
        
        Args:
            table: Table name
            
        Returns:
            List of column names
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info(`{table}`)")
        columns = [row[1] for row in cursor.fetchall()]
        return columns
    
    def extract_table(
        self, 
        table: str, 
        limit: Optional[int] = None, 
        max_rows: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract all data from a table.
        
        Args:
            table: Table name
            limit: Optional row limit
            max_rows: Maximum rows to extract
            
        Returns:
            List of rows as dictionaries
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            # Get columns
            columns = self.get_table_columns(table)
            if not columns:
                logger.warning(f"Table {table} has no columns")
                return []
            
            # Get total count first
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
            total_count = cursor.fetchone()[0]
            
            # Build query
            query = f"SELECT {', '.join(columns)} FROM `{table}`"
            
            # Add ordering if ID column exists
            if 'id' in columns:
                query += " ORDER BY `id`"
            
            # Add limit if specified
            if limit:
                query += f" LIMIT {limit}"
                total_count = limit
            elif max_rows and total_count > max_rows:
                logger.warning(f"  Table {table} has {total_count:,} rows. Limiting to {max_rows:,} rows...")
                if 'id' in columns:
                    query += f" LIMIT {max_rows}"
                total_count = max_rows
            
            # Execute query
            start_time = time.time()
            logger.info(f"  Fetching {total_count:,} rows from {table}...")
            
            cursor.execute(query)
            rows = []
            for row in cursor.fetchall():
                # Convert Row to dict
                row_dict = {}
                for idx, col in enumerate(columns):
                    row_dict[col] = row[idx]
                rows.append(row_dict)
            
            elapsed_time = time.time() - start_time
            logger.info(f"Extracted {len(rows)} rows from {table} in {elapsed_time:.2f}s")
            return rows
            
        except Exception as e:
            logger.error(f"Error extracting table {table}: {e}")
            raise
    
    def extract_all_tables(self, table_list: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from all tables.
        
        Args:
            table_list: Optional list of specific tables to extract
            
        Returns:
            Dictionary of {table_name: list_of_rows}
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        if table_list is None:
            table_list = self.discover_tables()
        
        data = {}
        total_tables = len(table_list)
        for idx, table in enumerate(table_list, 1):
            try:
                logger.info(f"Extracting table {idx}/{total_tables}: {table}...")
                data[table] = self.extract_table(table)
            except Exception as e:
                logger.error(f"Failed to extract {table}: {e}")
                data[table] = []
        
        return data
    
    def get_table_count(self, table: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table: Table name
            
        Returns:
            Number of rows
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
        result = cursor.fetchone()
        return result[0] if result else 0
    
    def extract_with_filter(
        self, 
        table: str, 
        where_clause: str, 
        params: tuple = ()
    ) -> List[Dict[str, Any]]:
        """
        Extract data with a WHERE clause filter.
        
        Args:
            table: Table name
            where_clause: WHERE clause (without WHERE keyword)
            params: Query parameters
            
        Returns:
            List of filtered rows
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        columns = self.get_table_columns(table)
        
        query = f"SELECT {', '.join(columns)} FROM `{table}` WHERE {where_clause}"
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        rows = []
        for row in cursor.fetchall():
            row_dict = {}
            for idx, col in enumerate(columns):
                row_dict[col] = row[idx]
            rows.append(row_dict)
        
        logger.info(f"Extracted {len(rows)} rows from {table} with filter")
        return rows
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Get statistics about the database.
        
        Returns:
            Dictionary of {table_name: row_count}
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        tables = self.discover_tables()
        stats = {}
        
        for table in tables:
            try:
                stats[table] = self.get_table_count(table)
            except Exception as e:
                logger.warning(f"Could not get count for {table}: {e}")
                stats[table] = 0
        
        return stats

