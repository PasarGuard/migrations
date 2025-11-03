"""
Database extractor for Marzneshin MySQL database.
"""

import logging
import sys
import time
from typing import Dict, List, Any, Optional
import pymysql
from pymysql.cursors import DictCursor

from migration.config import DatabaseConfig, EXCLUDE_TABLES, MIGRATION_CONFIG

logger = logging.getLogger(__name__)


class MarzneshinExtractor:
    """Extract data from Marzneshin MySQL database."""
    
    def __init__(self, config: DatabaseConfig):
        """
        Initialize extractor.
        
        Args:
            config: Database configuration
        """
        self.config = config
        self.conn: Optional[pymysql.Connection] = None
    
    def connect(self):
        """Connect to Marzneshin database."""
        try:
            logger.info(f"Connecting to Marzneshin at {self.config.host}:{self.config.port}...")
            self.conn = pymysql.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                charset=self.config.charset,
                cursorclass=DictCursor,
                connect_timeout=10,  # 10 second timeout
                read_timeout=300,  # 5 minute read timeout (for large tables)
                write_timeout=30  # 30 second write timeout
            )
            logger.info(f"✓ Connected to Marzneshin database at {self.config.host}")
        except pymysql.err.OperationalError as e:
            logger.error(f"✗ Cannot connect to Marzneshin database:")
            logger.error(f"  Host: {self.config.host}:{self.config.port}")
            logger.error(f"  Database: {self.config.database}")
            logger.error(f"  User: {self.config.user}")
            logger.error(f"  Error: {e}")
            raise ConnectionError(f"Failed to connect to Marzneshin: {e}")
        except Exception as e:
            logger.error(f"✗ Unexpected error connecting to Marzneshin: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Disconnected from Marzneshin database")
    
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
        
        with self.conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [row[f"Tables_in_{self.config.database}"] for row in cursor.fetchall()]
            
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
        
        with self.conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE `{table}`")
            return [row['Field'] for row in cursor.fetchall()]
    
    def extract_table(self, table: str, limit: Optional[int] = None, batch_size: int = 5000, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Extract all data from a table.
        
        Args:
            table: Table name
            limit: Optional row limit
            batch_size: Number of rows to fetch per batch (for progress reporting)
            max_rows: Maximum rows to extract (auto-applied for very large tables)
            
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
            
            # Escape column names with backticks
            escaped_columns = [f"`{col}`" for col in columns]
            
            # Get total count first to decide if we need filtering
            with self.conn.cursor() as count_cursor:
                count_cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
                total_count = count_cursor.fetchone()['count']
            
            # Build query
            query = f"SELECT {', '.join(escaped_columns)} FROM `{table}`"
            
            # For very large usage tables, apply intelligent filtering
            is_usage_table = table in ['node_user_usages', 'node_usages', 'admin_usage_logs', 'user_usage_logs']
            if is_usage_table and total_count > 100000 and 'created_at' in columns:
                # Limit to recent data for very large usage tables
                if max_rows is None:
                    max_rows = MIGRATION_CONFIG.max_usage_table_rows if MIGRATION_CONFIG.max_usage_table_rows > 0 else None
                
                if max_rows and max_rows < total_count:
                    logger.warning(
                        f"  Table {table} has {total_count:,} rows. "
                        f"Limiting to {max_rows:,} most recent rows (based on created_at). "
                        f"Set MIGRATION_CONFIG.max_usage_table_rows=0 to extract all rows."
                    )
                    query += " ORDER BY `created_at` DESC"
                    query += f" LIMIT {max_rows}"
                    total_count = max_rows
            else:
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
            
            # Use batch fetching for large tables
            rows = []
            start_time = time.time()
            logger.info(f"  Fetching {total_count:,} rows from {table}...")
            sys.stdout.flush()  # Force flush
            
            with self.conn.cursor() as cursor:
                query_start = time.time()
                cursor.execute(query)
                query_time = time.time() - query_start
                logger.info(f"  Query executed in {query_time:.2f}s, fetching results...")
                sys.stdout.flush()
                
                # Fetch in batches to show progress
                fetched = 0
                last_logged = 0
                last_log_time = time.time()
                while True:
                    fetch_start = time.time()
                    batch = cursor.fetchmany(batch_size)
                    fetch_time = time.time() - fetch_start
                    
                    if not batch:
                        break
                    
                    rows.extend(batch)
                    fetched += len(batch)
                    
                    # Log progress every batch (force flush to see real-time progress)
                    current_time = time.time()
                    if total_count > batch_size and (fetched - last_logged >= batch_size or current_time - last_log_time >= 2.0):
                        progress_pct = (fetched / total_count) * 100
                        elapsed = current_time - start_time
                        logger.info(f"  Progress: {fetched:,}/{total_count:,} rows ({progress_pct:.1f}%) - {elapsed:.1f}s elapsed")
                        sys.stdout.flush()
                        last_logged = fetched
                        last_log_time = current_time
            
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
        
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
            result = cursor.fetchone()
            return result['count'] if result else 0
    
    def extract_with_filter(self, table: str, where_clause: str, params: tuple = ()) -> List[Dict[str, Any]]:
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
        escaped_columns = [f"`{col}`" for col in columns]
        
        query = f"SELECT {', '.join(escaped_columns)} FROM `{table}` WHERE {where_clause}"
        
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        
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

