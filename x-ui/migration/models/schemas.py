"""
Pasarguard SQLite database schema definitions and helpers.
"""

from typing import Dict, Any, Optional
import sqlite3


def get_pasarguard_schema(conn) -> Dict[str, Dict[str, Any]]:
    """
    Get Pasarguard database schema information.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        Dictionary of {table_name: {column_name: column_info}}
    """
    schema = {}
    
    cursor = conn.cursor()
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        schema[table] = get_column_info(conn, table)
    
    return schema


def get_column_info(conn, table: str) -> Dict[str, Any]:
    """
    Get column information for a table.
    
    Args:
        conn: SQLite database connection
        table: Table name
        
    Returns:
        Dictionary of {column_name: column_info}
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info(`{table}`)")
    
    result = {}
    for row in cursor.fetchall():
        col_name = row[1]
        col_type = row[2].lower() if row[2] else 'text'
        notnull = row[3] == 1
        default_value = row[4]
        pk = row[5] == 1
        
        # Determine if it's an auto-increment (INTEGER PRIMARY KEY)
        is_auto_increment = pk and ('int' in col_type or col_type == 'integer')
        
        # Parse SQLite type to standard type
        if 'int' in col_type:
            data_type = 'integer'
        elif 'real' in col_type or 'float' in col_type or 'double' in col_type:
            data_type = 'real'
        elif 'blob' in col_type:
            data_type = 'blob'
        else:
            data_type = 'text'
        
        result[col_name] = {
            "type": data_type,
            "column_type": col_type,
            "nullable": not notnull,
            "default": default_value,
            "max_length": None,  # SQLite doesn't enforce length limits
            "is_enum": False,  # SQLite doesn't have enums
            "enum_values": None,
            "is_auto_increment": is_auto_increment,
        }
    
    return result


def table_exists(conn, table: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        conn: SQLite database connection
        table: Table name
        
    Returns:
        True if table exists, False otherwise
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
    """, (table,))
    result = cursor.fetchone()
    return result is not None


def get_foreign_keys(conn, table: str) -> Dict[str, str]:
    """
    Get foreign key constraints for a table.
    
    Args:
        conn: SQLite database connection
        table: Table name
        
    Returns:
        Dictionary of {column_name: referenced_table}
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA foreign_key_list(`{table}`)")
    
    result = {}
    for row in cursor.fetchall():
        # SQLite foreign_key_list returns: (id, seq, table, from, to, on_update, on_delete, match)
        column_name = row[3]  # 'from' column
        referenced_table = row[2]  # referenced table
        result[column_name] = referenced_table
    
    return result


def get_primary_key(conn, table: str) -> Optional[str]:
    """
    Get primary key column for a table.
    
    Args:
        conn: SQLite database connection
        table: Table name
        
    Returns:
        Primary key column name or None
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info(`{table}`)")
    
    for row in cursor.fetchall():
        if row[5] == 1:  # pk column
            return row[1]  # column name
    
    return None


def get_unique_constraints(conn, table: str) -> list:
    """
    Get unique constraints for a table.
    
    Args:
        conn: SQLite database connection
        table: Table name
        
    Returns:
        List of unique column names
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA index_list(`{table}`)")
    
    unique_columns = []
    for row in cursor.fetchall():
        index_name = row[1]
        is_unique = row[2] == 1
        
        if is_unique:
            # Get columns for this index
            cursor2 = conn.cursor()
            cursor2.execute(f"PRAGMA index_info(`{index_name}`)")
            for idx_row in cursor2.fetchall():
                col_name = idx_row[2]
                if col_name not in unique_columns:
                    unique_columns.append(col_name)
    
    return unique_columns

