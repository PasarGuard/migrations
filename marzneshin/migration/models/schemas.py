"""
Pasarguard database schema definitions and helpers.
"""

from typing import Dict, Any, Optional
import pymysql


def get_pasarguard_schema(conn) -> Dict[str, Dict[str, Any]]:
    """
    Get Pasarguard database schema information.
    
    Args:
        conn: Database connection
        
    Returns:
        Dictionary of {table_name: {column_name: column_info}}
    """
    schema = {}
    
    with conn.cursor() as cursor:
        # Get all tables
        cursor.execute("SHOW TABLES")
        results = cursor.fetchall()
        
        # Handle both dict and tuple results
        if results:
            if isinstance(results[0], dict):
                # DictCursor - get first value from dict
                tables = [list(row.values())[0] for row in results]
            else:
                # Regular cursor - get first element
                tables = [row[0] for row in results]
        else:
            tables = []
        
        for table in tables:
            schema[table] = get_column_info(conn, table)
    
    return schema


def get_column_info(conn, table: str) -> Dict[str, Any]:
    """
    Get column information for a table.
    
    Args:
        conn: Database connection
        table: Table name
        
    Returns:
        Dictionary of {column_name: column_info}
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                IS_NULLABLE, 
                COLUMN_DEFAULT, 
                CHARACTER_MAXIMUM_LENGTH,
                COLUMN_TYPE,
                EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = %s 
            AND TABLE_SCHEMA = DATABASE()
            ORDER BY ORDINAL_POSITION
        """, (table,))
        
        result = {}
        for row in cursor.fetchall():
            data_type = row['DATA_TYPE'].lower()
            column_type = row.get('COLUMN_TYPE', '').lower()
            extra = row.get('EXTRA', '').lower()
            is_enum = data_type == 'enum' or 'enum' in column_type
            is_auto_increment = 'auto_increment' in extra
            
            # Parse enum values if it's an enum
            enum_values = None
            if is_enum and 'enum(' in column_type:
                # Extract enum values: enum('value1','value2')
                enum_str = column_type[column_type.find('(') + 1:column_type.rfind(')')]
                enum_values = [v.strip("'") for v in enum_str.split(',')]
            
            result[row['COLUMN_NAME']] = {
                "type": data_type,
                "column_type": column_type,
                "nullable": row['IS_NULLABLE'] == "YES",
                "default": row['COLUMN_DEFAULT'],
                "max_length": row['CHARACTER_MAXIMUM_LENGTH'],
                "is_enum": is_enum,
                "enum_values": enum_values,
                "is_auto_increment": is_auto_increment,
            }
        
        return result


def table_exists(conn, table: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        conn: Database connection
        table: Table name
        
    Returns:
        True if table exists, False otherwise
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = %s
        """, (table,))
        result = cursor.fetchone()
        return result['count'] > 0 if result else False


def get_foreign_keys(conn, table: str) -> Dict[str, str]:
    """
    Get foreign key constraints for a table.
    
    Args:
        conn: Database connection
        table: Table name
        
    Returns:
        Dictionary of {column_name: referenced_table}
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                REFERENCED_TABLE_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = %s
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """, (table,))
        
        return {row['COLUMN_NAME']: row['REFERENCED_TABLE_NAME'] 
                for row in cursor.fetchall()}


def get_primary_key(conn, table: str) -> Optional[str]:
    """
    Get primary key column for a table.
    
    Args:
        conn: Database connection
        table: Table name
        
    Returns:
        Primary key column name or None
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = %s
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
            LIMIT 1
        """, (table,))
        
        result = cursor.fetchone()
        return result['COLUMN_NAME'] if result else None


def get_unique_constraints(conn, table: str) -> list:
    """
    Get unique constraints for a table.
    
    Args:
        conn: Database connection
        table: Table name
        
    Returns:
        List of unique column names
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = %s
            AND CONSTRAINT_NAME != 'PRIMARY'
        """, (table,))
        
        return [row['COLUMN_NAME'] for row in cursor.fetchall()]


