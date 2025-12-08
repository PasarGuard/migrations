"""
Inspect Pasarguard SQLite database structure.
"""

import sqlite3
import json
import sys
from pathlib import Path
from typing import Dict, List, Any

def convert_wsl_path(wsl_path: str) -> str:
    """Convert WSL Windows path to Linux path."""
    # Convert \\wsl.localhost\Ubuntu\home\katana\panel\db.sqlite3
    # to /home/katana/panel/db.sqlite3
    if 'wsl.localhost' in wsl_path or wsl_path.startswith('\\\\'):
        # Remove \\wsl.localhost\Ubuntu or \wsl.localhost\Ubuntu
        path = wsl_path.replace('\\\\wsl.localhost\\Ubuntu', '').replace('\\wsl.localhost\\Ubuntu', '')
        path = path.replace('\\', '/')
        # Ensure it starts with /
        if not path.startswith('/'):
            path = '/' + path
        return path
    return wsl_path

def inspect_database(db_path: str) -> Dict[str, Any]:
    """Inspect Pasarguard database structure."""
    # Convert WSL path if needed
    if 'wsl.localhost' in db_path or db_path.startswith('\\\\'):
        db_path = convert_wsl_path(db_path)
    
    # Try to connect
    conn = None
    original_path = db_path
    
    # Convert WSL path if needed
    if 'wsl.localhost' in db_path:
        db_path = convert_wsl_path(db_path)
        print(f"Converted WSL path to: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        print(f"✓ Connected successfully")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print(f"\nNote: WSL paths may not be accessible from Windows.")
        print(f"Options:")
        print(f"  1. Copy db.sqlite3 to Windows and use Windows path")
        print(f"  2. Run this script from WSL using Linux path: /home/katana/panel/db.sqlite3")
        print(f"  3. Use network share or mount point")
        raise Exception(f"Could not connect to database at: {original_path}")
    
    conn.row_factory = sqlite3.Row
    
    info = {
        'tables': {},
        'table_list': []
    }
    
    # Get all tables
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    info['table_list'] = tables
    
    # Get schema for each table
    for table in tables:
        if table == 'sqlite_sequence':
            continue
            
        # Get table schema
        cursor.execute(f"PRAGMA table_info(`{table}`)")
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'cid': row[0],
                'name': row[1],
                'type': row[2],
                'notnull': row[3],
                'dflt_value': row[4],
                'pk': row[5]
            })
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
        row_count = cursor.fetchone()[0]
        
        # Get sample data (first row)
        cursor.execute(f"SELECT * FROM `{table}` LIMIT 1")
        sample_row = cursor.fetchone()
        sample_data = {}
        if sample_row:
            for idx, col in enumerate(columns):
                value = sample_row[idx]
                # Truncate long values
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                sample_data[col['name']] = value
        
        # Get foreign keys
        cursor.execute(f"PRAGMA foreign_key_list(`{table}`)")
        foreign_keys = []
        for row in cursor.fetchall():
            foreign_keys.append({
                'id': row[0],
                'seq': row[1],
                'table': row[2],
                'from': row[3],
                'to': row[4],
                'on_update': row[5],
                'on_delete': row[6],
                'match': row[7]
            })
        
        info['tables'][table] = {
            'columns': columns,
            'row_count': row_count,
            'sample_data': sample_data,
            'foreign_keys': foreign_keys
        }
    
    conn.close()
    return info

if __name__ == '__main__':
    # Default path or use command line argument
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        # Default Pasarguard path
        db_path = r"\\wsl.localhost\Ubuntu\home\katana\panel\db.sqlite3"
    
    print("=" * 70)
    print("PASARGUARD DATABASE STRUCTURE")
    print("=" * 70)
    print(f"\nDatabase Path: {db_path}")
    
    try:
        info = inspect_database(db_path)
        
        print(f"\nTotal Tables: {len(info['table_list'])}")
        print(f"Tables: {', '.join(info['table_list'])}")
        
        print("\n" + "=" * 70)
        print("TABLE DETAILS")
        print("=" * 70)
        
        for table_name, table_info in info['tables'].items():
            print(f"\n{table_name.upper()}")
            print("-" * 70)
            print(f"Row Count: {table_info['row_count']}")
            print(f"Columns ({len(table_info['columns'])}):")
            for col in table_info['columns']:
                pk_str = " [PRIMARY KEY]" if col['pk'] else ""
                notnull_str = " [NOT NULL]" if col['notnull'] else ""
                default_str = f" [DEFAULT: {col['dflt_value']}]" if col['dflt_value'] else ""
                print(f"  - {col['name']}: {col['type']}{pk_str}{notnull_str}{default_str}")
            
            if table_info['foreign_keys']:
                print(f"\nForeign Keys:")
                for fk in table_info['foreign_keys']:
                    print(f"  - {fk['from']} -> {fk['table']}.{fk['to']}")
            
            if table_info['sample_data']:
                print(f"\nSample Data:")
                for key, value in table_info['sample_data'].items():
                    value_str = str(value)
                    if len(value_str) > 100:
                        value_str = value_str[:100] + "..."
                    print(f"  {key}: {value_str}")
        
        # Save to JSON
        output_file = 'pasarguard_schema.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(info, f, indent=2, default=str)
        print(f"\n\nSchema saved to: {output_file}")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        print("\nPlease provide the correct database path:")
        print("  python inspect_pasarguard_db.py /path/to/pasarguard.db")
        print("\nOr set PASARGUARD_DB_PATH environment variable")
        sys.exit(1)

