"""
Data validators for migration.
"""

import logging
from typing import Dict, List, Any, Set

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate data integrity during migration."""
    
    def __init__(self):
        """Initialize validator."""
        self.valid_user_ids: Set[int] = set()
        self.valid_inbound_ids: Set[int] = set()
        self.valid_admin_ids: Set[int] = set()
    
    def build_reference_sets(self, all_data: Dict[str, List[Dict[str, Any]]]):
        """
        Build sets of valid IDs for foreign key validation.
        
        Args:
            all_data: All extracted data
        """
        # Build admin IDs (from x-ui.users)
        if 'users' in all_data:
            self.valid_admin_ids = {row['id'] for row in all_data['users'] if 'id' in row}
            logger.info(f"Found {len(self.valid_admin_ids)} valid admin IDs")
        
        # Build inbound IDs
        if 'inbounds' in all_data:
            self.valid_inbound_ids = {row['id'] for row in all_data['inbounds'] if 'id' in row}
            logger.info(f"Found {len(self.valid_inbound_ids)} valid inbound IDs")
        
        # Build user IDs (from client_traffics after conversion)
        if 'client_traffics' in all_data:
            self.valid_user_ids = {row['id'] for row in all_data['client_traffics'] if 'id' in row}
            logger.info(f"Found {len(self.valid_user_ids)} valid user IDs")
    
    def validate_foreign_keys(
        self,
        table: str,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Filter out rows with invalid foreign key references.
        
        Args:
            table: Table name
            rows: List of rows to validate
            
        Returns:
            Filtered list of valid rows
        """
        valid_rows = []
        
        for row in rows:
            is_valid = True
            
            # Validate admin_id references
            if 'admin_id' in row and row['admin_id'] is not None:
                if row['admin_id'] not in self.valid_admin_ids:
                    logger.debug(f"Skipping row with invalid admin_id: {row.get('admin_id')}")
                    is_valid = False
            
            # Validate inbound_id references
            if 'inbound_id' in row and row['inbound_id'] is not None:
                if row['inbound_id'] not in self.valid_inbound_ids:
                    logger.debug(f"Skipping row with invalid inbound_id: {row.get('inbound_id')}")
                    is_valid = False
            
            if is_valid:
                valid_rows.append(row)
        
        return valid_rows
    
    def validate_required_fields(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        target_columns: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Validate required fields are present.
        
        Args:
            table: Table name
            rows: List of rows to validate
            target_columns: Target column information
            
        Returns:
            Filtered list of valid rows
        """
        valid_rows = []
        
        for row in rows:
            is_valid = True
            
            # Check required fields (non-nullable columns without defaults, excluding auto-increment)
            for col_name, col_info in target_columns.items():
                # Skip auto-increment fields (like id) - they're generated automatically
                if col_info.get('is_auto_increment', False):
                    continue
                if not col_info.get('nullable', True) and col_info.get('default') is None:
                    if col_name not in row or row[col_name] is None:
                        logger.debug(f"Skipping row missing required field {col_name} in table {table}")
                        is_valid = False
                        break
            
            if is_valid:
                valid_rows.append(row)
        
        return valid_rows

