"""
Data validators for migration.
"""

import logging
from typing import Dict, List, Any, Set
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate data integrity during migration."""
    
    def __init__(self):
        """Initialize validator."""
        self.valid_user_ids: Set[int] = set()
        self.valid_node_ids: Set[int] = set()
        self.valid_group_ids: Set[int] = set()
        self.valid_inbound_ids: Set[int] = set()
        self.valid_inbound_tags: Set[str] = set()
    
    def build_reference_sets(self, all_data: Dict[str, List[Dict[str, Any]]]):
        """
        Build sets of valid IDs for foreign key validation.
        
        Args:
            all_data: All extracted data
        """
        # Build user IDs
        if 'users' in all_data:
            self.valid_user_ids = {row['id'] for row in all_data['users'] if 'id' in row}
            logger.info(f"Found {len(self.valid_user_ids)} valid user IDs")
        
        # Build node IDs
        if 'nodes' in all_data:
            self.valid_node_ids = {row['id'] for row in all_data['nodes'] if 'id' in row}
            logger.info(f"Found {len(self.valid_node_ids)} valid node IDs")
        
        # Build group IDs (from services)
        if 'services' in all_data:
            self.valid_group_ids = {row['id'] for row in all_data['services'] if 'id' in row}
            logger.info(f"Found {len(self.valid_group_ids)} valid group IDs (services)")
        
        # Build inbound IDs and tags
        if 'inbounds' in all_data:
            self.valid_inbound_ids = {row['id'] for row in all_data['inbounds'] if 'id' in row}
            self.valid_inbound_tags = {row['tag'] for row in all_data['inbounds'] if 'tag' in row}
            logger.info(f"Found {len(self.valid_inbound_ids)} valid inbound IDs")
    
    def update_inbound_ids_from_database(self, conn):
        """
        Update valid_inbound_ids from the target database.
        This is needed when INSERT IGNORE is used and some inbounds might be skipped.
        
        Args:
            conn: Database connection
        """
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT id FROM inbounds")
                results = cursor.fetchall()
                self.valid_inbound_ids = {row['id'] for row in results}
                logger.info(f"Updated valid inbound IDs from database: {len(self.valid_inbound_ids)} IDs")
        except Exception as e:
            logger.warning(f"Failed to update inbound IDs from database: {e}")
    
    def validate_foreign_keys(
        self,
        table: str,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Filter out rows with invalid foreign key references.
        
        Args:
            table: Table name
            rows: Rows to validate
            
        Returns:
            Filtered rows with valid foreign keys
        """
        if not rows:
            return rows
        
        original_count = len(rows)
        
        if table == "users_groups_association":
            rows = self._validate_user_group_association(rows)
        
        elif table == "inbounds_groups_association":
            rows = self._validate_inbound_group_association(rows)
        
        elif table == "node_user_usages":
            rows = self._validate_node_user_usages(rows)
        
        elif table == "node_usages":
            rows = self._validate_node_usages(rows)
        
        elif table == "hosts":
            rows = self._validate_hosts(rows)
        
        elif table == "user_usage_logs":
            rows = self._validate_user_logs(rows)
        
        elif table == "admin_usage_logs":
            rows = self._validate_admin_logs(rows)
        
        elif table == "notification_reminders":
            rows = self._validate_notification_reminders(rows)
        
        elif table == "user_subscription_updates":
            rows = self._validate_user_subscription_updates(rows)
        
        filtered_count = original_count - len(rows)
        if filtered_count > 0:
            logger.info(f"Filtered {filtered_count} rows from {table} due to invalid foreign keys")
        
        return rows
    
    def _validate_user_group_association(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate users_groups_association foreign keys."""
        return [
            row for row in rows
            if row.get('user_id') in self.valid_user_ids
            and (row.get('groups_id') in self.valid_group_ids or row.get('service_id') in self.valid_group_ids)
        ]
    
    def _validate_inbound_group_association(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate inbounds_groups_association foreign keys."""
        return [
            row for row in rows
            if row.get('inbound_id') in self.valid_inbound_ids
            and (row.get('group_id') in self.valid_group_ids or row.get('service_id') in self.valid_group_ids)
        ]
    
    def _validate_node_user_usages(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate node_user_usages foreign keys."""
        return [
            row for row in rows
            if row.get('user_id') in self.valid_user_ids
            and (row.get('node_id') is None or row.get('node_id') in self.valid_node_ids)
        ]
    
    def _validate_node_usages(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate node_usages foreign keys."""
        return [
            row for row in rows
            if row.get('node_id') is None or row.get('node_id') in self.valid_node_ids
        ]
    
    def _validate_hosts(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate hosts foreign keys."""
        return [
            row for row in rows
            if row.get('inbound_tag') is None 
            or row.get('inbound_tag') in self.valid_inbound_tags
        ]
    
    def _validate_user_logs(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate user_usage_logs foreign keys."""
        return [
            row for row in rows
            if row.get('user_id') in self.valid_user_ids
        ]
    
    def _validate_admin_logs(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate admin_usage_logs foreign keys (no admin ID validation for now)."""
        return rows
    
    def _validate_notification_reminders(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate notification_reminders foreign keys."""
        return [
            row for row in rows
            if row.get('user_id') in self.valid_user_ids
        ]
    
    def _validate_user_subscription_updates(
        self,
        rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate user_subscription_updates foreign keys."""
        return [
            row for row in rows
            if row.get('user_id') in self.valid_user_ids
        ]
    
    def validate_required_fields(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        target_columns: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Validate that required fields are present.
        
        Args:
            table: Table name
            rows: Rows to validate
            target_columns: Target column definitions
            
        Returns:
            Valid rows
        """
        if not rows or not target_columns:
            return rows
        
        # Get required columns (NOT NULL without default, but skip AUTO_INCREMENT id columns)
        required_cols = []
        for col, info in target_columns.items():
            # Skip id columns that are AUTO_INCREMENT (they're auto-generated)
            if col == 'id' and info.get('is_auto_increment', False):
                continue
            # Skip columns that are nullable or have defaults
            if not info['nullable'] and info['default'] is None:
                required_cols.append(col)
        
        if not required_cols:
            return rows
        
        valid_rows = []
        for row in rows:
            is_valid = True
            for col in required_cols:
                # Check if field is missing, None, or empty string
                if col not in row or row[col] is None or (isinstance(row[col], str) and not row[col].strip()):
                    logger.warning(f"Row missing required field {table}.{col} (value: {row.get(col)})")
                    # Special handling for core_configs.name - try to generate a fallback
                    if table == "core_configs" and col == "name":
                        fallback_name = f"core_config_{len(valid_rows) + 1}"
                        row[col] = fallback_name
                        logger.warning(f"Generated fallback name '{fallback_name}' for core_configs row")
                        continue  # Don't mark as invalid, use the fallback
                    is_valid = False
                    break
            
            if is_valid:
                valid_rows.append(row)
        
        filtered_count = len(rows) - len(valid_rows)
        if filtered_count > 0:
            logger.info(f"Filtered {filtered_count} rows from {table} due to missing required fields")
        
        return valid_rows
    
    def check_unique_constraints(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        unique_columns: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Check for duplicate values in unique columns.
        
        Args:
            table: Table name
            rows: Rows to check
            unique_columns: List of unique column names
            
        Returns:
            Rows without duplicates (keeps first occurrence)
        """
        if not rows or not unique_columns:
            return rows
        
        seen_values = {col: set() for col in unique_columns}
        unique_rows = []
        duplicates = 0
        
        for row in rows:
            is_unique = True
            
            for col in unique_columns:
                if col in row and row[col] is not None:
                    value = row[col]
                    if value in seen_values[col]:
                        logger.warning(f"Duplicate value in {table}.{col}: {value}")
                        is_unique = False
                        duplicates += 1
                        break
                    seen_values[col].add(value)
            
            if is_unique:
                unique_rows.append(row)
        
        if duplicates > 0:
            logger.info(f"Filtered {duplicates} duplicate rows from {table}")
        
        return unique_rows

