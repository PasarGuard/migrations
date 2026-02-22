"""
Data validators for migration output rows.
"""

from typing import Any, Dict, List


class DataValidator:
    """Validate converted rows against target schema metadata."""

    def validate_required_fields(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        target_columns: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        valid_rows: List[Dict[str, Any]] = []

        for row in rows:
            is_valid = True
            for col_name, col_info in target_columns.items():
                if col_info.get("is_auto_increment", False):
                    continue

                if not col_info.get("nullable", True) and col_info.get("default") is None:
                    if col_name not in row or row[col_name] is None:
                        is_valid = False
                        break

            if is_valid:
                valid_rows.append(row)

        return valid_rows
