"""
SQL dump extractor for v2board migrations.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..config import V2BoardConfig

logger = logging.getLogger(__name__)


class V2BoardExtractor:
    """Extract target table data from a v2board SQL dump file."""

    TARGET_TABLES = {
        "v2_user",
        "v2_plan",
        "v2_server_group",
        "v2_server_vmess",
        "v2_server_trojan",
        "v2_server_shadowsocks",
    }

    def __init__(self, config: V2BoardConfig):
        self.config = config
        self.dump_path = Path(config.sql_dump_path)

    def connect(self):
        """Validate dump file exists."""
        if not self.dump_path.exists():
            raise FileNotFoundError(f"v2board SQL dump not found: {self.dump_path}")
        logger.info("Using v2board SQL dump: %s", self.dump_path)

    def disconnect(self):
        """No-op for file-based extractor."""

    def extract_all_tables(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract rows for migration-relevant tables from SQL dump."""
        self.connect()

        start_time = time.time()
        extracted: Dict[str, List[Dict[str, Any]]] = {table: [] for table in self.TARGET_TABLES}

        statement_lines: List[str] = []
        statement_table: Optional[str] = None
        statement_count = 0

        with self.dump_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if statement_table is None:
                    if not line.startswith("INSERT INTO `"):
                        continue

                    table = self._extract_insert_table(line)
                    if table not in self.TARGET_TABLES:
                        continue

                    statement_table = table
                    statement_lines = [line]

                    if line.rstrip().endswith(";"):
                        rows = self._parse_insert_statement("".join(statement_lines), statement_table)
                        extracted[statement_table].extend(rows)
                        statement_table = None
                        statement_lines = []
                        statement_count += 1
                else:
                    statement_lines.append(line)
                    if line.rstrip().endswith(";"):
                        rows = self._parse_insert_statement("".join(statement_lines), statement_table)
                        extracted[statement_table].extend(rows)
                        statement_table = None
                        statement_lines = []
                        statement_count += 1

        elapsed = time.time() - start_time
        logger.info("Parsed %d INSERT statement(s) in %.2fs", statement_count, elapsed)

        for table in sorted(extracted.keys()):
            logger.info("  %s: %d row(s)", table, len(extracted[table]))

        return extracted

    def _extract_insert_table(self, line: str) -> str:
        match = re.match(r"INSERT INTO `([^`]+)`", line)
        if not match:
            return ""
        return match.group(1)

    def _parse_insert_statement(self, statement: str, expected_table: str) -> List[Dict[str, Any]]:
        match = re.match(
            r"INSERT INTO `(?P<table>[^`]+)`\s*\((?P<columns>.*?)\)\s*VALUES\s*(?P<values>.*);\s*$",
            statement,
            flags=re.DOTALL,
        )
        if not match:
            logger.warning("Skipping unparsable INSERT statement for table %s", expected_table)
            return []

        table = match.group("table")
        if table != expected_table:
            logger.warning("Statement table mismatch: expected %s, got %s", expected_table, table)
            return []

        columns = re.findall(r"`([^`]+)`", match.group("columns"))
        if not columns:
            logger.warning("No columns found in INSERT statement for table %s", table)
            return []

        values_blob = match.group("values")
        tuples = self._split_tuples(values_blob)

        rows: List[Dict[str, Any]] = []
        for tup in tuples:
            values = self._split_values(tup)
            if len(values) != len(columns):
                logger.debug(
                    "Skipping row in %s due to column/value mismatch (%d != %d)",
                    table,
                    len(values),
                    len(columns),
                )
                continue

            parsed_values = [self._parse_scalar(token) for token in values]
            rows.append(dict(zip(columns, parsed_values)))

        return rows

    def _split_tuples(self, values_blob: str) -> List[str]:
        tuples: List[str] = []
        depth = 0
        in_string = False
        escaped = False
        start_idx = -1

        for idx, char in enumerate(values_blob):
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == "'":
                    in_string = False
                continue

            if char == "'":
                in_string = True
                continue

            if char == "(":
                if depth == 0:
                    start_idx = idx + 1
                depth += 1
                continue

            if char == ")":
                depth -= 1
                if depth == 0 and start_idx >= 0:
                    tuples.append(values_blob[start_idx:idx])
                    start_idx = -1

        return tuples

    def _split_values(self, row_blob: str) -> List[str]:
        values: List[str] = []
        in_string = False
        escaped = False
        current: List[str] = []

        for char in row_blob:
            if in_string:
                current.append(char)
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == "'":
                    in_string = False
                continue

            if char == "'":
                in_string = True
                current.append(char)
                continue

            if char == ",":
                values.append("".join(current).strip())
                current = []
                continue

            current.append(char)

        if current:
            values.append("".join(current).strip())

        return values

    def _parse_scalar(self, token: str) -> Any:
        if token.upper() == "NULL":
            return None

        if token.startswith("'") and token.endswith("'"):
            return self._unescape_mysql_string(token[1:-1])

        # int / float fallback
        try:
            return int(token)
        except (TypeError, ValueError):
            pass

        try:
            return float(token)
        except (TypeError, ValueError):
            pass

        return token

    def _unescape_mysql_string(self, value: str) -> str:
        mapping = {
            "0": "\0",
            "b": "\b",
            "n": "\n",
            "r": "\r",
            "t": "\t",
            "Z": "\x1a",
            "\\": "\\",
            "'": "'",
            '"': '"',
        }

        out: List[str] = []
        idx = 0
        while idx < len(value):
            char = value[idx]
            if char == "\\" and idx + 1 < len(value):
                next_char = value[idx + 1]
                out.append(mapping.get(next_char, next_char))
                idx += 2
                continue
            out.append(char)
            idx += 1

        return "".join(out)
