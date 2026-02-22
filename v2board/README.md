# v2board to Pasarguard Migration

Database migration tool for migrating data from a v2board SQL dump to a Pasarguard SQLite database.

## What This Migrates

- `v2_user` -> `admins`, `users`
- `v2_server_group` -> `groups`
- `v2_server_vmess`, `v2_server_trojan`, `v2_server_shadowsocks` -> `inbounds` + single `core_configs`
- Group associations:
  - `users_groups_association`
  - `inbounds_groups_association`

## Prerequisites

- Python 3.8+
- uv (recommended) or plain pip/python

## Quick Start

```bash
cd v2board
uv sync
uv run migrate.py --input-sql ../sql_multiserver_.sql
```

Output database will be created at:

- `v2board/output-db/db.sqlite3`

## Usage

```bash
uv run migrate.py [OPTIONS]
```

Options:

- `--input-sql PATH` Path to v2board SQL dump file
- `--schema-db PATH` Path to Pasarguard schema reference DB (default: `v2board/input-db-pg/db.sqlite3`)
- `--output-folder PATH` Output folder (creates `db.sqlite3` inside)
- `--log-level {DEBUG,INFO,WARNING,ERROR}`
- `--log-file PATH`

Example:

```bash
uv run migrate.py \
  --input-sql /path/to/sql_multiserver_.sql \
  --schema-db /path/to/pasarguard-schema.db \
  --output-folder /path/to/output-db \
  --log-level INFO
```

## Notes

- This migration expects a **v2board MySQL dump file** (with `INSERT INTO` rows), not just schema-only SQL.
- Existing rows in migration target tables are cleared before loading migrated rows.
- The tool copies seed/system tables (`settings`, `system`, `jwt`, `alembic_version`) from the schema reference DB when output DB is empty.
