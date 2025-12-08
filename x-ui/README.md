# X-UI to Pasarguard Migration

Database migration tool for migrating data from x-ui SQLite database to Pasarguard SQLite database.

## Prerequisites

- **Python 3.8+**
- **uv** - Fast Python package installer and resolver

### Install uv

```bash
# Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or visit: https://github.com/astral-sh/uv
```

```bash
# Linux/macOS - Add to PATH
source $HOME/.cargo/env
# Or: export PATH="$HOME/.cargo/bin:$PATH"
```

## Setup

Before running the migration, place your x-ui database file (`x-ui.db`) inside the `x-ui` directory:

```bash
# Copy your x-ui database to the x-ui directory
cp /path/to/your/x-ui.db x-ui/
# Or on Windows:
# copy C:\path\to\your\x-ui.db x-ui\
```

## Quick Start

```bash
# Install dependencies using uv (recommended) or pip
uv sync
# Or: pip install python-dotenv

# Run migration with default paths (expects x-ui.db in x-ui directory)
uv run migrate.py
```

## Usage

```bash
uv run migrate.py [OPTIONS]
# Or activate the virtual environment and use python directly
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate    # Windows
python migrate.py [OPTIONS]
```

**Options:**
- `--input-db PATH` - Path to input x-ui database (default: `x-ui.db` in x-ui directory)
- `--schema-db PATH` - Path to Pasarguard schema reference database (read-only, default: `input-db-pg/db.sqlite3`)
- `--output-folder PATH` - Path to output folder for Pasarguard database (default: `output-db`, creates `output-db/db.sqlite3`)
- `--log-level {DEBUG,INFO,WARNING,ERROR}` - Set logging level (default: INFO)
- `--log-file PATH` - Write logs to a file

**Example:**
```bash
uv run migrate.py \
  --input-db /path/to/x-ui.db \
  --schema-db /path/to/pasarguard-schema.db \
  --output-folder /path/to/output \
  --log-level DEBUG
```

## Important Notes

- **This migration requires a Pasarguard schema reference database** - The `--schema-db` parameter should point to an existing Pasarguard database with the correct schema (tables must exist)
- **The output database is created in the output folder** - Default location is `output-db/db.sqlite3`
- Make sure you have a backup of both databases before running the migration

## Table Mappings

- `x-ui.inbounds` → `pasarguard.inbounds` + `pasarguard.core_configs`
- `x-ui.client_traffics` → `pasarguard.users` (client users)

**Note:** Admin users (`x-ui.users`) are **not migrated** to Pasarguard. You should create admin accounts manually in Pasarguard after migration.

## Migration Process

1. Extracts data from x-ui SQLite database
2. Validates foreign key references
3. Converts data format (x-ui → Pasarguard)
4. Inserts data into Pasarguard database (created in output folder)
5. Creates a group for each inbound (users are limited to inbounds in x-ui)
6. Associates users with their inbound groups

The migration converter automatically removes problematic TLS settings (security, tlsSettings, externalProxy, proxySettings, sockopt) during migration.

## Generating Subscription URL Mapping

After migration, you can generate a subscription URL mapping file for the redirect server:

```bash
# Generate subscription URL mapping
uv run migration/generate_subscription_url_mapping.py --output subscription_url_mapping.json

# Or with custom paths
uv run migration/generate_subscription_url_mapping.py \
  --xui-db /path/to/x-ui.db \
  --pasarguard-db /path/to/output-db/db.sqlite3 \
  --output subscription_url_mapping.json
```

**Options:**
- `--output` - Output JSON file path (default: `subscription_url_mapping.json`)
- `--xui-path` - x-ui subscription path (default: `sub`)
- `--pasarguard-path` - Pasarguard subscription path (default: `sub`)
- `--xui-db` - Path to x-ui SQLite database (default: `x-ui.db` in x-ui directory)
- `--pasarguard-db` - Path to Pasarguard SQLite database (default: `output-db/db.sqlite3` - the database created by the migration)
- `--log-level` - Logging level (DEBUG, INFO, WARNING, ERROR)

**Note:** The Pasarguard database is only saved to `output-db/db.sqlite3` (or the folder specified by `--output-folder` during migration). There is no other default location.

The generated mapping file includes:
- Old x-ui subscription URLs (extracted from inbound settings if available)
- New Pasarguard subscription URLs with tokens
- User mappings with inbound IDs
- Panel identifier (`"x-ui"`)

This mapping file can be used with the redirect server to handle subscription URL redirects.
