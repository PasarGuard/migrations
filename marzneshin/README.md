# Marzneshin to Pasarguard Migration

Database migration tool for migrating data from Marzneshin to Pasarguard.

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

## Quick Start

```bash
# Install dependencies using uv (recommended) or pip
uv sync
# Or: pip install pymysql python-dotenv xxhash

# Configure and run
cp .env.example .env
# Edit .env with your database credentials
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

**Common Options:**
- `--log-level {DEBUG,INFO,WARNING,ERROR}` - Set logging level (default: INFO)
- `--log-file PATH` - Write logs to a file
- `--exclude-tables TABLE1,TABLE2` - Exclude tables from migration
- `--max-usage-rows N` - Limit usage table rows (default: 100000)
- `--generate-url-mapping` - Generate subscription URL mapping

