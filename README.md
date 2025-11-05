# Panel Migration Tools

Database migration tools for migrating data from various panel systems to Pasarguard.

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

## Supported Panels

- **Marzneshin** - See [marzneshin/README.md](marzneshin/README.md)

## Quick Start

```bash
cd marzneshin

# Install dependencies using uv (recommended) or pip
uv sync
# Or: pip install pymysql python-dotenv xxhash

# Configure and run
cp .env.example .env
# Edit .env with your database credentials
uv run migrate.py
```
