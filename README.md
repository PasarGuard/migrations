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

## Migration Workflow

### Step 1: Clone and Prepare

```bash
# Clone (or fork) the repository
git clone https://github.com/PasarGuard/migrations.git
cd migrations
```

### Step 2: Run the Migration

Migrate your panel data to generate the URL mapping file.

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

**Important**: The migration tool generates `subscription_url_mapping.json` - save this file path, you'll need it in the next step.

### Step 3: Install Redirect Server

After migration completes, install the redirect server to handle old URL redirects.

```bash
# Run the automated installer with interactive setup
curl -fsSL https://raw.githubusercontent.com/PasarGuard/migrations/main/redirect-server/install_redirect_server.sh | sudo bash
```

The installer will:
1. Download and install the redirect-server binary
2. **Prompt you for your URL mapping file** (from Step 2)
3. Guide you through server configuration (port, SSL, redirect domain)
4. Set up and start the service automatically

#### Post-Install Verification

```bash
# Check service status
sudo systemctl status redirect-server

# View logs
sudo journalctl -u redirect-server -f

# Test a redirect (replace with your old URL)
curl -I http://localhost:8080/sub/old_username/old_key
```

For detailed configuration options, SSL setup, and troubleshooting, see [redirect-server/README.md](redirect-server/README.md).
