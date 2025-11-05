#!/bin/bash
# Marzneshin to Pasarguard Migration Script
# Usage: ./migrate.sh [options]

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change to script directory
cd "$SCRIPT_DIR" || exit 1

# Load .env file if it exists
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Run the migration script
python3 migrate.py "$@"

