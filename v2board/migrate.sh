#!/bin/bash
# v2board to Pasarguard migration script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

python3 migrate.py "$@"
