#!/usr/bin/env python3
"""
Simple wrapper script to run the migration from any directory.
Usage: 
    python migrate.py [options]                    # Run migration
"""

import sys
from pathlib import Path

# Add the x-ui directory to Python path
xui_dir = Path(__file__).parent
sys.path.insert(0, str(xui_dir))

# Import and run the migration
from migration.main import main

if __name__ == '__main__':
    main()

