#!/usr/bin/env python3
"""
Simple wrapper script to run the migration from any directory.
Usage: 
    python migrate.py [options]                    # Run migration
    python migrate.py --generate-subscription-mapping [output_file]  # Generate subscription URL mapping
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import and run the migration
from migration.main import main

if __name__ == '__main__':
    main()

