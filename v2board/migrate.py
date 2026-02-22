#!/usr/bin/env python3
"""
Simple wrapper script to run the v2board migration from any directory.
"""

import sys
from pathlib import Path

v2board_dir = Path(__file__).parent
sys.path.insert(0, str(v2board_dir))

from migration.main import main

if __name__ == "__main__":
    main()
