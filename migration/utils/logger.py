"""
Logging configuration with colorization.
"""

import logging
import sys
import os
from pathlib import Path
from typing import Optional

# Enable ANSI colors on Windows 10+
if sys.platform == 'win32':
    try:
        # Enable ANSI escape sequences on Windows
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except:
        pass  # If it fails, colors might not work but that's okay


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and no timestamps."""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m',       # Reset
    }
    
    # Symbols for log levels
    SYMBOLS = {
        'DEBUG': '🔍',
        'INFO': '✓',
        'WARNING': '⚠',
        'ERROR': '✗',
        'CRITICAL': '🚨',
    }
    
    def format(self, record):
        # Get color for log level
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        symbol = self.SYMBOLS.get(record.levelname, '')
        
        # Format message with color and symbol
        levelname = f"{color}{symbol} {record.levelname}{reset}"
        
        # Format: [LEVEL] message (no timestamp, no logger name)
        message = f"{levelname:25} {record.getMessage()}"
        
        return message


def setup_logging(
    level: str = 'INFO',
    log_file: Optional[str] = None,
    format_string: Optional[str] = None,
    use_colors: bool = True
):
    """
    Setup logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        format_string: Optional custom format string (for file logging)
        use_colors: Whether to use colored output for console
    """
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create handlers
    handlers = []
    
    # Console handler with colors (no timestamps)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    if use_colors and sys.stdout.isatty():
        # Use colored formatter for console
        console_formatter = ColoredFormatter()
    else:
        # Plain formatter without timestamps
        console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)
    
    # File handler if specified (with timestamps)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(numeric_level)
        if format_string is None:
            format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        file_formatter = logging.Formatter(format_string)
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        handlers=handlers,
        format='%(message)s'  # Default format (will be overridden by handlers)
    )
    
    # Set level for migration package
    migration_logger = logging.getLogger('migration')
    migration_logger.setLevel(numeric_level)
    
    # Reduce noise from other libraries
    logging.getLogger('pymysql').setLevel(logging.WARNING)
    
    # Don't log initialization message to avoid noise
    # logging.info(f"Logging initialized at {level} level")
    # if log_file:
    #     logging.info(f"Logging to file: {log_file}")

