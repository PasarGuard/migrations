"""
Helper utility functions.
"""

from typing import Dict, Any


def confirm_action(prompt: str) -> bool:
    """
    Ask user to confirm an action.
    
    Args:
        prompt: Confirmation prompt message
        
    Returns:
        True if user confirms, False otherwise
    """
    while True:
        response = input(f"{prompt} (yes/no): ").strip().lower()
        if response in ('yes', 'y'):
            return True
        elif response in ('no', 'n'):
            return False
        else:
            print("Please enter 'yes' or 'no'")


def print_statistics(stats: Dict[str, Any], title: str = "STATISTICS"):
    """
    Print statistics in a formatted table.
    
    Args:
        stats: Dictionary of statistics to print
        title: Title for the statistics table
    """
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)
    
    max_key_length = max(len(str(k)) for k in stats.keys()) if stats else 0
    
    for key, value in stats.items():
        key_str = str(key).ljust(max_key_length)
        print(f"  {key_str}: {value}")
    
    print("=" * 70)


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string (e.g., "1h 23m 45s")
    """
    if seconds < 0:
        return "0s"
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    milliseconds = int((seconds % 1) * 1000)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    if milliseconds > 0 and seconds < 60:
        parts.append(f"{milliseconds}ms")
    
    return " ".join(parts) if parts else "0s"

