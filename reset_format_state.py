#!/usr/bin/env python3
"""
Command-line utility to reset the state for a Pokémon Showdown format.
This allows the ETL process to fetch all replays from the beginning.

Usage:
    python reset_format_state.py <format_id>
    
Example:
    python reset_format_state.py gen9vgc2024regh
"""
import sys
import os
import json
from pathlib import Path

def reset_format_state(format_id):
    """Reset the state for a specific format."""
    # Determine the base directory
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    
    # Create the state file path
    state_file = os.path.join(base_dir, f"{format_id}_state.json")
    
    # Create a new state with default values
    new_state = {
        "last_seen_ts": 0,
        "oldest_ts": None,
        "last_processed_id": None,
        "format_id": format_id
    }
    
    # Save the state
    with open(state_file, 'w') as f:
        json.dump(new_state, f, indent=2)
    
    print(f"State for format '{format_id}' has been reset.")
    print(f"The next ETL run will fetch all replays from the beginning.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <format_id>")
        sys.exit(1)
    
    format_id = sys.argv[1]
    reset_format_state(format_id) 