"""
State management functions for tracking processed replays and timestamps.
"""
import json
import logging
import os
from typing import Dict, Any

from .constants import BASE_DIR

logger = logging.getLogger(__name__)

def get_state_file_path(format_id: str) -> str:
    """Get the path to the state file for a format."""
    return os.path.join(BASE_DIR, f"{format_id}_state.json")

def load_state(format_id: str) -> Dict[str, Any]:
    """
    Load the state for a specific format.
    
    Args:
        format_id: The format ID to load state for
        
    Returns:
        A dictionary containing the state
    """
    state_file = get_state_file_path(format_id)
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return json.load(f)
    return {
        "last_seen_ts": 0,
        "oldest_ts": None,
        "last_processed_id": None,
        "format_id": format_id
    }

def save_state(state: Dict[str, Any], format_id: str) -> None:
    """
    Save the state for a specific format.
    
    Args:
        state: The state dictionary to save
        format_id: The format ID
    """
    state_file = get_state_file_path(format_id)
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)
    logger.info(f"State saved to {state_file}") 