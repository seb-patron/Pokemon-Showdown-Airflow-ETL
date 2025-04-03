"""
API functions for interacting with the Pokemon Showdown API.
"""
import logging
import requests
from typing import List, Dict, Any, Optional

from .constants import SEARCH_API_URL, REPLAY_API_URL

logger = logging.getLogger(__name__)

def fetch_replay_page(format_id: str, before_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch a page of replay search results.
    
    Args:
        format_id: The format ID (e.g. "gen9vgc2024regh")
        before_ts: Optional timestamp to get replays from before this time
        
    Returns:
        List of replay data dictionaries
    """
    params = {"format": format_id}
    
    if before_ts:
        params["before"] = before_ts
        
    try:
        response = requests.get(SEARCH_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching replay page: {e}")
        return []

def fetch_replay_data(replay_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch the data for a single replay.
    
    Args:
        replay_id: The ID of the replay to fetch
        
    Returns:
        The replay data dictionary or None if an error occurred
    """
    url = REPLAY_API_URL.format(replay_id)
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching replay {replay_id}: {e}")
        return None 