"""
API functions for interacting with the Pokemon Showdown API.
"""
import logging
import requests
import time
from typing import List, Dict, Any, Optional

from showdown_replay_etl.constants import SEARCH_API_URL, REPLAY_API_URL

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
        # Add timeouts: 5s connect, 30s read
        response = requests.get(SEARCH_API_URL, params=params, timeout=(5, 30))
        response.raise_for_status()
        return response.json()
    except requests.Timeout:
        logger.error(f"Timeout error fetching replay page for {format_id}")
        return []
    except requests.ConnectionError:
        logger.error(f"Connection error fetching replay page for {format_id}")
        return []
    except requests.RequestException as e:
        logger.error(f"Error fetching replay page: {e}")
        return []

def fetch_replay_data(replay_id: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch the data for a single replay.
    
    Args:
        replay_id: The ID of the replay to fetch
        
    Returns:
        A tuple with (replay_data, error_message) where:
        - replay_data: The replay data dictionary or None if an error occurred
        - error_message: Error description if an error occurred, otherwise None
    """
    url = REPLAY_API_URL.format(replay_id)
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            # Add timeouts: 3s connect, 20s read
            response = requests.get(url, timeout=(3.05, 20))
            response.raise_for_status()
            # Small delay after successful request to avoid overwhelming the server
            time.sleep(0.1)
            return response.json(), None
        except requests.Timeout:
            error_msg = f"Timeout error fetching replay {replay_id} (attempt {retry_count+1}/{max_retries})"
            logger.error(error_msg)
            retry_count += 1
            if retry_count < max_retries:
                # Back off exponentially (0.5s, 1s, 2s...)
                time.sleep(0.5 * (2 ** (retry_count - 1)))
                continue
            return None, error_msg
        except requests.ConnectionError:
            error_msg = f"Connection error fetching replay {replay_id} (attempt {retry_count+1}/{max_retries})"
            logger.error(error_msg)
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(0.5 * (2 ** (retry_count - 1)))
                continue
            return None, error_msg
        except requests.HTTPError as e:
            error_msg = f"HTTP error ({e.response.status_code}) fetching replay {replay_id}: {e}"
            logger.error(error_msg)
            # For certain status codes (like 404), no point in retrying
            if e.response.status_code in (404, 403, 401):
                return None, error_msg
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(0.5 * (2 ** (retry_count - 1)))
                continue
            return None, error_msg
        except requests.RequestException as e:
            error_msg = f"Error fetching replay {replay_id}: {e}"
            logger.error(error_msg)
            return None, error_msg 