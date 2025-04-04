"""
Constants and configuration variables for the Showdown Replay ETL.
"""
import os

# Directory where data will be stored
BASE_DIR = os.path.join("/opt/airflow/data")
REPLAYS_DIR = os.path.join(BASE_DIR, "replays")
REPLAY_IDS_DIR = os.path.join(BASE_DIR, "replay_ids")
COMPACTED_REPLAYS_DIR = os.path.join(BASE_DIR, "compacted_replays")

# Default format to scrape replays for
DEFAULT_FORMAT = "gen9vgc2024regh"  # Default format, can be overridden by DAG param

# Default maximum number of pages to fetch (safety to avoid infinite loop)
DEFAULT_MAX_PAGES = 5

# API endpoints
SEARCH_API_URL = "https://replay.pokemonshowdown.com/search.json"
REPLAY_API_URL = "https://replay.pokemonshowdown.com/{}.json"

# Create necessary directories
for directory in [BASE_DIR, REPLAYS_DIR, REPLAY_IDS_DIR, COMPACTED_REPLAYS_DIR]:
    os.makedirs(directory, exist_ok=True) 