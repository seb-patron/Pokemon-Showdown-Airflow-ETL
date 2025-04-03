# Pokémon Replay ETL Module

A modular Airflow implementation for extracting, transforming, and loading Pokémon Showdown battle replays.

## Module Structure

This module is organized as follows:

- `__init__.py` - Package marker
- `constants.py` - Configuration variables and constants
- `state.py` - State management functions
- `api.py` - API interaction functions
- `tasks.py` - Airflow task functions

## Components Overview

### Constants (`constants.py`)

Contains all configuration variables and constants:
- Directory paths
- Default format and page settings
- API URLs
- Directory creation logic

### State Management (`state.py`)

Provides functions for managing the ETL state:
- Loading state for a format
- Saving state
- Getting state file paths

### API Functions (`api.py`)

Handles all interactions with the Pokémon Showdown API:
- Fetching replay search pages
- Downloading individual replay data

### Tasks (`tasks.py`)

Implements the three main Airflow tasks:
- `get_replay_ids()`: Fetch replay IDs from the API
- `download_replays()`: Download and save replay data
- `retry_failed_replays()`: Retry previously failed downloads

## Main DAG File

The main DAG is defined in `pokemon_replay_etl_dag.py` in the parent directory, which imports the components from this module.

## Data Flow

1. First task fetches replay IDs and saves them to a file
2. Second task downloads replay data for each ID and organizes by date
3. Third task retries any failed downloads from previous runs

## State Tracking

States are stored in JSON files in the data directory. Key information tracked:
- Latest timestamp seen (`last_seen_ts`)
- Last processed ID (`last_processed_id`)
- Format ID (`format_id`)

## Organization Benefits

- **Better code organization**: Smaller, focused modules
- **Maintainability**: Easier to update or modify individual components
- **Testability**: Components can be tested independently
- **Readability**: Easier to understand the purpose of each part
- **Scalability**: New formats or features can be added with minimal changes 