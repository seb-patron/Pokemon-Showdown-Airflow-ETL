# Showdown Replay ETL with Airflow

This project provides an Apache Airflow DAG for extracting, transforming, and loading Pokémon Showdown battle replays. It's based on the original script from [seb-patron/VGC-Analysis](https://github.com/seb-patron/VGC-Analysis/blob/main/src/python/extract_replays.py).

## Features

- Fetches replay IDs for a specified format from Pokémon Showdown
- Downloads and stores replays in a date-organized directory structure
- Compacts daily replays into single files for easier analysis
- Tracks processing in a SQLite database for better reliability
- Manages failed downloads with automatic retry mechanism
- Configurable via Airflow UI parameters

## Setup

1. Make sure you have Docker and Docker Compose installed
2. Clone this repository
3. Run `docker-compose up -d` to start Airflow
4. Access the Airflow UI at http://localhost:8080 (username: admin, password: admin)

## How to Use

The DAG includes the following parameters you can configure in the Airflow UI:

- `format_id`: The Pokémon Showdown format to fetch replays for (e.g., "gen9vgc2024regh")
- `max_pages`: Maximum number of pages to fetch from the API (default: 55)

The DAG consists of four primary tasks:

1. `get_replay_ids`: Fetches and saves replay IDs for the specified format
2. `download_replays`: Downloads replay data for each ID
3. `retry_failed_replays`: Retries any failed downloads from previous runs
4. `compact_daily_replays`: Compacts all replays by date into single JSON files

## Data Organization

Replays are organized in the following directory structure:

```
/opt/airflow/data/
├── replays/                     # Main directory for individual replay data
│   └── {format_id}/             # Format-specific directory
│       └── {YYYY-MM-DD}/        # Date-specific directory
│           └── {replay_id}.json # Individual replay data
├── compacted_replays/           # Contains compacted replay files by date
│   └── {format_id}/             # Format-specific directory
│       └── {YYYY-MM-DD}.json    # Compacted file with all replays for the date
├── replay_ids/                  # Stores lists of replay IDs to process
└── replay_metadata.db           # SQLite database for tracking processing state
```

## Database Management

The ETL process uses a SQLite database to maintain state information:

- Tracks which replays have been downloaded and compacted
- Records error information for failed downloads
- Maintains metadata like upload timestamps and player information
- Provides resilience across restarts and failures

## Utilities

The project includes several utility scripts:

- `init_db.py`: Initialize the SQLite database schema
- `import_existing_replays.py`: Import existing replay files into the database
- `reset_format_state.py`: Reset the state for a specific format

## Customization

You can customize the DAG by editing the files in the `dags/showdown_replay_etl` directory:

- `constants.py`: Directory paths and default values
- `tasks.py`: Task implementations
- `db.py`: Database interaction functions
- `api.py`: API interaction with Pokémon Showdown 