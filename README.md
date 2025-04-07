# Showdown Replay ETL with Airflow

This project provides an Apache Airflow DAG for extracting, transforming, and loading Pokémon Showdown battle replays. It downloads replay data which is then analyzed using the [VGC-Analysis](https://github.com/seb-patron/VGC-Analysis) Spark project.

## Features

- Fetches replay IDs for a specified format from Pokémon Showdown
- Downloads and stores replays in a date-organized directory structure
- Compacts daily replays into single files for easier analysis
- Tracks processing in a SQLite database for better reliability
- Manages failed downloads with automatic retry mechanism
- Configurable via Airflow UI parameters
- Modular code structure for better maintainability

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

## Code Organization

The codebase is organized in a modular structure for better readability and maintainability:

- `dags/showdown_replay_etl/constants.py`: Directory paths and default values
- `dags/showdown_replay_etl/api.py`: API interaction with Pokémon Showdown
- `dags/showdown_replay_etl/db.py`: Database interaction functions
- `dags/showdown_replay_etl/state.py`: State management utilities

Tasks are organized into separate modules based on functionality:

- `tasks/discovery.py`: Tasks related to discovering replay IDs from the Showdown API
  - `get_replay_ids`: Fetches new replay IDs for a format
  - `get_backfill_replay_ids`: Fetches older replay IDs for backfilling

- `tasks/download.py`: Tasks related to downloading replay data
  - `download_replay`: Downloads a single replay
  - `download_replays`: Main task for downloading multiple replays in parallel

- `tasks/retry.py`: Tasks related to retrying failed downloads
  - `retry_failed_replays`: Retries previously failed replay downloads

- `tasks/compaction.py`: Tasks related to compacting replays by date
  - `compact_daily_replays`: Compacts all replays for each date into a single file

## Data Analysis

Once the data is collected, it can be analyzed using the [VGC-Analysis](https://github.com/seb-patron/VGC-Analysis) project. This separate project provides a framework for analyzing Pokémon VGC battle data using Apache Spark, offering:

- Tools for processing large datasets of battle replays
- Jupyter notebooks for interactive analysis
- Pre-built analysis templates for common metrics (win rates, team compositions, etc.)
- Examples of how to derive insights from the replay data

Connect this Airflow ETL with the VGC-Analysis project by pointing the analysis notebooks to your compacted replay directory.

## Contributing

Contributions are welcome! Feel free to open issues or pull requests if you have suggestions for improvements. 