# Pokémon Showdown Replay ETL with Airflow

This project provides an Apache Airflow DAG for extracting, transforming, and loading Pokémon Showdown battle replays. It's based on the original script from [seb-patron/VGC-Analysis](https://github.com/seb-patron/VGC-Analysis/blob/main/src/python/extract_replays.py).

## Features

- Fetches replay IDs for a specified format from Pokémon Showdown
- Downloads and stores replays in a date-organized directory structure
- Tracks processing state to avoid duplicating work
- Manages failed downloads with automatic retry mechanism
- Configurable via Airflow UI parameters

## Setup

1. Make sure you have Docker and Docker Compose installed
2. Clone this repository
3. Run `mkdir -p data/{replays,replay_ids,processed_ids,failed_ids}` to create necessary directories
4. Run `docker-compose up -d` to start Airflow
5. Access the Airflow UI at http://localhost:8080 (username: admin, password: admin)

## How to Use

The DAG includes the following parameters you can configure in the Airflow UI:

- `format_id`: The Pokémon Showdown format to fetch replays for (e.g., "gen9vgc2024regh")
- `max_pages`: Maximum number of pages to fetch from the API (default: 55)

The DAG consists of three tasks:

1. `get_replay_ids`: Fetches and saves replay IDs for the specified format
2. `download_replays`: Downloads replay data for each ID
3. `retry_failed_replays`: Retries any failed downloads from previous runs

## Data Organization

Replays are organized in the following directory structure:

```
/opt/airflow/data/
├── replays/                   # Main directory for replay data
│   └── {format_id}/           # Format-specific directory
│       └── {YYYY-MM-DD}/      # Date-specific directory
│           └── {replay_id}.json  # Replay data
├── replay_ids/                # Stores lists of replay IDs to process
├── processed_ids/             # Records of successfully processed IDs
└── failed_ids/                # Records of failed IDs for retry
```

## State Management

The DAG maintains state in JSON files stored in the data directory. This allows it to:

- Remember which replays have been processed
- Keep track of the latest timestamp seen for each format
- Resume processing where it left off after restart

## Customization

You can customize the DAG by editing the `dags/pokemon_replay_etl.py` file and adjusting:

- Schedule interval
- Default format
- Maximum pages to fetch
- Storage directories 