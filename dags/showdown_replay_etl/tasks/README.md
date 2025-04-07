# Showdown Replay ETL Tasks

This directory contains the implementation of all Airflow tasks used in the Showdown Replay ETL pipeline.

## Module Organization

Tasks are organized by function into separate modules:

- **discovery.py**: Tasks related to discovering replay IDs from the Showdown API
  - `get_replay_ids`: Fetches new replay IDs for a format
  - `get_backfill_replay_ids`: Fetches older replay IDs for backfilling

- **download.py**: Tasks related to downloading replay data
  - `download_replay`: Downloads a single replay (helper function)
  - `download_replays`: Main task for downloading multiple replays in parallel

- **retry.py**: Tasks related to retrying failed downloads
  - `retry_failed_replays`: Retries previously failed replay downloads

- **compaction.py**: Tasks related to compacting replays by date
  - `compact_daily_replays`: Compacts all replays for each date into a single file
  - `_batch_mark_replays_compacted`: Helper function for batch database updates

## Usage in DAGs

All task functions are re-exported through the `__init__.py` file, so import statements in DAG files don't need to change:

```python
# This still works after refactoring
from showdown_replay_etl.tasks import get_replay_ids, download_replays
```

The module structure is purely for code organization and maintenance purposes. 