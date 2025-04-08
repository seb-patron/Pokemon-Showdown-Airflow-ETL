"""
Tasks package for Showdown Replay ETL DAG.
Contains modules for different stages of the ETL process.

Import the specific functions directly from their respective modules:
- from showdown_replay_etl.tasks.discovery import get_replay_ids, get_backfill_replay_ids
- from showdown_replay_etl.tasks.download import download_replay, download_replays
- from showdown_replay_etl.tasks.retry import retry_failed_replays
- from showdown_replay_etl.tasks.compaction import compact_daily_replays
"""
