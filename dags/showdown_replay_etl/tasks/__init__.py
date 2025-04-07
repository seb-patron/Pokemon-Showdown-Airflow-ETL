"""
Tasks for the Showdown Replay ETL DAG.
This module imports and re-exports all task functions to maintain backwards compatibility.
"""

from showdown_replay_etl.tasks.discovery import get_replay_ids, get_backfill_replay_ids
from showdown_replay_etl.tasks.download import download_replay, download_replays
from showdown_replay_etl.tasks.retry import retry_failed_replays
from showdown_replay_etl.tasks.compaction import compact_daily_replays

# Re-export all tasks to maintain compatibility with existing code
__all__ = [
    'get_replay_ids',
    'get_backfill_replay_ids',
    'download_replay',
    'download_replays',
    'retry_failed_replays',
    'compact_daily_replays'
]
