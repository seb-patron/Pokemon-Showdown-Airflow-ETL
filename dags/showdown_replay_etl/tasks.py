"""
Task functions for the Showdown Replay ETL DAG.

This file is deprecated and kept for backward compatibility.
Use the modules in the tasks/ directory instead.
"""

# Re-export all task functions from their respective modules
from showdown_replay_etl.tasks.discovery import get_replay_ids, get_backfill_replay_ids
from showdown_replay_etl.tasks.download import download_replay, download_replays
from showdown_replay_etl.tasks.retry import retry_failed_replays
from showdown_replay_etl.tasks.compaction import compact_daily_replays, _batch_mark_replays_compacted

# Export all these symbols for backward compatibility
__all__ = [
    'get_replay_ids',
    'get_backfill_replay_ids',
    'download_replay',
    'download_replays',
    'retry_failed_replays',
    'compact_daily_replays',
    '_batch_mark_replays_compacted'
] 