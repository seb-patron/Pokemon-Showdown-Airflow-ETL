"""
Tasks related to compacting daily replays into single files.
"""
import logging
import os
import json
from datetime import datetime

from airflow.models import TaskInstance

from showdown_replay_etl.constants import DEFAULT_FORMAT, REPLAYS_DIR, COMPACTED_REPLAYS_DIR
from showdown_replay_etl.db import (
    is_replay_compacted, get_replays_by_date, get_db_connection
)
from showdown_replay_etl.timer import time_process, timed, enable_detailed_timing

logger = logging.getLogger(__name__)

@timed(process_name="Batch mark replays as compacted")
def _batch_mark_replays_compacted(replays_dict):
    """
    Helper function to mark multiple replays as compacted in a single database transaction.
    
    Args:
        replays_dict: Dictionary where keys are replay_ids and values are tuples of 
                     (format_id, details, batch_id)
    """
    if not replays_dict:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    
    try:
        # Prepare batch update
        batch_data = []
        for replay_id, (format_id, details, batch_id) in replays_dict.items():
            batch_data.append((
                timestamp, 
                batch_id, 
                details,
                replay_id, 
                format_id
            ))
        
        # Execute batch update
        cursor.executemany('''
        UPDATE replay_status
        SET is_compacted = TRUE,
            compacted_at = ?,
            compacted_batch = ?,
            compacted_details = ?
        WHERE replay_id = ? AND format_id = ?
        ''', batch_data)
        
        conn.commit()
        logger.info(f"Successfully marked {len(batch_data)} replays as compacted in batch")
    except Exception as e:
        logger.error(f"Error marking replays as compacted in batch: {e}")
        conn.rollback()
    finally:
        conn.close()

def compact_daily_replays(**context):
    """
    Airflow task to compact all replays for each date into a single JSON file.
    This makes it easier to analyze and process the data in bulk.
    
    Creates files named like: format_YYYY-MM-DD.json in the compacted_replays directory.
    Each file contains an array of all replays for that date.
    
    If a compacted file already exists for a date, this function will append new replays to it.
    Uses the metadata database to track which replays have been compacted to avoid duplicates.
    
    Updates are now batched to prevent SQLite contention and improve performance.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    ignore_history = context['params'].get('ignore_history', False)
    
    # Enable detailed timing if requested
    enable_timing = context['params'].get('enable_detailed_timing', False)
    if enable_timing:
        enable_detailed_timing(True)
    
    # Create a unique batch ID for this compaction run
    compact_batch_id = f"compact_{format_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"Compacting daily replays for format: {format_id} (batch {compact_batch_id})")
    
    # Get the base directory for this format's replays
    format_dir = os.path.join(REPLAYS_DIR, format_id)
    
    if not os.path.exists(format_dir):
        logger.info(f"No replay directory found for format {format_id}")
        return
    
    # Get all date directories (YYYY-MM-DD)
    date_dirs = [d for d in os.listdir(format_dir) if os.path.isdir(os.path.join(format_dir, d))]
    
    if not date_dirs:
        logger.info(f"No date directories found for format {format_id}")
        return
    
    # Get all downloaded replays organized by date
    logger.info(f"Getting all downloaded replays organized by date for format {format_id}")
    
    with time_process("Fetching all replays organized by date from database"):
        replays_by_date = get_replays_by_date(format_id)
    
    total_replays_to_process = sum(len(ids) for ids in replays_by_date.values())
    logger.info(f"Found {total_replays_to_process} total downloaded replays across {len(replays_by_date)} dates")
    
    # Create format-specific directory for compacted replays
    compacted_format_dir = os.path.join(COMPACTED_REPLAYS_DIR, format_id)
    os.makedirs(compacted_format_dir, exist_ok=True)
    
    # Process each date separately
    total_compacted = 0
    stats = {
        "dates_processed": 0,
        "total_replays": total_replays_to_process,
        "replays_compacted": 0,
        "skipped": 0,
        "failed": 0,
        "by_date": {},
        # Add detailed categorization of skips
        "skipped_already_in_file": 0,
        "skipped_file_not_found": 0,
        "skipped_already_compacted": 0
    }
    
    # Dictionary to collect all replay IDs that need to be marked as compacted
    # Key is replay_id, value is (format_id, details, batch_id)
    replays_to_mark_compacted = {}
    batch_size = 500  # Process database updates in batches of this size
    
    for date_str, date_replay_ids in replays_by_date.items():
        if not date_replay_ids:
            continue
            
        date_dir = os.path.join(format_dir, date_str)
        successfully_compacted_for_date = []
        
        logger.info(f"Processing {len(date_replay_ids)} replays for date {date_str}")
        
        # Check if a compacted file already exists for this date
        output_file = os.path.join(compacted_format_dir, f"{date_str}.json")
        existing_replays = []
        existing_replay_ids = set()
        
        with time_process(f"Checking existing compacted file for date {date_str}"):
            if os.path.exists(output_file):
                logger.info(f"Found existing compacted file for {date_str}, will append to it")
                try:
                    with open(output_file, 'r') as f:
                        existing_replays = json.load(f)
                        # Extract replay IDs from existing data to avoid duplicates
                        for replay in existing_replays:
                            if 'id' in replay:
                                existing_replay_ids.add(replay['id'])
                    logger.info(f"Loaded {len(existing_replays)} existing replays from {output_file}")
                except Exception as e:
                    logger.error(f"Error loading existing compacted file {output_file}: {e}")
                    logger.info("Will create a new file instead")
                    existing_replays = []
                    existing_replay_ids = set()
        
        # Load all replays for this date
        new_replays = []
        
        with time_process(f"Processing {len(date_replay_ids)} replays for date {date_str}"):
            for replay_id in date_replay_ids:
                # Skip if already in the existing compacted file
                if replay_id in existing_replay_ids:
                    logger.debug(f"Replay {replay_id} is already in the compacted file, skipping")
                    stats["skipped"] += 1
                    stats["skipped_already_in_file"] += 1
                    continue
                    
                replay_file = os.path.join(date_dir, f"{replay_id}.json")
                
                if not os.path.exists(replay_file):
                    logger.warning(f"Replay file {replay_file} not found despite being marked as downloaded")
                    stats["skipped"] += 1
                    stats["skipped_file_not_found"] += 1
                    continue
                    
                # Check if already compacted (unless we're ignoring history)
                if not ignore_history and is_replay_compacted(replay_id, format_id):
                    logger.debug(f"Replay {replay_id} was already compacted in a previous run, skipping")
                    stats["skipped"] += 1
                    stats["skipped_already_compacted"] += 1
                    continue
                
                try:
                    with open(replay_file, 'r') as f:
                        replay_data = json.load(f)
                        new_replays.append(replay_data)
                        
                    # Add to the list to be marked as compacted
                    successfully_compacted_for_date.append(replay_id)
                    replays_to_mark_compacted[replay_id] = (
                        format_id, 
                        f"Compacted to {output_file}", 
                        compact_batch_id
                    )
                except Exception as e:
                    logger.error(f"Error loading replay file {replay_file}: {e}")
                    stats["failed"] += 1
                    continue
        
        if not new_replays and not existing_replays:
            logger.info(f"No replays to compact for date {date_str}")
            continue
            
        # Combine existing and new replays
        all_replays = existing_replays + new_replays
        logger.info(f"Compacting {len(new_replays)} new replays with {len(existing_replays)} existing replays for {date_str}")
        
        # Save the compacted file
        with time_process(f"Writing compacted file for date {date_str}"):
            with open(output_file, 'w') as f:
                json.dump(all_replays, f, indent=2)
        
        logger.info(f"Compacted {len(successfully_compacted_for_date)} new replays for {date_str} to {output_file}")
        
        stats["dates_processed"] += 1
        stats["by_date"][date_str] = len(successfully_compacted_for_date)
        stats["replays_compacted"] += len(successfully_compacted_for_date)
        total_compacted += len(successfully_compacted_for_date)
        
        # Process database updates in batches to avoid keeping the connection open too long
        if len(replays_to_mark_compacted) >= batch_size:
            with time_process(f"Marking batch of {len(replays_to_mark_compacted)} replays as compacted in database"):
                _batch_mark_replays_compacted(replays_to_mark_compacted)
                replays_to_mark_compacted = {}  # Reset after processing
    
    # Process any remaining replays to mark as compacted
    if replays_to_mark_compacted:
        with time_process(f"Marking final batch of {len(replays_to_mark_compacted)} replays as compacted in database"):
            _batch_mark_replays_compacted(replays_to_mark_compacted)
    
    # Log summary
    if total_compacted > 0:
        logger.info(f"Compaction complete for format {format_id}. "
                  f"Compacted {total_compacted} new replays across {stats['dates_processed']} dates. "
                  f"Skipped: {stats['skipped']} (Already in file: {stats['skipped_already_in_file']}, "
                  f"File not found: {stats['skipped_file_not_found']}, "
                  f"Already compacted: {stats['skipped_already_compacted']}), Failed: {stats['failed']}")
        
        # Push stats to XCom
        ti.xcom_push(key='compact_stats', value=stats)
    else:
        logger.info(f"No new replays compacted for format {format_id}")
        
        # Push empty stats to XCom
        ti.xcom_push(key='compact_stats', value={
            'dates_processed': 0,
            'replays_compacted': 0,
            'skipped': 0,
            'skipped_already_in_file': 0,
            'skipped_file_not_found': 0,
            'skipped_already_compacted': 0,
        }) 