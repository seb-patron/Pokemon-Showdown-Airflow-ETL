"""
Tasks related to retrying failed replay downloads.
"""
import logging
import os
import json
import time
import traceback
from datetime import datetime

from airflow.models import TaskInstance

from showdown_replay_etl.constants import DEFAULT_FORMAT, REPLAYS_DIR
from showdown_replay_etl.api import fetch_replay_data
from showdown_replay_etl.db import (
    get_failed_downloads, is_replay_downloaded, get_replay_metadata,
    mark_retry_attempt
)

logger = logging.getLogger(__name__)

def retry_failed_replays(**context):
    """
    Airflow task to retry downloading failed replays.
    Uses the metadata database to find failed downloads.
    Processes retries in batches for better resilience.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    
    # Create a unique batch ID for this retry run
    retry_batch_id = f"retry_{format_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Get failed downloads from the database
    failed_replays = get_failed_downloads(format_id)
    
    if not failed_replays:
        logger.info(f"No failed replays to retry for format {format_id}")
        return
    
    logger.info(f"Found {len(failed_replays)} failed replays to retry in batch {retry_batch_id}")
    
    # Track processing stats
    stats = {
        "total": len(failed_replays),
        "retried": 0,
        "recovered": 0,
        "failed": 0,
        "skipped": 0
    }
    
    # Process retries in batches
    BATCH_SIZE = 50  # Retry in smaller batches
    
    for batch_start in range(0, len(failed_replays), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(failed_replays))
        current_batch = failed_replays[batch_start:batch_end]
        
        logger.info(f"Processing retry mini-batch {batch_start//BATCH_SIZE + 1} of {(len(failed_replays)-1)//BATCH_SIZE + 1} "
                   f"(retries {batch_start+1}-{batch_end} of {len(failed_replays)})")
        
        # Process each failed replay in this batch
        for replay_id in current_batch:
            # Double-check that this replay hasn't been successfully downloaded since we queried
            if is_replay_downloaded(replay_id, format_id):
                logger.info(f"Replay {replay_id} was already successfully downloaded in a previous run, skipping")
                stats["skipped"] += 1
                continue
                
            stats["retried"] += 1
            
            try:
                # Get metadata for this replay 
                metadata = get_replay_metadata(replay_id)
                
                if not metadata:
                    logger.error(f"No metadata found for replay {replay_id}")
                    stats["failed"] += 1
                    mark_retry_attempt(replay_id, format_id, False, 
                                     f"No metadata found for replay", retry_batch_id)
                    continue
                
                logger.info(f"Retrying download of replay: {replay_id}")
                
                # Fetch the replay data - returns a tuple of (data, error)
                replay_data, error_msg = fetch_replay_data(replay_id)
                
                if not replay_data:
                    error_details = error_msg or "Failed to download replay data on retry (reason unknown)"
                    logger.error(f"Failed to download replay {replay_id} on retry: {error_details}")
                    stats["failed"] += 1
                    mark_retry_attempt(replay_id, format_id, False, error_details, retry_batch_id)
                    continue
                
                # Get the date from uploadtime in metadata
                upload_time = datetime.fromtimestamp(metadata['uploadtime'])
                date_str = upload_time.strftime("%Y-%m-%d")
                format_dir = os.path.join(REPLAYS_DIR, format_id)
                date_dir = os.path.join(format_dir, date_str)
                os.makedirs(date_dir, exist_ok=True)
                
                # Save the replay data
                replay_file = os.path.join(date_dir, f"{replay_id}.json")
                with open(replay_file, 'w') as f:
                    json.dump(replay_data, f, indent=2)
                
                # Mark as recovered
                stats["recovered"] += 1
                
                # Record successful retry in the database
                mark_retry_attempt(replay_id, format_id, True, 
                                 f"Successfully recovered on retry - saved to {replay_file}", 
                                 retry_batch_id)
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error retrying replay {replay_id}: {error_msg}")
                logger.error(traceback.format_exc())
                stats["failed"] += 1
                mark_retry_attempt(replay_id, format_id, False, 
                                 f"Error: {error_msg}", retry_batch_id)
        
        # Log progress after each mini-batch
        logger.info(f"Mini-batch retry progress: {stats['recovered']} recovered, "
                   f"{stats['failed']} still failed, {stats['skipped']} skipped "
                   f"(total: {stats['recovered'] + stats['failed'] + stats['skipped']}/{stats['total']})")
        
        # Save progress to XCom after each mini-batch
        ti.xcom_push(key=f'retry_progress_{batch_start}', value={
            'batch_start': batch_start,
            'batch_end': batch_end,
            'stats': stats
        })
        
        # Add a small delay between batches
        if batch_end < len(failed_replays):
            time.sleep(1)
    
    # Log summary
    logger.info(f"Retry summary: {stats['retried']} retried, {stats['recovered']} recovered, "
               f"{stats['failed']} still failed, {stats['skipped']} skipped")
    
    # Push stats to XCom
    ti.xcom_push(key='retry_stats', value=stats) 