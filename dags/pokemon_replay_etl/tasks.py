"""
Task functions for the Pokemon Replay ETL DAG.
"""
import logging
import os
import json
import traceback
from datetime import datetime
from typing import Dict, Any, List
import glob

from airflow.models import TaskInstance
from airflow.exceptions import AirflowSkipException

from .constants import (
    DEFAULT_FORMAT, DEFAULT_MAX_PAGES, REPLAYS_DIR, 
    REPLAY_IDS_DIR, PROCESSED_IDS_DIR, FAILED_IDS_DIR,
    COMPACTED_REPLAYS_DIR
)
from .state import load_state, save_state
from .api import fetch_replay_page, fetch_replay_data
from .db import record_processing, get_unprocessed_replays, get_processing_status

logger = logging.getLogger(__name__)

def get_replay_ids(**context):
    """
    Airflow task to fetch and save replay IDs for a format.
    Continues until it reaches the last processed ID or runs out of IDs.
    Also records the fetched IDs in the metadata database.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    max_pages = context['params'].get('max_pages', DEFAULT_MAX_PAGES)
    
    logger.info(f"Fetching replay IDs for format: {format_id}, max pages: {max_pages}")
    
    # Load the current state for this format
    state = load_state(format_id)
    last_seen_ts = state.get("last_seen_ts", 0)
    last_processed_id = state.get("last_processed_id")
    
    new_replay_ids = []
    new_reference_ts = None
    page = 0
    total_replays_found = 0
    before_ts = None
    done = False
    
    # Create format-specific directory for replay IDs
    format_dir = os.path.join(REPLAY_IDS_DIR, format_id)
    os.makedirs(format_dir, exist_ok=True)
    
    output_file = os.path.join(format_dir, f"{format_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    try:
        while page < max_pages and not done:
            logger.info(f"Fetching page {page+1} for {format_id}")
            
            replays = fetch_replay_page(format_id, before_ts)
            
            if not replays:
                logger.info(f"No replays found on page {page+1}")
                break
                
            total_replays_found += len(replays)
            logger.info(f"Found {len(replays)} replays on page {page+1}")
            
            # Process each replay on this page
            for rep in replays:
                r_id = rep["id"]
                r_time = rep["uploadtime"]
                
                # Check if we've reached the last seen timestamp
                if r_time <= last_seen_ts and last_seen_ts > 0:
                    logger.info(f"Reached already seen replay with timestamp {r_time}, stopping")
                    done = True
                    break
                
                # Track the max timestamp seen
                if not new_reference_ts or r_time > new_reference_ts:
                    new_reference_ts = r_time
                
                # If we've already processed this ID, skip it
                if r_id == last_processed_id:
                    logger.info(f"Reached already processed replay ID {r_id}, stopping")
                    done = True
                    break
                    
                # Add to our list of IDs to process
                new_replay_ids.append({
                    "id": r_id,
                    "uploadtime": r_time,
                    "format": format_id,
                    "players": rep.get("p1", "") + " vs " + rep.get("p2", "")
                })
                
                # Record that we've discovered this replay ID in the database
                record_processing(r_id, format_id, 'discovered', 'success', f"Discovered in page {page+1}")
            
            # Prepare for next page if needed
            if done or len(replays) < 51:  # 51 indicates more pages
                logger.info(f"Reached end of available replays (got {len(replays)} < 51)")
                break
                
            # Set 'before' to the uploadtime of the last replay in this page
            before_ts = replays[-1]["uploadtime"]
            page += 1
            
    except Exception as e:
        logger.error(f"Error fetching replays: {e}")
        logger.error(traceback.format_exc())
    
    # Save the new replay IDs to a file
    if new_replay_ids:
        with open(output_file, 'w') as f:
            json.dump(new_replay_ids, f, indent=2)
        logger.info(f"Saved {len(new_replay_ids)} replay IDs to {output_file}")
        
        # Update the state with the new timestamp
        if new_reference_ts and new_reference_ts > last_seen_ts:
            state["last_seen_ts"] = new_reference_ts
            save_state(state, format_id)
            
        # Pass the output file to the next task
        ti.xcom_push(key='replay_ids_file', value=output_file)
        ti.xcom_push(key='replay_count', value=len(new_replay_ids))
    else:
        logger.info("No new replays found")
        # Skip the next task if no new replays
        raise AirflowSkipException("No new replays found to process")

def download_replays(**context):
    """
    Airflow task to download replays from the list of IDs.
    Organizes replays by date and records processing status in the metadata database.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    
    # Get the file containing replay IDs from the previous task
    replay_ids_file = ti.xcom_pull(key='replay_ids_file', task_ids='get_replay_ids')
    
    if not replay_ids_file or not os.path.exists(replay_ids_file):
        logger.error(f"Replay IDs file not found: {replay_ids_file}")
        return
    
    # Load the replay IDs
    with open(replay_ids_file, 'r') as f:
        replay_list = json.load(f)
    
    logger.info(f"Processing {len(replay_list)} replays")
    
    # Load the current state
    state = load_state(format_id)
    
    # Track processing stats
    stats = {
        "downloaded": 0,
        "failed": 0
    }
    
    for replay_info in replay_list:
        replay_id = replay_info["id"]
        logger.info(f"Downloading replay: {replay_id}")
        
        try:
            # Fetch the replay data
            replay_data = fetch_replay_data(replay_id)
            
            if not replay_data:
                logger.error(f"Failed to download replay {replay_id}")
                stats["failed"] += 1
                record_processing(replay_id, format_id, 'downloaded', 'failed', 'Failed to download replay data')
                continue
            
            # Create a directory for this replay based on the date
            upload_time = datetime.fromtimestamp(replay_info["uploadtime"])
            date_str = upload_time.strftime("%Y-%m-%d")
            format_dir = os.path.join(REPLAYS_DIR, format_id)
            date_dir = os.path.join(format_dir, date_str)
            os.makedirs(date_dir, exist_ok=True)
            
            # Save the replay data
            replay_file = os.path.join(date_dir, f"{replay_id}.json")
            with open(replay_file, 'w') as f:
                json.dump(replay_data, f, indent=2)
            
            # Mark as processed in the database
            stats["downloaded"] += 1
            record_processing(replay_id, format_id, 'downloaded', 'success', replay_file)
            
            # Update the last processed ID in state
            state["last_processed_id"] = replay_id
            
        except Exception as e:
            logger.error(f"Error processing replay {replay_id}: {e}")
            logger.error(traceback.format_exc())
            stats["failed"] += 1
            record_processing(replay_id, format_id, 'downloaded', 'failed', str(e))
    
    # Save the updated state
    save_state(state, format_id)
    
    # Log summary
    logger.info(f"Download summary: {stats['downloaded']} downloaded, {stats['failed']} failed")
    
    # Push stats to XCom for potential use by other tasks
    ti.xcom_push(key='download_stats', value=stats)

def retry_failed_replays(**context):
    """
    Airflow task to retry downloading failed replays.
    Uses the metadata database to find failed downloads.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    
    # Query the database for failed downloads
    conn = None
    try:
        from .db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get replay IDs that failed in the download stage
        cursor.execute('''
        SELECT DISTINCT replay_id, details 
        FROM replay_processing 
        WHERE format_id = ? 
        AND stage = 'downloaded' 
        AND status = 'failed'
        AND NOT EXISTS (
            SELECT 1 FROM replay_processing 
            WHERE replay_id = replay_processing.replay_id 
            AND stage = 'downloaded' 
            AND status = 'success'
        )
        ''', (format_id,))
        
        failed_replays = cursor.fetchall()
    finally:
        if conn:
            conn.close()
    
    if not failed_replays:
        logger.info(f"No failed replays to retry for format {format_id}")
        return
    
    logger.info(f"Found {len(failed_replays)} failed replays to retry")
    
    # Load the current state
    state = load_state(format_id)
    
    # Track processing stats
    stats = {
        "retried": 0,
        "recovered": 0,
        "failed": 0
    }
    
    # We need additional details for each replay - query the first discovery of each
    for replay_data in failed_replays:
        replay_id = replay_data['replay_id']
        stats["retried"] += 1
        
        # Get additional info needed to download
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
            SELECT details FROM replay_processing 
            WHERE replay_id = ? AND stage = 'discovered' AND status = 'success'
            LIMIT 1
            ''', (replay_id,))
            discovery = cursor.fetchone()
            
            if not discovery:
                logger.error(f"No discovery info found for replay {replay_id}")
                stats["failed"] += 1
                continue
                
            # Get info about this replay from the database or API
            logger.info(f"Retrying download of replay: {replay_id}")
            
            # Fetch the replay data
            replay_data = fetch_replay_data(replay_id)
            
            if not replay_data:
                logger.error(f"Failed to download replay {replay_id} on retry")
                stats["failed"] += 1
                record_processing(replay_id, format_id, 'retry', 'failed', 'Failed to download replay data on retry')
                continue
            
            # Since we don't have upload time stored, use current date
            # In a real solution, we would store more metadata in the database
            date_str = datetime.now().strftime("%Y-%m-%d")
            format_dir = os.path.join(REPLAYS_DIR, format_id)
            date_dir = os.path.join(format_dir, date_str)
            os.makedirs(date_dir, exist_ok=True)
            
            # Save the replay data
            replay_file = os.path.join(date_dir, f"{replay_id}.json")
            with open(replay_file, 'w') as f:
                json.dump(replay_data, f, indent=2)
            
            # Mark as recovered
            stats["recovered"] += 1
            
            # Record in the metadata database
            record_processing(replay_id, format_id, 'downloaded', 'success', replay_file)
            record_processing(replay_id, format_id, 'retry', 'success', 'Successfully recovered on retry')
            
        except Exception as e:
            logger.error(f"Error retrying replay {replay_id}: {e}")
            logger.error(traceback.format_exc())
            stats["failed"] += 1
            record_processing(replay_id, format_id, 'retry', 'failed', str(e))
        finally:
            if conn:
                conn.close()
    
    # Log summary
    logger.info(f"Retry summary: {stats['retried']} retried, {stats['recovered']} recovered, {stats['failed']} still failed")
    
    # Push stats to XCom
    ti.xcom_push(key='retry_stats', value=stats)

def compact_daily_replays(**context):
    """
    Airflow task to compact all replays for each date into a single JSON file.
    This makes it easier to analyze and process the data in bulk.
    
    Creates files named like: format_YYYY-MM-DD_HHMMSS.json in the compacted_replays directory.
    Each file contains an array of all replays for that date.
    
    Uses the metadata database to track which replays have been compacted to avoid duplicates.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    
    logger.info(f"Compacting daily replays for format: {format_id}")
    
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
    
    # Get replay IDs that haven't been compacted yet
    uncompacted_replay_ids = get_unprocessed_replays(format_id, 'compacted')
    
    if not uncompacted_replay_ids:
        logger.info(f"No new replays to compact for format {format_id}")
        return
        
    logger.info(f"Found {len(uncompacted_replay_ids)} uncompacted replays for format {format_id}")
    
    # Create format-specific directory for compacted replays
    compacted_format_dir = os.path.join(COMPACTED_REPLAYS_DIR, format_id)
    os.makedirs(compacted_format_dir, exist_ok=True)
    
    # Process each date directory
    compacted_by_date = {}
    total_compacted = 0
    
    for date_str in date_dirs:
        date_dir = os.path.join(format_dir, date_str)
        replay_files = glob.glob(os.path.join(date_dir, "*.json"))
        
        if not replay_files:
            logger.info(f"No replay files found for date {date_str}")
            continue
        
        # Load replays that haven't been compacted yet
        new_replays = []
        for replay_file in replay_files:
            # Extract replay_id from filename (e.g., "gen9vgc2024regh-2273103153.json")
            replay_id = os.path.basename(replay_file).split('.')[0]
            
            # Only process replays that haven't been compacted yet
            if replay_id in uncompacted_replay_ids:
                try:
                    with open(replay_file, 'r') as f:
                        replay_data = json.load(f)
                        new_replays.append(replay_data)
                        
                    # Record successful compaction
                    record_processing(replay_id, format_id, 'compacted', 'success', f"Compacted in {date_str}")
                except Exception as e:
                    logger.error(f"Error loading replay file {replay_file}: {e}")
                    record_processing(replay_id, format_id, 'compacted', 'failed', str(e))
                    continue
        
        if not new_replays:
            logger.info(f"No new replays to compact for date {date_str}")
            continue
        
        # Check if we already have existing compacted data for this date
        existing_compacted_files = glob.glob(os.path.join(compacted_format_dir, f"{date_str}_*.json"))
        existing_replays = []
        
        if existing_compacted_files:
            # Load the most recent compacted file
            latest_file = max(existing_compacted_files)
            try:
                with open(latest_file, 'r') as f:
                    existing_replays = json.load(f)
                logger.info(f"Loaded {len(existing_replays)} existing replays from {latest_file}")
            except Exception as e:
                logger.error(f"Error loading existing compacted file {latest_file}: {e}")
        
        # Merge existing and new replays
        all_replays = existing_replays + new_replays
        
        # Create a timestamped filename for the new compacted file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(compacted_format_dir, f"{date_str}_{timestamp}.json")
        
        # Save the compacted file
        with open(output_file, 'w') as f:
            json.dump(all_replays, f, indent=2)
        
        logger.info(f"Compacted {len(new_replays)} new replays (total: {len(all_replays)}) for {date_str} to {output_file}")
        
        compacted_by_date[date_str] = len(new_replays)
        total_compacted += len(new_replays)
    
    # Log summary
    if total_compacted > 0:
        logger.info(f"Compacting complete for format {format_id}. Compacted {total_compacted} new replays across {len(compacted_by_date)} dates")
        
        # Push stats to XCom
        ti.xcom_push(key='compact_stats', value={
            'dates_processed': len(compacted_by_date),
            'replays_compacted': total_compacted,
            'by_date': compacted_by_date
        })
    else:
        logger.info(f"No new replays compacted for format {format_id}")
        
        # Push empty stats to XCom
        ti.xcom_push(key='compact_stats', value={
            'dates_processed': 0,
            'replays_compacted': 0,
            'by_date': {}
        }) 