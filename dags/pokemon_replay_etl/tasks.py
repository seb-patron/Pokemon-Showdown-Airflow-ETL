"""
Task functions for the Pokemon Replay ETL DAG.
"""
import logging
import os
import json
import traceback
from datetime import datetime
from typing import Dict, Any, List

from airflow.models import TaskInstance
from airflow.exceptions import AirflowSkipException

from .constants import (
    DEFAULT_FORMAT, DEFAULT_MAX_PAGES, REPLAYS_DIR, 
    REPLAY_IDS_DIR, PROCESSED_IDS_DIR, FAILED_IDS_DIR
)
from .state import load_state, save_state
from .api import fetch_replay_page, fetch_replay_data

logger = logging.getLogger(__name__)

def get_replay_ids(**context):
    """
    Airflow task to fetch and save replay IDs for a format.
    Continues until it reaches the last processed ID or runs out of IDs.
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
    
    output_file = os.path.join(REPLAY_IDS_DIR, f"{format_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
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
    Organizes replays by date and marks each ID as processed or failed.
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
    
    processed_ids = []
    failed_ids = []
    
    for replay_info in replay_list:
        replay_id = replay_info["id"]
        logger.info(f"Downloading replay: {replay_id}")
        
        try:
            # Fetch the replay data
            replay_data = fetch_replay_data(replay_id)
            
            if not replay_data:
                logger.error(f"Failed to download replay {replay_id}")
                failed_ids.append(replay_info)
                stats["failed"] += 1
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
            
            # Mark as processed
            processed_ids.append(replay_info)
            stats["downloaded"] += 1
            
            # Update the last processed ID in state
            state["last_processed_id"] = replay_id
            
        except Exception as e:
            logger.error(f"Error processing replay {replay_id}: {e}")
            logger.error(traceback.format_exc())
            failed_ids.append(replay_info)
            stats["failed"] += 1
    
    # Save processed and failed IDs
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if processed_ids:
        processed_file = os.path.join(PROCESSED_IDS_DIR, f"{format_id}_{timestamp}_processed.json")
        with open(processed_file, 'w') as f:
            json.dump(processed_ids, f, indent=2)
        logger.info(f"Saved {len(processed_ids)} processed IDs to {processed_file}")
    
    if failed_ids:
        failed_file = os.path.join(FAILED_IDS_DIR, f"{format_id}_{timestamp}_failed.json")
        with open(failed_file, 'w') as f:
            json.dump(failed_ids, f, indent=2)
        logger.info(f"Saved {len(failed_ids)} failed IDs to {failed_file}")
    
    # Save the updated state
    save_state(state, format_id)
    
    # Log summary
    logger.info(f"Download summary: {stats['downloaded']} downloaded, {stats['failed']} failed")
    
    # Push stats to XCom for potential use by other tasks
    ti.xcom_push(key='download_stats', value=stats)

def retry_failed_replays(**context):
    """
    Airflow task to retry downloading failed replays.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    
    # Find all failed replay files for this format
    failed_files = []
    for filename in os.listdir(FAILED_IDS_DIR):
        if filename.startswith(f"{format_id}_") and filename.endswith("_failed.json"):
            failed_files.append(os.path.join(FAILED_IDS_DIR, filename))
    
    if not failed_files:
        logger.info(f"No failed replays to retry for format {format_id}")
        return
    
    logger.info(f"Found {len(failed_files)} failed replay files to retry")
    
    # Load the current state
    state = load_state(format_id)
    
    # Track processing stats
    stats = {
        "retried": 0,
        "recovered": 0,
        "failed": 0
    }
    
    recovered_ids = []
    still_failed_ids = []
    
    # Process each failed file
    for failed_file in failed_files:
        with open(failed_file, 'r') as f:
            failed_replays = json.load(f)
        
        logger.info(f"Retrying {len(failed_replays)} failed replays from {failed_file}")
        stats["retried"] += len(failed_replays)
        
        for replay_info in failed_replays:
            replay_id = replay_info["id"]
            logger.info(f"Retrying download of replay: {replay_id}")
            
            try:
                # Fetch the replay data
                replay_data = fetch_replay_data(replay_id)
                
                if not replay_data:
                    logger.error(f"Failed to download replay {replay_id} on retry")
                    still_failed_ids.append(replay_info)
                    stats["failed"] += 1
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
                
                # Mark as recovered
                recovered_ids.append(replay_info)
                stats["recovered"] += 1
                
            except Exception as e:
                logger.error(f"Error retrying replay {replay_id}: {e}")
                logger.error(traceback.format_exc())
                still_failed_ids.append(replay_info)
                stats["failed"] += 1
        
        # Delete the processed failed file
        os.remove(failed_file)
    
    # Save recovered and still-failed IDs
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if recovered_ids:
        recovered_file = os.path.join(PROCESSED_IDS_DIR, f"{format_id}_{timestamp}_recovered.json")
        with open(recovered_file, 'w') as f:
            json.dump(recovered_ids, f, indent=2)
        logger.info(f"Saved {len(recovered_ids)} recovered IDs to {recovered_file}")
    
    if still_failed_ids:
        still_failed_file = os.path.join(FAILED_IDS_DIR, f"{format_id}_{timestamp}_failed.json")
        with open(still_failed_file, 'w') as f:
            json.dump(still_failed_ids, f, indent=2)
        logger.info(f"Saved {len(still_failed_ids)} still-failed IDs to {still_failed_file}")
    
    # Log summary
    logger.info(f"Retry summary: {stats['retried']} retried, {stats['recovered']} recovered, {stats['failed']} still failed")
    
    # Push stats to XCom
    ti.xcom_push(key='retry_stats', value=stats) 