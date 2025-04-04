"""
Task functions for the Showdown Replay ETL DAG.
"""
import logging
import os
import json
import time
import traceback
import uuid
from datetime import datetime
from typing import Dict, Any, List
import glob

from airflow.models import TaskInstance
from airflow.exceptions import AirflowSkipException

from showdown_replay_etl.constants import (
    DEFAULT_FORMAT, DEFAULT_MAX_PAGES, REPLAYS_DIR, 
    REPLAY_IDS_DIR, COMPACTED_REPLAYS_DIR
)
from showdown_replay_etl.state import load_state, save_state
from showdown_replay_etl.api import fetch_replay_page, fetch_replay_data
from showdown_replay_etl.db import (
    record_replay_discovery, get_undownloaded_replays, get_downloaded_uncompacted_replays,
    is_replay_downloaded, is_replay_compacted, mark_replay_downloaded, mark_download_failed,
    mark_replay_compacted, mark_retry_attempt, get_failed_downloads,
    get_latest_uploadtime, get_replay_metadata, get_stats_by_format, get_replays_by_date,
    batch_record_replay_discoveries, check_replays_existence, batch_mark_replays_downloaded,
    batch_mark_replays_failed
)

logger = logging.getLogger(__name__)

def get_replay_ids(**context):
    """
    Airflow task to fetch and save replay IDs for a format.
    Continues until it reaches the last processed ID or runs out of IDs.
    Records the fetched IDs directly in the metadata database using batch processing.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    max_pages = context['params'].get('max_pages', DEFAULT_MAX_PAGES)
    ignore_history = context['params'].get('ignore_history', False)
    
    # Batch processing config
    BATCH_SIZE = 50  # Number of replays to process in a single database transaction
    
    logger.info(f"Fetching replay IDs for format: {format_id}, max pages: {max_pages}, ignore_history: {ignore_history}")
    
    # Get the last processed information from the database
    last_seen_ts = 0
    
    if not ignore_history:
        # Use the latest uploadtime function to get the most recent timestamp
        latest_ts = get_latest_uploadtime(format_id)
        if latest_ts:
            last_seen_ts = latest_ts
            logger.info(f"Last seen timestamp: {last_seen_ts}")
        else:
            logger.info("No previously processed replays found in database")
    else:
        logger.info("Ignoring history, will process all available replays")
    
    # Create a unique batch ID for this run to track related replays
    batch_id = f"{format_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    new_replay_count = 0
    page = 0
    total_replays_found = 0
    before_ts = None
    done = False
    
    try:
        while page < max_pages and not done:
            logger.info(f"Fetching page {page+1} for {format_id}")
            
            replays = fetch_replay_page(format_id, before_ts)
            
            if not replays:
                logger.info(f"No replays found on page {page+1}")
                break
                
            total_replays_found += len(replays)
            logger.info(f"Found {len(replays)} replays on page {page+1}")
            
            # Add a small delay between page fetches to avoid API rate limits
            if page > 0:
                time.sleep(0.5)
            
            # Extract IDs from all replays on this page
            replay_ids = [rep["id"] for rep in replays]
            replay_map = {rep["id"]: rep for rep in replays}
            
            # Check which replays are already downloaded (in a single batch query)
            if not ignore_history:
                logger.info(f"Checking existence of {len(replay_ids)} replay IDs in database")
                existing_replays = check_replays_existence(replay_ids, format_id)
                
                # Filter out already downloaded replays
                new_replay_ids = [r_id for r_id, is_downloaded in existing_replays.items() 
                                  if not is_downloaded]
                
                if len(new_replay_ids) < len(replay_ids):
                    logger.info(f"Filtered out {len(replay_ids) - len(new_replay_ids)} already downloaded replays")
            else:
                # If ignoring history, process all replays
                new_replay_ids = replay_ids
            
            # Process replays in batches
            batch_to_store = []
            
            for i in range(0, len(new_replay_ids), BATCH_SIZE):
                current_batch_ids = new_replay_ids[i:i+BATCH_SIZE]
                
                # Check for reaching last seen timestamp
                if not ignore_history and last_seen_ts > 0:
                    # Check if any replay in this batch is older than our last seen timestamp
                    for r_id in current_batch_ids:
                        rep = replay_map[r_id]
                        r_time = rep["uploadtime"]
                        
                        if r_time <= last_seen_ts:
                            logger.info(f"Reached already seen replay with timestamp {r_time}, stopping")
                            done = True
                            break
                
                if done:
                    break
                    
                # Prepare batch for database insertion
                batch_data = []
                for r_id in current_batch_ids:
                    rep = replay_map[r_id]
                    batch_data.append({
                        "id": r_id,
                        "uploadtime": rep["uploadtime"],
                        "format": format_id,
                        "players": rep.get("p1", "") + " vs " + rep.get("p2", ""),
                        "page": page+1
                    })
                
                if batch_data:
                    logger.info(f"Processing batch of {len(batch_data)} replays")
                    batch_record_replay_discoveries(batch_data, format_id, batch_id)
                    new_replay_count += len(batch_data)
            
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
    
    # Process summary and prepare for the next task
    if new_replay_count > 0:
        logger.info(f"Added {new_replay_count} new replay IDs to the database")
        
        # Pass data to the next task
        ti.xcom_push(key='replay_count', value=new_replay_count)
        ti.xcom_push(key='batch_id', value=batch_id)
        
        # Build a list of replay IDs to pass to the download task directly
        undownloaded_replays = get_undownloaded_replays(format_id)
        replay_ids_to_download = [r['replay_id'] for r in undownloaded_replays]
        ti.xcom_push(key='replay_ids_to_download', value=replay_ids_to_download)
    else:
        logger.info("No new replays found")
        # Skip the next task if no new replays
        raise AirflowSkipException("No new replays found to process")

def download_replays(**context):
    """
    Airflow task to download replays from the database.
    Organizes replays by date and records processing status in the metadata database.
    Processes replays in batches for better resilience.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    ignore_history = context['params'].get('ignore_history', False)
    
    # Get batch ID from previous task
    batch_id = ti.xcom_pull(key='batch_id', task_ids='get_replay_ids')
    replay_count = ti.xcom_pull(key='replay_count', task_ids='get_replay_ids')
    
    # Get replay IDs directly from the database instead of loading from a file
    replay_ids = ti.xcom_pull(key='replay_ids_to_download', task_ids='get_replay_ids')
    
    if not replay_ids:
        # Fallback to querying the database directly
        logger.info("No replay IDs received from previous task, querying database directly")
        undownloaded_replays = get_undownloaded_replays(format_id)
        if not undownloaded_replays:
            logger.info("No undownloaded replays found in the database")
            return
            
        # Extract just the replay IDs from the query results
        replay_ids = [r['replay_id'] for r in undownloaded_replays]
    
    total_replays = len(replay_ids)
    logger.info(f"Processing {total_replays} replays in batch {batch_id}")
    
    # Track processing stats
    stats = {
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "total": total_replays
    }
    
    # Process replays in smaller batches to allow for better resilience
    BATCH_SIZE = 100  # Process 100 replays at a time
    MARK_BATCH = 10  # Number of replays to mark in a batch
    
    # Track downloads for batch processing
    successful_downloads = []
    failed_downloads = []
    
    for batch_start in range(0, total_replays, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_replays)
        current_batch = replay_ids[batch_start:batch_end]
        
        logger.info(f"Processing mini-batch {batch_start//BATCH_SIZE + 1} of {(total_replays-1)//BATCH_SIZE + 1} "
                    f"(replays {batch_start+1}-{batch_end} of {total_replays})")
        
        for replay_id in current_batch:
            # Get metadata for this replay
            metadata = get_replay_metadata(replay_id)
            
            if not metadata:
                logger.error(f"No metadata found for replay {replay_id}")
                stats["failed"] += 1
                failed_downloads.append({
                    'replay_id': replay_id,
                    'error': 'No metadata found for replay'
                })
                continue
                
            uploadtime = metadata['uploadtime']
                
            # Check if already processed (may have been processed in a previous failed run)
            if not ignore_history and is_replay_downloaded(replay_id, format_id):
                logger.info(f"Replay {replay_id} already downloaded, skipping")
                stats["skipped"] += 1
                continue
                
            logger.info(f"Downloading replay: {replay_id}")
            
            try:
                # Fetch the replay data - returns a tuple of (data, error)
                replay_data, error_msg = fetch_replay_data(replay_id)
                
                if not replay_data:
                    error_details = error_msg or "Failed to download replay data (reason unknown)"
                    logger.error(f"Failed to download replay {replay_id}: {error_details}")
                    stats["failed"] += 1
                    failed_downloads.append({
                        'replay_id': replay_id,
                        'error': error_details
                    })
                    continue
                
                # Create a directory for this replay based on the date
                upload_time = datetime.fromtimestamp(uploadtime)
                date_str = upload_time.strftime("%Y-%m-%d")
                format_dir = os.path.join(REPLAYS_DIR, format_id)
                date_dir = os.path.join(format_dir, date_str)
                os.makedirs(date_dir, exist_ok=True)
                
                # Save the replay data
                replay_file = os.path.join(date_dir, f"{replay_id}.json")
                with open(replay_file, 'w') as f:
                    json.dump(replay_data, f, indent=2)
                
                # Track for batch mark as downloaded
                successful_downloads.append({
                    'replay_id': replay_id,
                    'details': f"Downloaded to {replay_file}"
                })
                
                stats["downloaded"] += 1
                
                # If we've reached our batch size for marking downloads, process them
                if len(successful_downloads) >= MARK_BATCH:
                    batch_mark_replays_downloaded(successful_downloads, format_id, batch_id)
                    successful_downloads = []
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error processing replay {replay_id}: {error_msg}")
                logger.error(traceback.format_exc())
                stats["failed"] += 1
                failed_downloads.append({
                    'replay_id': replay_id,
                    'error': error_msg
                })
                
                # If we've reached our batch size for marking failures, process them
                if len(failed_downloads) >= MARK_BATCH:
                    batch_mark_replays_failed(failed_downloads, format_id, batch_id)
                    failed_downloads = []
        
        # Process any remaining downloads
        if successful_downloads:
            batch_mark_replays_downloaded(successful_downloads, format_id, batch_id)
            successful_downloads = []
            
        if failed_downloads:
            batch_mark_replays_failed(failed_downloads, format_id, batch_id)
            failed_downloads = []
        
        # Log progress after each mini-batch
        logger.info(f"Mini-batch progress: {stats['downloaded']} downloaded, "
                    f"{stats['failed']} failed, {stats['skipped']} skipped "
                    f"(total: {stats['downloaded'] + stats['failed'] + stats['skipped']}/{total_replays})")
        
        # Save progress to XCom at each mini-batch - this helps track progress even if the task fails
        ti.xcom_push(key=f'download_progress_{batch_start}', value={
            'batch_start': batch_start,
            'batch_end': batch_end,
            'stats': stats
        })
        
        # Add a small delay between batches to avoid overloading the services
        if batch_end < total_replays:
            time.sleep(1)
    
    # Log final summary
    logger.info(f"Download summary: {stats['downloaded']} downloaded, "
                f"{stats['failed']} failed, {stats['skipped']} skipped out of {total_replays} total")
    
    # Push final stats to XCom for potential use by other tasks
    ti.xcom_push(key='download_stats', value=stats)

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

def compact_daily_replays(**context):
    """
    Airflow task to compact all replays for each date into a single JSON file.
    This makes it easier to analyze and process the data in bulk.
    
    Creates files named like: format_YYYY-MM-DD.json in the compacted_replays directory.
    Each file contains an array of all replays for that date.
    
    If a compacted file already exists for a date, this function will append new replays to it.
    Uses the metadata database to track which replays have been compacted to avoid duplicates.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    ignore_history = context['params'].get('ignore_history', False)
    
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
        "by_date": {}
    }
    
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
        for replay_id in date_replay_ids:
            # Skip if already in the existing compacted file
            if replay_id in existing_replay_ids:
                logger.debug(f"Replay {replay_id} is already in the compacted file, skipping")
                stats["skipped"] += 1
                continue
                
            replay_file = os.path.join(date_dir, f"{replay_id}.json")
            
            if not os.path.exists(replay_file):
                logger.warning(f"Replay file {replay_file} not found despite being marked as downloaded")
                stats["skipped"] += 1
                continue
                
            # Check if already compacted (unless we're ignoring history)
            if not ignore_history and is_replay_compacted(replay_id, format_id):
                logger.debug(f"Replay {replay_id} was already compacted in a previous run, skipping")
                stats["skipped"] += 1
                continue
            
            try:
                with open(replay_file, 'r') as f:
                    replay_data = json.load(f)
                    new_replays.append(replay_data)
                    
                # Mark for successful compaction
                successfully_compacted_for_date.append(replay_id)
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
        with open(output_file, 'w') as f:
            json.dump(all_replays, f, indent=2)
        
        # Mark all replays as compacted in the database
        for replay_id in successfully_compacted_for_date:
            mark_replay_compacted(replay_id, format_id, 
                                f"Compacted to {output_file}", compact_batch_id)
        
        logger.info(f"Compacted {len(successfully_compacted_for_date)} new replays for {date_str} to {output_file}")
        
        stats["dates_processed"] += 1
        stats["by_date"][date_str] = len(successfully_compacted_for_date)
        stats["replays_compacted"] += len(successfully_compacted_for_date)
        total_compacted += len(successfully_compacted_for_date)
    
    # Log summary
    if total_compacted > 0:
        logger.info(f"Compaction complete for format {format_id}. "
                  f"Compacted {total_compacted} new replays across {stats['dates_processed']} dates. "
                  f"Skipped: {stats['skipped']}, Failed: {stats['failed']}")
        
        # Push stats to XCom
        ti.xcom_push(key='compact_stats', value=stats)
    else:
        logger.info(f"No new replays compacted for format {format_id}")
        
        # Push empty stats to XCom
        ti.xcom_push(key='compact_stats', value={
            'dates_processed': 0,
            'replays_compacted': 0,
            'skipped': 0,
            'failed': 0,
            'by_date': {}
        }) 