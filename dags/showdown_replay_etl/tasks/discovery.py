"""
Tasks related to discovering replay IDs from Showdown API.
"""
import logging
import time
import traceback
from datetime import datetime
from airflow.models import TaskInstance
from airflow.exceptions import AirflowSkipException

from showdown_replay_etl.constants import DEFAULT_FORMAT, DEFAULT_MAX_PAGES
from showdown_replay_etl.api import fetch_replay_page
from showdown_replay_etl.db import (
    get_latest_uploadtime, get_oldest_uploadtime, 
    get_undownloaded_replays, efficiently_record_replays
)
from showdown_replay_etl.timer import time_process, timed, enable_detailed_timing

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
    
    # Enable detailed timing if requested
    enable_timing = context['params'].get('enable_detailed_timing', False)
    if enable_timing:
        enable_detailed_timing(True)
    
    logger.info(f"Fetching replay IDs for format: {format_id}, max pages: {max_pages}, ignore_history: {ignore_history}")
    
    # Get the last processed information from the database
    last_seen_ts = 0
    
    with time_process("Getting latest timestamp"):
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
            
            with time_process(f"Fetching page {page+1} of replay IDs"):
                replays = fetch_replay_page(format_id, before_ts)
            
            if not replays:
                logger.info(f"No replays found on page {page+1}")
                break
                
            total_replays_found += len(replays)
            logger.info(f"Found {len(replays)} replays on page {page+1}")
            
            # Add a small delay between page fetches to avoid API rate limits
            if page > 0:
                time.sleep(0.1)
            
            # Check if we need to filter based on last seen timestamp
            if not ignore_history and last_seen_ts > 0:
                with time_process("Filtering already seen replays"):
                    # Filter out replays with timestamps we've already seen
                    filtered_replays = []
                    for rep in replays:
                        r_time = rep["uploadtime"]
                        if r_time > last_seen_ts:
                            filtered_replays.append(rep)
                        else:
                            logger.info(f"Skipping replay with timestamp {r_time} <= {last_seen_ts}")
                            done = True
                    
                    if len(filtered_replays) < len(replays):
                        logger.info(f"Filtered out {len(replays) - len(filtered_replays)} already seen replays")
                        replays = filtered_replays
                    
                    if not replays:
                        logger.info("No new replays to process after filtering")
                        break
            
            # Use the efficient method to record replays in a single database transaction
            with time_process("Recording replays in database"):
                _, new_count = efficiently_record_replays(replays, format_id, batch_id)
                new_replay_count += new_count
            
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
        with time_process("Getting undownloaded replays for next task"):
            undownloaded_replays = get_undownloaded_replays(format_id)
            replay_ids_to_download = [r['replay_id'] for r in undownloaded_replays]
            ti.xcom_push(key='replay_ids_to_download', value=replay_ids_to_download)
    else:
        logger.info("No new replays found")
        # Skip the next task if no new replays
        raise AirflowSkipException("No new replays found to process")

def get_backfill_replay_ids(**context):
    """
    Airflow task to fetch and save replay IDs for a format as part of backfill process.
    Continues until it reaches the oldest previously processed ID or runs out of IDs.
    Records the fetched IDs directly in the metadata database using batch processing.
    
    This function specifically looks for replays OLDER than the oldest one we have recorded.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    max_pages = context['params'].get('max_pages', DEFAULT_MAX_PAGES)
    ignore_history = context['params'].get('ignore_history', False)
    
    # Enable detailed timing if requested
    enable_timing = context['params'].get('enable_detailed_timing', False)
    if enable_timing:
        enable_detailed_timing(True)
    
    logger.info(f"Backfill: Fetching older replay IDs for format: {format_id}, max pages: {max_pages}, ignore_history: {ignore_history}")
    
    # Get the oldest processed replay from the database to use as a starting point
    oldest_seen_ts = None
    
    with time_process("Getting oldest timestamp for backfill"):
        if not ignore_history:
            # Use the oldest uploadtime function to get the oldest timestamp
            oldest_ts = get_oldest_uploadtime(format_id)
            if oldest_ts:
                oldest_seen_ts = oldest_ts
                logger.info(f"Backfill: Oldest seen timestamp: {oldest_seen_ts}")
            else:
                logger.info("Backfill: No previously processed replays found in database")
        else:
            logger.info("Backfill: Ignoring history, will process all available replays")
    
    # Create a unique batch ID for this run to track related replays
    batch_id = f"backfill_{format_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    new_replay_count = 0
    page = 0
    total_replays_found = 0
    before_ts = oldest_seen_ts  # Start from the oldest timestamp we have
    done = False
    
    try:
        while page < max_pages and not done:
            logger.info(f"Backfill: Fetching page {page+1} for {format_id}")
            
            with time_process(f"Backfill: Fetching page {page+1} of replay IDs"):
                replays = fetch_replay_page(format_id, before_ts)
            
            if not replays:
                logger.info(f"Backfill: No replays found on page {page+1}")
                break
                
            total_replays_found += len(replays)
            logger.info(f"Backfill: Found {len(replays)} replays on page {page+1}")
            
            # Add a small delay between page fetches to avoid API rate limits
            if page > 0:
                time.sleep(0.1)
            
            # Use the efficient method to record replays in a single database transaction
            with time_process("Backfill: Recording replays in database"):
                _, new_count = efficiently_record_replays(replays, format_id, batch_id)
                new_replay_count += new_count
            
            # Prepare for next page if needed
            if done or len(replays) < 51:  # 51 indicates more pages
                logger.info(f"Backfill: Reached end of available replays (got {len(replays)} < 51)")
                break
                
            # Set 'before' to the uploadtime of the last replay in this page
            before_ts = replays[-1]["uploadtime"]
            page += 1
            
    except Exception as e:
        logger.error(f"Backfill: Error fetching replays: {e}")
        logger.error(traceback.format_exc())
    
    # Process summary and prepare for the next task
    if new_replay_count > 0:
        logger.info(f"Backfill: Added {new_replay_count} new replay IDs to the database")
        
        # Pass data to the next task
        ti.xcom_push(key='replay_count', value=new_replay_count)
        ti.xcom_push(key='batch_id', value=batch_id)
        
        # Build a list of replay IDs to pass to the download task directly
        with time_process("Backfill: Getting undownloaded replays for next task"):
            undownloaded_replays = get_undownloaded_replays(format_id)
            replay_ids_to_download = [r['replay_id'] for r in undownloaded_replays]
            ti.xcom_push(key='replay_ids_to_download', value=replay_ids_to_download)
    else:
        logger.info("Backfill: No new replays found")
        # Skip the next task if no new replays
        raise AirflowSkipException("No new replays found to process") 