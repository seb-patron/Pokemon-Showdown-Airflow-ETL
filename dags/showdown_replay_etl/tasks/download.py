"""
Tasks related to downloading replay data from Showdown API.
"""
import logging
import os
import json
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

from airflow.models import TaskInstance
from airflow.exceptions import AirflowSkipException

from showdown_replay_etl.constants import DEFAULT_FORMAT, REPLAYS_DIR
from showdown_replay_etl.api import fetch_replay_data
from showdown_replay_etl.db import (
    get_undownloaded_replays, is_replay_downloaded, get_replay_metadata,
    check_replays_existence, batch_mark_replays_downloaded, batch_mark_replays_failed
)

logger = logging.getLogger(__name__)

def download_replay(replay_id: str, format_id: str, metadata: Dict[str, Any], ignore_history: bool = False) -> Dict[str, Any]:
    """
    Download a single replay and save it to the appropriate directory.
    
    Args:
        replay_id: The ID of the replay to download
        format_id: The format ID of the replay
        metadata: The metadata for the replay
        ignore_history: Whether to ignore history checks
        
    Returns:
        A dictionary with information about the download result
    """
    if not metadata:
        return {
            'replay_id': replay_id,
            'success': False,
            'error': 'No metadata found for replay'
        }
        
    uploadtime = metadata['uploadtime']
        
    # Check if already processed (may have been processed in a previous failed run)
    if not ignore_history and is_replay_downloaded(replay_id, format_id):
        logger.info(f"Replay {replay_id} already downloaded, skipping")
        return {
            'replay_id': replay_id,
            'success': True,
            'skipped': True,
            'details': 'Already downloaded'
        }
        
    logger.info(f"Downloading replay: {replay_id}")
    
    try:
        # Fetch the replay data - returns a tuple of (data, error)
        replay_data, error_msg = fetch_replay_data(replay_id)
        
        if not replay_data:
            error_details = error_msg or "Failed to download replay data (reason unknown)"
            logger.error(f"Failed to download replay {replay_id}: {error_details}")
            return {
                'replay_id': replay_id,
                'success': False,
                'error': error_details
            }
        
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
        
        return {
            'replay_id': replay_id,
            'success': True,
            'details': f"Downloaded to {replay_file}"
        }
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error processing replay {replay_id}: {error_msg}")
        logger.error(traceback.format_exc())
        return {
            'replay_id': replay_id,
            'success': False,
            'error': error_msg
        }

def download_replays(**context):
    """
    Airflow task to download replay files for the discovered replay IDs.
    Skips replays that are already downloaded.
    
    Uses parallel processing for better performance.
    """
    ti: TaskInstance = context['ti']
    format_id = context['params'].get('format_id', DEFAULT_FORMAT)
    batch_id = ti.xcom_pull(key='batch_id')
    max_workers = context['params'].get('download_workers', 5)
    ignore_history = context['params'].get('ignore_history', False)
    
    # Try to get replay IDs passed from the previous task
    replay_ids_to_download = ti.xcom_pull(key='replay_ids_to_download')
    
    if not replay_ids_to_download:
        logger.info("No replay IDs provided, fetching from database")
        undownloaded_replays = get_undownloaded_replays(format_id)
        replay_ids_to_download = [r['replay_id'] for r in undownloaded_replays]
    
    total_replays = len(replay_ids_to_download)
    logger.info(f"Found {total_replays} replays to download for format {format_id}")
    
    if total_replays == 0:
        logger.info("No replays to download")
        raise AirflowSkipException("No replays to download")
    
    # First, do a bulk check of existence to avoid redundant database lookups during download
    logger.info(f"Performing bulk existence check for {total_replays} replays")
    existence_map = check_replays_existence(replay_ids_to_download, format_id)
    
    # Filter to only download replays that don't exist (if not ignoring history)
    if not ignore_history:
        filtered_replay_ids = [r_id for r_id, is_downloaded in existence_map.items() 
                              if not is_downloaded]
        
        if len(filtered_replay_ids) < total_replays:
            logger.info(f"Filtered out {total_replays - len(filtered_replay_ids)} already downloaded replays")
            replay_ids_to_download = filtered_replay_ids
            total_replays = len(replay_ids_to_download)
    
    # Prepare directories
    os.makedirs(REPLAYS_DIR, exist_ok=True)
    format_dir = os.path.join(REPLAYS_DIR, format_id)
    os.makedirs(format_dir, exist_ok=True)
    
    # Setting up progress tracking
    successful_downloads = []
    failed_downloads = []
    
    # Load metadata for all replays at once to avoid redundant DB queries
    metadata_map = {}
    for replay_id in replay_ids_to_download:
        metadata = get_replay_metadata(replay_id)
        if metadata:
            metadata_map[replay_id] = metadata
        else:
            logger.warning(f"No metadata found for replay {replay_id}")
    
    # Process replays in parallel
    if max_workers > 1:
        logger.info(f"Using {max_workers} workers for parallel downloading")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            future_to_replay = {
                executor.submit(download_replay, replay_id, format_id, metadata_map.get(replay_id, {}), ignore_history): replay_id
                for replay_id in replay_ids_to_download
                if replay_id in metadata_map  # Skip replays with missing metadata
            }
            
            # Process results as they complete
            for i, future in enumerate(as_completed(future_to_replay)):
                replay_id = future_to_replay[future]
                try:
                    result = future.result()
                    if result['success']:
                        successful_downloads.append({
                            'replay_id': replay_id,
                            'details': result.get('message', 'Successfully downloaded')
                        })
                    else:
                        failed_downloads.append({
                            'replay_id': replay_id,
                            'details': result.get('error', 'Unknown error')
                        })
                except Exception as e:
                    logger.error(f"Error processing replay {replay_id}: {e}")
                    failed_downloads.append({
                        'replay_id': replay_id,
                        'details': f"Failed: {str(e)}"
                    })
                
                # Log progress periodically
                if (i + 1) % 10 == 0 or (i + 1) == total_replays:
                    logger.info(f"Progress: {i + 1}/{total_replays} replays processed")
    else:
        # Sequential processing
        logger.info("Using sequential processing for downloading")
        for i, replay_id in enumerate(replay_ids_to_download):
            if replay_id not in metadata_map:
                logger.warning(f"Skipping replay {replay_id} due to missing metadata")
                continue
                
            try:
                result = download_replay(replay_id, format_id, metadata_map.get(replay_id, {}), ignore_history)
                if result['success']:
                    successful_downloads.append({
                        'replay_id': replay_id,
                        'details': result.get('message', 'Successfully downloaded')
                    })
                else:
                    failed_downloads.append({
                        'replay_id': replay_id,
                        'details': result.get('error', 'Unknown error')
                    })
            except Exception as e:
                logger.error(f"Error processing replay {replay_id}: {e}")
                failed_downloads.append({
                    'replay_id': replay_id,
                    'details': f"Failed: {str(e)}"
                })
            
            # Log progress periodically
            if (i + 1) % 10 == 0 or (i + 1) == total_replays:
                logger.info(f"Progress: {i + 1}/{total_replays} replays processed")
    
    # Update the database with batch operations
    if successful_downloads:
        logger.info(f"Marking {len(successful_downloads)} successful downloads in bulk")
        batch_mark_replays_downloaded(successful_downloads, format_id, batch_id)
    
    if failed_downloads:
        logger.info(f"Marking {len(failed_downloads)} failed downloads in bulk")
        batch_mark_replays_failed(failed_downloads, format_id, batch_id)
    
    # Summary
    logger.info(f"Download summary: {len(successful_downloads)} successful, {len(failed_downloads)} failed")
    
    # Pass results to the next task
    ti.xcom_push(key='successful_downloads', value=len(successful_downloads))
    ti.xcom_push(key='failed_downloads', value=len(failed_downloads))
    
    if len(successful_downloads) > 0:
        return {
            'successful': len(successful_downloads),
            'failed': len(failed_downloads),
            'total': total_replays
        }
    else:
        logger.warning("No replays were successfully downloaded")
        raise AirflowSkipException("No replays were successfully downloaded") 