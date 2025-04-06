#!/usr/bin/env python3
"""
Script to fix the discrepancy between compacted files and the database.
This script finds replays that are in the compacted files but not marked as compacted
in the database, and updates their status.
"""
import os
import sys
import json
import sqlite3
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = os.path.join(os.getcwd(), 'data')
REPLAYS_DIR = os.path.join(DATA_DIR, 'replays')
COMPACTED_REPLAYS_DIR = os.path.join(DATA_DIR, 'compacted_replays')
DB_PATH = os.path.join(DATA_DIR, 'replay_metadata.db')
DEFAULT_FORMAT = "gen9vgc2024regh"

def get_db_connection():
    """Get a connection to the SQLite database with row factory enabled"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_uncompacted_replays_by_date(format_id: str) -> Dict[str, Set[str]]:
    """
    Get all replays that are downloaded but not compacted, organized by date.
    
    Args:
        format_id: Format ID to get replays for
        
    Returns:
        Dictionary with dates as keys and sets of replay IDs as values
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT replay_id, date(datetime(uploadtime, 'unixepoch')) as upload_date
    FROM replay_status
    WHERE format_id = ? AND is_downloaded = 1 AND is_compacted = 0
    ''', (format_id,))
    
    results = {}
    for row in cursor.fetchall():
        date_str = row['upload_date']
        replay_id = row['replay_id']
        
        if date_str not in results:
            results[date_str] = set()
        results[date_str].add(replay_id)
    
    conn.close()
    
    # Count total replays
    total = sum(len(ids) for ids in results.values())
    logger.info(f"Found {total} downloaded but uncompacted replays across {len(results)} dates")
    
    return results

def get_compacted_replay_ids(format_id: str, date_str: str) -> Set[str]:
    """
    Get all replay IDs from a compacted file for a specific date.
    
    Args:
        format_id: Format ID
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        Set of replay IDs found in the compacted file
    """
    # Path to the compacted file
    compacted_file = os.path.join(COMPACTED_REPLAYS_DIR, format_id, f"{date_str}.json")
    
    if not os.path.exists(compacted_file):
        logger.warning(f"No compacted file found for {date_str}")
        return set()
    
    try:
        with open(compacted_file, 'r') as f:
            compacted_data = json.load(f)
        
        # Extract replay IDs
        replay_ids = {replay.get('id') for replay in compacted_data if 'id' in replay}
        logger.info(f"Found {len(replay_ids)} replays in compacted file for {date_str}")
        return replay_ids
    except Exception as e:
        logger.error(f"Error reading compacted file for {date_str}: {e}")
        return set()

def mark_replays_as_compacted(replay_ids: List[str], format_id: str, date_str: str, dry_run: bool = True) -> int:
    """
    Mark a list of replays as compacted in the database.
    
    Args:
        replay_ids: List of replay IDs to mark as compacted
        format_id: Format ID
        date_str: Date string used for reference
        dry_run: If True, show what would be updated but don't actually update
        
    Returns:
        Number of replays marked as compacted
    """
    if not replay_ids:
        return 0
    
    if dry_run:
        logger.info(f"Would mark {len(replay_ids)} replays as compacted for {date_str}")
        return len(replay_ids)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    batch_id = f"fix_compacted_{format_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Prepare batch update
        batch_data = []
        compacted_file = os.path.join(COMPACTED_REPLAYS_DIR, format_id, f"{date_str}.json")
        
        for replay_id in replay_ids:
            batch_data.append((
                timestamp,
                batch_id,
                f"Fixed: Compacted to {compacted_file}",
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
        logger.info(f"Successfully marked {len(batch_data)} replays as compacted for {date_str}")
        return len(batch_data)
    except Exception as e:
        logger.error(f"Error marking replays as compacted: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()

def fix_compacted_status(format_id: str, date: Optional[str] = None, dry_run: bool = True):
    """
    Fix the compacted status for replays that are in compacted files but not marked in the database.
    
    Args:
        format_id: Format ID to process
        date: Optional specific date to process (YYYY-MM-DD)
        dry_run: If True, show what would be updated but don't actually update
    """
    # Get all uncompacted replays by date
    uncompacted_by_date = get_uncompacted_replays_by_date(format_id)
    
    # If a specific date is provided, only process that date
    if date:
        if date not in uncompacted_by_date:
            logger.info(f"No uncompacted replays found for {date}")
            return
        dates_to_process = [date]
    else:
        dates_to_process = sorted(uncompacted_by_date.keys())
    
    total_fixed = 0
    for date_str in dates_to_process:
        # Get uncompacted replays for this date
        uncompacted_ids = uncompacted_by_date.get(date_str, set())
        if not uncompacted_ids:
            logger.info(f"No uncompacted replays for {date_str}")
            continue
        
        logger.info(f"Processing {len(uncompacted_ids)} uncompacted replays for {date_str}")
        
        # Get the IDs of replays in the compacted file for this date
        compacted_ids = get_compacted_replay_ids(format_id, date_str)
        if not compacted_ids:
            logger.warning(f"No compacted file or no replay IDs found for {date_str}")
            continue
        
        # Find the intersection - replays that are in the compacted file but not marked in DB
        to_fix = list(set(uncompacted_ids).intersection(compacted_ids))
        
        if not to_fix:
            logger.info(f"No discrepancies found for {date_str}")
            continue
        
        logger.info(f"Found {len(to_fix)} replays that need fixing for {date_str}")
        
        # Mark these replays as compacted
        fixed = mark_replays_as_compacted(to_fix, format_id, date_str, dry_run)
        total_fixed += fixed
    
    if dry_run:
        logger.info(f"Dry run complete. Would fix {total_fixed} replays across {len(dates_to_process)} dates")
    else:
        logger.info(f"Fixed {total_fixed} replays across {len(dates_to_process)} dates")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix compacted status discrepancies')
    parser.add_argument('--format', default=DEFAULT_FORMAT, help=f'Format ID to process (default: {DEFAULT_FORMAT})')
    parser.add_argument('--date', help='Specific date to process (YYYY-MM-DD format)')
    parser.add_argument('--execute', action='store_true', help='Actually perform the updates (default is dry run)')
    
    args = parser.parse_args()
    
    logger.info(f"Starting fix_compacted_status for format {args.format}")
    if args.date:
        logger.info(f"Processing specific date: {args.date}")
    if args.execute:
        logger.info("EXECUTE mode: Will perform actual database updates")
    else:
        logger.info("DRY RUN mode: No database changes will be made")
    
    fix_compacted_status(args.format, args.date, not args.execute) 