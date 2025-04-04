"""
Database utilities for the Pokemon Replay ETL pipeline.

This module handles the metadata database connection and operations.
"""
import os
import sqlite3
import logging
import json
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Union

from airflow.hooks.sqlite_hook import SqliteHook
from showdown_replay_etl.constants import BASE_DIR

logger = logging.getLogger(__name__)

# Define the metadata database path
METADATA_DB_PATH = os.path.join(BASE_DIR, "replay_metadata.db")

def get_db_connection():
    """Get a connection to the metadata database using Airflow's connection management."""
    try:
        # Try to use Airflow's SqliteHook first
        hook = SqliteHook(sqlite_conn_id='replay_metadata_db')
        conn = hook.get_conn()
    except Exception as e:
        # Fallback to direct connection if hook fails
        logger.warning(f"Failed to use SqliteHook: {e}. Falling back to direct connection.")
        conn = sqlite3.connect(METADATA_DB_PATH)
    
    # Set row factory to return dictionaries
    conn.row_factory = sqlite3.Row
    return conn

def initialize_db():
    """Create the metadata database and tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create the replay_status table with separate columns for each stage
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS replay_status (
        replay_id TEXT PRIMARY KEY,
        format_id TEXT NOT NULL,
        
        discovered_at TIMESTAMP,
        discovered_batch TEXT,
        
        is_downloaded BOOLEAN DEFAULT FALSE,
        downloaded_at TIMESTAMP,
        downloaded_batch TEXT,
        download_details TEXT,
        
        is_compacted BOOLEAN DEFAULT FALSE,
        compacted_at TIMESTAMP,
        compacted_batch TEXT,
        compacted_details TEXT,
        
        is_retry_attempted BOOLEAN DEFAULT FALSE,
        retry_at TIMESTAMP,
        retry_batch TEXT,
        retry_details TEXT,
        
        uploadtime INTEGER NOT NULL,
        players TEXT,
        additional_info TEXT
    )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_replay_format ON replay_status(format_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_replay_uploadtime ON replay_status(uploadtime)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_replay_downloaded ON replay_status(is_downloaded)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_replay_compacted ON replay_status(is_compacted)')
    
    # Create a temporary migration tracking table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS db_migration (
        id INTEGER PRIMARY KEY,
        version INTEGER NOT NULL,
        migrated_at TIMESTAMP NOT NULL,
        description TEXT
    )
    ''')
    
    conn.commit()
    conn.close()
    
    logger.info(f"Initialized metadata database at {METADATA_DB_PATH}")
    
    # Run migrations if needed
    run_migrations()

def run_migrations():
    """Run any necessary database migrations."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check current migration version
    cursor.execute('SELECT MAX(version) as current_version FROM db_migration')
    result = cursor.fetchone()
    current_version = result['current_version'] if result and result['current_version'] is not None else 0
    
    try:
        # If we're on version 0 (initial state), run migration to version 1
        if current_version < 1:
            logger.info("Running migration to version 1: Converting replay_processing/replay_metadata to replay_status")
            
            # Check if old tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='replay_processing'")
            has_old_tables = cursor.fetchone() is not None
            
            if has_old_tables:
                # Migrate data from old tables to new structure
                logger.info("Migrating data from old tables...")
                
                # First, get all unique replay IDs with metadata
                cursor.execute('''
                SELECT rm.replay_id, rm.format_id, rm.uploadtime, rm.players, rm.discovered_at, 
                       rm.batch_id as discovered_batch, rm.additional_info
                FROM replay_metadata rm
                ''')
                
                replay_metadata = cursor.fetchall()
                
                for metadata in replay_metadata:
                    replay_id = metadata['replay_id']
                    format_id = metadata['format_id']
                    
                    # Check downloaded status
                    cursor.execute('''
                    SELECT * FROM replay_processing 
                    WHERE replay_id = ? AND stage = 'downloaded' AND status = 'success'
                    ORDER BY processed_at DESC LIMIT 1
                    ''', (replay_id,))
                    downloaded = cursor.fetchone()
                    
                    # Check compacted status
                    cursor.execute('''
                    SELECT * FROM replay_processing 
                    WHERE replay_id = ? AND stage = 'compacted' AND status = 'success'
                    ORDER BY processed_at DESC LIMIT 1
                    ''', (replay_id,))
                    compacted = cursor.fetchone()
                    
                    # Check retry status
                    cursor.execute('''
                    SELECT * FROM replay_processing 
                    WHERE replay_id = ? AND stage = 'retry' AND status = 'success'
                    ORDER BY processed_at DESC LIMIT 1
                    ''', (replay_id,))
                    retry = cursor.fetchone()
                    
                    # Insert into new table
                    cursor.execute('''
                    INSERT OR REPLACE INTO replay_status (
                        replay_id, format_id, 
                        discovered_at, discovered_batch,
                        is_downloaded, downloaded_at, downloaded_batch, download_details,
                        is_compacted, compacted_at, compacted_batch, compacted_details,
                        is_retry_attempted, retry_at, retry_batch, retry_details,
                        uploadtime, players, additional_info
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        replay_id, format_id,
                        metadata['discovered_at'], metadata['discovered_batch'],
                        bool(downloaded), 
                        downloaded['processed_at'] if downloaded else None,
                        downloaded['details'].split('(batch ')[1].split(')')[0] if downloaded and '(batch ' in downloaded['details'] else None,
                        downloaded['details'] if downloaded else None,
                        bool(compacted),
                        compacted['processed_at'] if compacted else None,
                        compacted['details'].split('(batch ')[1].split(')')[0] if compacted and '(batch ' in compacted['details'] else None,
                        compacted['details'] if compacted else None,
                        bool(retry),
                        retry['processed_at'] if retry else None,
                        retry['details'].split('(batch ')[1].split(')')[0] if retry and '(batch ' in retry['details'] else None,
                        retry['details'] if retry else None,
                        metadata['uploadtime'],
                        metadata['players'],
                        metadata['additional_info']
                    ))
                
                logger.info(f"Migrated {len(replay_metadata)} replay records to new table structure")
                
                # Backup old tables with a timestamp prefix
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                cursor.execute(f"ALTER TABLE replay_processing RENAME TO backup_{timestamp}_replay_processing")
                cursor.execute(f"ALTER TABLE replay_metadata RENAME TO backup_{timestamp}_replay_metadata")
                logger.info(f"Backed up old tables with prefix backup_{timestamp}_")
            
            # Record the migration
            cursor.execute('''
            INSERT INTO db_migration (version, migrated_at, description)
            VALUES (?, ?, ?)
            ''', (1, datetime.now().isoformat(), "Convert replay_processing/replay_metadata to replay_status"))
            
            conn.commit()
            logger.info("Migration to version 1 completed successfully")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during database migration: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
    finally:
        conn.close()

def record_replay_discovery(replay_id: str, format_id: str, uploadtime: int, players: Optional[str] = None, 
                            additional_info: Optional[Dict[str, Any]] = None, batch_id: Optional[str] = None):
    """
    Record the discovery of a replay.
    
    Args:
        replay_id: The ID of the replay
        format_id: The format ID
        uploadtime: The upload timestamp of the replay
        players: String representation of the players (e.g. "player1 vs player2")
        additional_info: Dictionary of additional metadata
        batch_id: Optional batch identifier
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now().isoformat()
    additional_info_json = json.dumps(additional_info) if additional_info else None
    
    # Use INSERT OR REPLACE to handle potential duplicates
    cursor.execute('''
    INSERT OR REPLACE INTO replay_status (
        replay_id, format_id, 
        discovered_at, discovered_batch,
        uploadtime, players, additional_info
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (replay_id, format_id, now, batch_id, uploadtime, players, additional_info_json))
    
    conn.commit()
    conn.close()

def batch_record_replay_discoveries(replays: List[Dict[str, Any]], format_id: str, batch_id: Optional[str] = None):
    """
    Record multiple replay discoveries in a single database transaction for better efficiency.
    
    Args:
        replays: List of dictionaries, each containing replay information
                Each dictionary must have 'id' and 'uploadtime' keys, and optionally 'players' and other metadata
        format_id: The format ID that applies to all replays in this batch
        batch_id: Optional batch identifier
    """
    if not replays:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now().isoformat()
    
    try:
        # Start a transaction
        cursor.execute("BEGIN TRANSACTION")
        
        for replay in replays:
            replay_id = replay['id']
            uploadtime = replay['uploadtime']
            players = replay.get('players')
            
            # Extract additional info and remove known fields
            additional_info = {k: v for k, v in replay.items() if k not in ('id', 'uploadtime', 'players', 'format')}
            additional_info_json = json.dumps(additional_info) if additional_info else None
            
            # Use INSERT OR REPLACE to handle potential duplicates
            cursor.execute('''
            INSERT OR REPLACE INTO replay_status (
                replay_id, format_id, 
                discovered_at, discovered_batch,
                uploadtime, players, additional_info
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (replay_id, format_id, now, batch_id, uploadtime, players, additional_info_json))
        
        # Commit the transaction
        conn.commit()
        logger.info(f"Successfully recorded {len(replays)} replays in batch")
        
    except Exception as e:
        # Roll back the transaction if there's an error
        conn.rollback()
        logger.error(f"Error in batch insert: {e}")
        logger.error(traceback.format_exc())
        
    finally:
        conn.close()

def get_replay_metadata(replay_id: str) -> Optional[Dict[str, Any]]:
    """
    Get metadata for a replay.
    
    Args:
        replay_id: The ID of the replay
    
    Returns:
        Dictionary with replay metadata or None if not found
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM replay_status
    WHERE replay_id = ?
    ''', (replay_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        metadata = dict(result)
        if metadata.get('additional_info'):
            try:
                metadata['additional_info'] = json.loads(metadata['additional_info'])
            except json.JSONDecodeError:
                pass
        return metadata
    return None

def mark_replay_downloaded(replay_id: str, format_id: str, details: Optional[str] = None, batch_id: Optional[str] = None):
    """
    Mark a replay as successfully downloaded.
    
    Args:
        replay_id: The ID of the replay
        format_id: The format ID
        details: Optional details about the download
        batch_id: Optional batch identifier
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    timestamp = datetime.now().isoformat()
    
    cursor.execute('''
    UPDATE replay_status
    SET is_downloaded = TRUE,
        downloaded_at = ?,
        downloaded_batch = ?,
        download_details = ?
    WHERE replay_id = ? AND format_id = ?
    ''', (timestamp, batch_id, details, replay_id, format_id))
    
    conn.commit()
    conn.close()

def mark_download_failed(replay_id: str, format_id: str, error_details: str, batch_id: Optional[str] = None):
    """
    Record a failed download attempt for a replay.
    
    Args:
        replay_id: The ID of the replay
        format_id: The format ID
        error_details: Error message or details
        batch_id: Optional batch identifier
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    timestamp = datetime.now().isoformat()
    full_details = f"Failed: {error_details} (batch {batch_id})" if batch_id else f"Failed: {error_details}"
    
    cursor.execute('''
    UPDATE replay_status
    SET downloaded_at = ?,
        downloaded_batch = ?,
        download_details = ?
    WHERE replay_id = ? AND format_id = ?
    ''', (timestamp, batch_id, full_details, replay_id, format_id))
    
    conn.commit()
    conn.close()

def mark_replay_compacted(replay_id: str, format_id: str, details: Optional[str] = None, batch_id: Optional[str] = None):
    """
    Mark a replay as successfully compacted.
    
    Args:
        replay_id: The ID of the replay
        format_id: The format ID
        details: Optional details about the compaction
        batch_id: Optional batch identifier
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    timestamp = datetime.now().isoformat()
    
    cursor.execute('''
    UPDATE replay_status
    SET is_compacted = TRUE,
        compacted_at = ?,
        compacted_batch = ?,
        compacted_details = ?
    WHERE replay_id = ? AND format_id = ?
    ''', (timestamp, batch_id, details, replay_id, format_id))
    
    conn.commit()
    conn.close()

def mark_retry_attempt(replay_id: str, format_id: str, success: bool, details: Optional[str] = None, batch_id: Optional[str] = None):
    """
    Record a retry attempt for a replay.
    
    Args:
        replay_id: The ID of the replay
        format_id: The format ID
        success: Whether the retry was successful
        details: Optional details about the retry
        batch_id: Optional batch identifier
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    timestamp = datetime.now().isoformat()
    
    update_fields = [
        "is_retry_attempted = TRUE",
        "retry_at = ?",
        "retry_batch = ?",
        "retry_details = ?"
    ]
    
    params = [timestamp, batch_id, details, replay_id, format_id]
    
    # If the retry was successful, also mark as downloaded
    if success:
        update_fields.append("is_downloaded = TRUE")
    
    query = f"UPDATE replay_status SET {', '.join(update_fields)} WHERE replay_id = ? AND format_id = ?"
    
    cursor.execute(query, params)
    
    conn.commit()
    conn.close()

def is_replay_downloaded(replay_id: str, format_id: str) -> bool:
    """
    Check if a replay has already been downloaded.
    
    Args:
        replay_id: The ID of the replay to check
        format_id: The format ID
        
    Returns:
        True if the replay has been successfully downloaded, False otherwise
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT is_downloaded
    FROM replay_status
    WHERE replay_id = ? AND format_id = ?
    ''', (replay_id, format_id))
    
    result = cursor.fetchone()
    conn.close()
    
    return result and result['is_downloaded']

def is_replay_compacted(replay_id: str, format_id: str) -> bool:
    """
    Check if a replay has already been compacted.
    
    Args:
        replay_id: The ID of the replay to check
        format_id: The format ID
        
    Returns:
        True if the replay has been successfully compacted, False otherwise
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT is_compacted
    FROM replay_status
    WHERE replay_id = ? AND format_id = ?
    ''', (replay_id, format_id))
    
    result = cursor.fetchone()
    conn.close()
    
    return result and result['is_compacted']

def get_undownloaded_replays(format_id: str) -> List[Dict[str, Any]]:
    """
    Get replays that haven't been downloaded yet.
    
    Args:
        format_id: The format ID to check
        
    Returns:
        A list of dictionaries with replay info that haven't been downloaded
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT replay_id, format_id, uploadtime, players, additional_info
    FROM replay_status
    WHERE format_id = ? AND is_downloaded = FALSE
    ''', (format_id,))
    
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    for row in results:
        if row.get('additional_info'):
            try:
                row['additional_info'] = json.loads(row['additional_info'])
            except:
                pass
    
    logger.info(f"Found {len(results)} replays that need to be downloaded for format {format_id}")
    return results

def get_downloaded_uncompacted_replays(format_id: str) -> List[str]:
    """
    Get replay IDs that have been downloaded but not compacted.
    
    Args:
        format_id: The format ID to check
        
    Returns:
        A list of replay IDs that have been downloaded but not compacted
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT replay_id
    FROM replay_status
    WHERE format_id = ? AND is_downloaded = TRUE AND is_compacted = FALSE
    ''', (format_id,))
    
    results = [row['replay_id'] for row in cursor.fetchall()]
    conn.close()
    
    logger.info(f"Found {len(results)} replays that have been downloaded but not compacted for format {format_id}")
    return results

def get_failed_downloads(format_id: str) -> List[str]:
    """
    Get replay IDs with failed download attempts.
    
    Args:
        format_id: The format ID to check
        
    Returns:
        A list of replay IDs where downloads failed and no retry has been attempted
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT replay_id
    FROM replay_status
    WHERE format_id = ? 
    AND is_downloaded = FALSE 
    AND download_details LIKE 'Failed:%'
    AND (is_retry_attempted = FALSE OR is_retry_attempted IS NULL)
    ''', (format_id,))
    
    results = [row['replay_id'] for row in cursor.fetchall()]
    conn.close()
    
    logger.info(f"Found {len(results)} replays with failed downloads that need retry for format {format_id}")
    return results

def get_latest_uploadtime(format_id: str) -> Optional[int]:
    """
    Get the most recent uploadtime for a specific format.
    
    Args:
        format_id: The format ID to check
        
    Returns:
        The most recent uploadtime or None if no replays found
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT MAX(uploadtime) as latest_ts
    FROM replay_status
    WHERE format_id = ?
    ''', (format_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result and result['latest_ts'] is not None:
        return result['latest_ts']
    return None

def get_stats_by_format(format_id: str) -> Dict[str, int]:
    """
    Get processing statistics for a format.
    
    Args:
        format_id: The format ID to check
        
    Returns:
        A dictionary with counts for different processing stages
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN is_downloaded = TRUE THEN 1 ELSE 0 END) as downloaded,
        SUM(CASE WHEN is_compacted = TRUE THEN 1 ELSE 0 END) as compacted,
        SUM(CASE WHEN is_retry_attempted = TRUE THEN 1 ELSE 0 END) as retried,
        SUM(CASE WHEN download_details LIKE 'Failed:%' THEN 1 ELSE 0 END) as failed_downloads
    FROM replay_status
    WHERE format_id = ?
    ''', (format_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return dict(result)
    return {
        "total": 0,
        "downloaded": 0,
        "compacted": 0,
        "retried": 0,
        "failed_downloads": 0
    }

def get_replays_by_date(format_id: str) -> Dict[str, List[str]]:
    """
    Group replay IDs by their upload date.
    
    Args:
        format_id: The format ID to check
        
    Returns:
        A dictionary with dates as keys and lists of replay IDs as values
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all downloaded replays
    cursor.execute('''
    SELECT replay_id, uploadtime
    FROM replay_status
    WHERE format_id = ? AND is_downloaded = TRUE
    ''', (format_id,))
    
    results = cursor.fetchall()
    conn.close()
    
    # Group by date
    replays_by_date = {}
    for row in results:
        replay_id = row['replay_id']
        upload_time = datetime.fromtimestamp(row['uploadtime'])
        date_str = upload_time.strftime("%Y-%m-%d")
        
        if date_str not in replays_by_date:
            replays_by_date[date_str] = []
        replays_by_date[date_str].append(replay_id)
    
    return replays_by_date

def check_replays_existence(replay_ids: List[str], format_id: str) -> Dict[str, bool]:
    """
    Check if multiple replay IDs already exist and are downloaded in the database.
    This is more efficient than individual checks for each replay.
    
    Args:
        replay_ids: List of replay IDs to check
        format_id: The format ID to check against
        
    Returns:
        A dictionary mapping replay IDs to boolean values (True if downloaded, False if not)
    """
    if not replay_ids:
        return {}
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Use placeholders for the query
    placeholders = ','.join(['?'] * len(replay_ids))
    
    # Check which replays are already downloaded
    cursor.execute(f'''
    SELECT replay_id, is_downloaded 
    FROM replay_status
    WHERE replay_id IN ({placeholders}) AND format_id = ?
    ''', replay_ids + [format_id])
    
    results = {row['replay_id']: bool(row['is_downloaded']) for row in cursor.fetchall()}
    conn.close()
    
    # For any replay_id not in the results, it doesn't exist in the database
    return {replay_id: results.get(replay_id, False) for replay_id in replay_ids}

def batch_mark_replays_downloaded(replay_info_list: List[Dict[str, Any]], format_id: str, batch_id: Optional[str] = None):
    """
    Mark multiple replays as downloaded in a single database transaction for better efficiency.
    
    Args:
        replay_info_list: List of dictionaries containing replay information
                         Each dictionary must have 'replay_id' and 'details' keys
        format_id: The format ID that applies to all replays in this batch
        batch_id: Optional batch identifier
    """
    if not replay_info_list:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now().isoformat()
    
    try:
        # Start a transaction
        cursor.execute("BEGIN TRANSACTION")
        
        for info in replay_info_list:
            replay_id = info['replay_id']
            details = info.get('details', 'Downloaded in batch operation')
            
            cursor.execute('''
            UPDATE replay_status
            SET is_downloaded = TRUE,
                downloaded_at = ?,
                downloaded_batch = ?,
                download_details = ?
            WHERE replay_id = ? AND format_id = ?
            ''', (now, batch_id, details, replay_id, format_id))
        
        # Commit the transaction
        conn.commit()
        logger.info(f"Successfully marked {len(replay_info_list)} replays as downloaded")
        
    except Exception as e:
        # Roll back the transaction if there's an error
        conn.rollback()
        logger.error(f"Error in batch update: {e}")
        logger.error(traceback.format_exc())
        
    finally:
        conn.close()

def batch_mark_replays_failed(replay_info_list: List[Dict[str, Any]], format_id: str, batch_id: Optional[str] = None):
    """
    Mark multiple replays as having failed download in a single database transaction.
    
    Args:
        replay_info_list: List of dictionaries containing replay information
                         Each dictionary must have 'replay_id' and 'error' keys
        format_id: The format ID that applies to all replays in this batch
        batch_id: Optional batch identifier
    """
    if not replay_info_list:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now().isoformat()
    
    try:
        # Start a transaction
        cursor.execute("BEGIN TRANSACTION")
        
        for info in replay_info_list:
            replay_id = info['replay_id']
            error_details = info.get('error', 'Unknown error during batch operation')
            
            cursor.execute('''
            UPDATE replay_status
            SET is_downloaded = FALSE,
                downloaded_at = ?,
                downloaded_batch = ?,
                download_details = ?
            WHERE replay_id = ? AND format_id = ?
            ''', (now, batch_id, f"ERROR: {error_details}", replay_id, format_id))
        
        # Commit the transaction
        conn.commit()
        logger.info(f"Successfully marked {len(replay_info_list)} replays as failed download")
        
    except Exception as e:
        # Roll back the transaction if there's an error
        conn.rollback()
        logger.error(f"Error in batch update for failed replays: {e}")
        logger.error(traceback.format_exc())
        
    finally:
        conn.close()

# Initialize the database when the module is imported
initialize_db() 