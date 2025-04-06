#!/usr/bin/env python3
"""
Script to import existing replay files that are present on disk 
but not recorded in the database.

This will scan the replay directories, find all JSON files, and add them
to the database if they're not already there, marking them as downloaded.
It can also check compacted files and mark replays as compacted if they exist there.
"""
import os
import sys
import json
import sqlite3
from datetime import datetime
import re
import argparse
import glob

# Constants - will be overridden with command line args
REPLAYS_DIR = os.path.join(os.getcwd(), "data/replays")
COMPACTED_REPLAYS_DIR = os.path.join(os.getcwd(), "data/compacted_replays")
DB_PATH = os.path.join(os.getcwd(), "data/replay_metadata.db")
FORMAT_ID = "gen9vgc2024regh"
BATCH_ID = f"import_existing_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def get_db_connection():
    """Get a connection to the SQLite database"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_replay_id_from_filename(filename):
    """Extract replay ID from filename"""
    return os.path.splitext(os.path.basename(filename))[0]

def extract_upload_time_from_file(file_path):
    """Extract uploadtime from replay file content"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            if 'uploadtime' in data:
                return data['uploadtime']
            else:
                # If uploadtime not found, derive from the timestamp in the battle log
                if 'log' in data:
                    # Look for |t:|TIMESTAMP pattern in the log
                    match = re.search(r'\|t:\|(\d+)', data['log'])
                    if match:
                        return int(match.group(1))
    except Exception as e:
        print(f"Error extracting uploadtime from {file_path}: {e}")
    
    # If we couldn't extract a timestamp, use the file modification time
    return int(os.path.getmtime(file_path))

def extract_players_from_file(file_path):
    """Extract player info from replay file content"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            if 'players' in data and isinstance(data['players'], list):
                return " vs ".join(data['players'])
            elif 'p1' in data and 'p2' in data:
                return f"{data['p1']} vs {data['p2']}"
    except Exception as e:
        print(f"Error extracting players from {file_path}: {e}")
    
    return "Unknown vs Unknown"

def get_replay_ids_from_compacted_file(file_path):
    """Get a set of replay IDs from a compacted file"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            replay_ids = set()
            for replay in data:
                if 'id' in replay:
                    replay_ids.add(replay['id'])
            return replay_ids
    except Exception as e:
        print(f"Error reading compacted file {file_path}: {e}")
        return set()

def find_compacted_replay_ids(format_id):
    """Find all replay IDs that are in compacted files"""
    format_dir = os.path.join(COMPACTED_REPLAYS_DIR, format_id)
    if not os.path.exists(format_dir):
        print(f"Compacted directory {format_dir} does not exist or is not accessible")
        return set()
    
    compacted_ids = set()
    compacted_files = glob.glob(os.path.join(format_dir, "*.json"))
    print(f"Found {len(compacted_files)} compacted files")
    
    for file_path in compacted_files:
        file_ids = get_replay_ids_from_compacted_file(file_path)
        compacted_ids.update(file_ids)
        print(f"  Added {len(file_ids)} IDs from {os.path.basename(file_path)}")
    
    print(f"Total of {len(compacted_ids)} unique replay IDs found in compacted files")
    return compacted_ids

def import_replay_files(format_id=FORMAT_ID, specific_date=None, check_compacted=True):
    """Import all replay files found in the replay directories"""
    format_dir = os.path.join(REPLAYS_DIR, format_id)
    if not os.path.exists(format_dir):
        print(f"Error: Format directory {format_dir} does not exist")
        return
    
    # Get list of date directories
    if specific_date:
        # Only process the specified date
        date_dirs = [specific_date] if os.path.isdir(os.path.join(format_dir, specific_date)) else []
        if not date_dirs:
            print(f"Error: Date directory {specific_date} does not exist in {format_dir}")
            return
    else:
        # Process all date directories
        date_dirs = [d for d in os.listdir(format_dir) 
                    if os.path.isdir(os.path.join(format_dir, d))]
    
    if not date_dirs:
        print("No date directories found")
        return
    
    print(f"Found {len(date_dirs)} date directories")
    
    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get existing replay IDs in the database
    cursor.execute("SELECT replay_id FROM replay_status")
    existing_ids = {row['replay_id'] for row in cursor.fetchall()}
    print(f"Found {len(existing_ids)} existing replay IDs in database")
    
    # Get compacted replay IDs if requested
    compacted_ids = find_compacted_replay_ids(format_id) if check_compacted else set()
    
    # Stats
    total_files = 0
    imported = 0
    already_exists = 0
    errors = 0
    marked_compacted = 0
    
    timestamp = datetime.now().isoformat()
    
    # Process each date directory
    for date_dir_name in date_dirs:
        date_dir = os.path.join(format_dir, date_dir_name)
        print(f"Processing directory: {date_dir_name}")
        
        # Get list of JSON files
        try:
            files = [f for f in os.listdir(date_dir) if f.endswith('.json')]
        except Exception as e:
            print(f"Error listing files in {date_dir}: {e}")
            continue
        
        print(f"  Found {len(files)} files")
        total_files += len(files)
        
        # Process files in batches to avoid memory issues
        batch_size = 100
        for i in range(0, len(files), batch_size):
            batch = files[i:i+batch_size]
            
            for filename in batch:
                try:
                    file_path = os.path.join(date_dir, filename)
                    replay_id = get_replay_id_from_filename(filename)
                    
                    # Skip if already in database
                    if replay_id in existing_ids:
                        already_exists += 1
                        continue
                    
                    # Extract data from file
                    uploadtime = extract_upload_time_from_file(file_path)
                    players = extract_players_from_file(file_path)
                    
                    # Check if this replay is in a compacted file
                    is_compacted = replay_id in compacted_ids
                    compacted_details = f"Found in compacted file for {date_dir_name}" if is_compacted else None
                    
                    if is_compacted:
                        marked_compacted += 1
                    
                    # Add to database, mark as discovered and downloaded
                    additional_info = json.dumps({"imported": True, "date_dir": date_dir_name})
                    
                    cursor.execute('''
                    INSERT INTO replay_status (
                        replay_id, format_id,
                        discovered_at, discovered_batch,
                        is_downloaded, downloaded_at, downloaded_batch, download_details,
                        is_compacted, compacted_at, compacted_batch, compacted_details,
                        uploadtime, players, additional_info
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        replay_id, format_id,
                        timestamp, BATCH_ID,
                        True, timestamp, BATCH_ID, f"Imported from file {file_path}",
                        is_compacted, timestamp if is_compacted else None, BATCH_ID if is_compacted else None, compacted_details,
                        uploadtime, players, additional_info
                    ))
                    
                    imported += 1
                except Exception as e:
                    print(f"  Error processing {filename}: {e}")
                    errors += 1
            
            # Commit after each batch
            conn.commit()
            print(f"  Processed {i+len(batch)}/{len(files)} files")
    
    # Final commit and close connection
    conn.commit()
    conn.close()
    
    print("\nImport Summary:")
    print(f"Total files found: {total_files}")
    print(f"Files already in database: {already_exists}")
    print(f"New files imported: {imported}")
    print(f"Files marked as compacted: {marked_compacted}")
    print(f"Errors: {errors}")
    print(f"Batch ID: {BATCH_ID}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import existing replay files into the database')
    parser.add_argument('--format', default=FORMAT_ID, help=f'Format ID to process (default: {FORMAT_ID})')
    parser.add_argument('--date', help='Specific date to process (YYYY-MM-DD format)')
    parser.add_argument('--no-check-compacted', action='store_true', help='Skip checking for compacted files')
    parser.add_argument('--db-path', default=DB_PATH, help=f'Path to the database (default: {DB_PATH})')
    parser.add_argument('--replays-dir', default=REPLAYS_DIR, help=f'Path to the replays directory (default: {REPLAYS_DIR})')
    parser.add_argument('--compacted-dir', default=COMPACTED_REPLAYS_DIR, help=f'Path to the compacted replays directory (default: {COMPACTED_REPLAYS_DIR})')
    
    args = parser.parse_args()
    
    # Update global variables with command line arguments
    FORMAT_ID = args.format
    DB_PATH = args.db_path
    REPLAYS_DIR = args.replays_dir
    COMPACTED_REPLAYS_DIR = args.compacted_dir
    
    print("Starting import of existing replay files...")
    print(f"Format: {FORMAT_ID}")
    print(f"Database: {DB_PATH}")
    print(f"Replays directory: {REPLAYS_DIR}")
    print(f"Compacted replays directory: {COMPACTED_REPLAYS_DIR}")
    if args.date:
        print(f"Processing only date: {args.date}")
    
    import_replay_files(FORMAT_ID, args.date, not args.no_check_compacted)
    print("Import completed") 