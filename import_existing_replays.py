#!/usr/bin/env python3
"""
Script to import existing replay files that are present on disk 
but not recorded in the database.

This will scan the replay directories, find all JSON files, and add them
to the database if they're not already there, marking them as downloaded.
"""
import os
import sys
import json
import sqlite3
from datetime import datetime
import re

# Constants
REPLAYS_DIR = "/opt/airflow/data/replays"
DB_PATH = "/opt/airflow/data/replay_metadata.db"
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

def import_replay_files():
    """Import all replay files found in the replay directories"""
    format_dir = os.path.join(REPLAYS_DIR, FORMAT_ID)
    if not os.path.exists(format_dir):
        print(f"Error: Format directory {format_dir} does not exist")
        return
    
    # Get list of date directories
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
    
    # Stats
    total_files = 0
    imported = 0
    already_exists = 0
    errors = 0
    
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
                        replay_id, FORMAT_ID,
                        timestamp, BATCH_ID,
                        True, timestamp, BATCH_ID, f"Imported from file {file_path}",
                        False, None, None, None,
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
    print(f"Errors: {errors}")
    print(f"Batch ID: {BATCH_ID}")

if __name__ == "__main__":
    print("Starting import of existing replay files...")
    import_replay_files()
    print("Import completed") 