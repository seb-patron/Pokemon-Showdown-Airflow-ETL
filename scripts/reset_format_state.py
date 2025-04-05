#!/usr/bin/env python3
"""
Command-line utility to reset the state for a Pokémon Showdown format.
This allows the ETL process to fetch all replays from the beginning.

Usage:
    python reset_format_state.py <format_id>
    
Example:
    python reset_format_state.py gen9vgc2024regh
"""
import sys
import os
import json
from pathlib import Path
import sqlite3
import argparse
import shutil
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def reset_format_state(format_id):
    """Reset the state for a specific format."""
    # Determine the base directory
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    
    # Create the state file path
    state_file = os.path.join(base_dir, f"{format_id}_state.json")
    
    # Create a new state with default values
    new_state = {
        "last_seen_ts": 0,
        "oldest_ts": None,
        "last_processed_id": None,
        "format_id": format_id
    }
    
    # Save the state
    with open(state_file, 'w') as f:
        json.dump(new_state, f, indent=2)
    
    print(f"State for format '{format_id}' has been reset.")
    print(f"The next ETL run will fetch all replays from the beginning.")

def vacuum_database():
    """
    Repair and optimize the SQLite database to fix corruption and reduce file size.
    Creates a backup before attempting repairs.
    """
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at {DB_PATH}")
        return False
    
    # Create a backup before attempting any repairs
    backup_path = f"{DB_PATH}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"Creating backup of database at {backup_path}")
    shutil.copy2(DB_PATH, backup_path)
    
    logger.info("Attempting to repair and optimize database...")
    
    try:
        # First try to copy data to a new database
        new_db_path = f"{DB_PATH}.new"
        old_conn = sqlite3.connect(DB_PATH)
        old_conn.row_factory = sqlite3.Row
        
        # Create new database
        if os.path.exists(new_db_path):
            os.remove(new_db_path)
        
        new_conn = sqlite3.connect(new_db_path)
        
        # Copy schema
        for sql in old_conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND type='table'").fetchall():
            new_conn.execute(sql[0])
        
        # Copy indexes 
        for sql in old_conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND type='index'").fetchall():
            if sql[0] is not None:  # Skip NULL entries
                try:
                    new_conn.execute(sql[0])
                except sqlite3.OperationalError:
                    logger.warning(f"Skipping problematic index: {sql[0]}")
        
        # Copy replay_status data
        logger.info("Copying replay_status data to new database...")
        old_cursor = old_conn.cursor()
        new_cursor = new_conn.cursor()
        
        # Get column names for replay_status table
        old_cursor.execute("PRAGMA table_info(replay_status)")
        columns = [row[1] for row in old_cursor.fetchall()]
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        
        # Fetch data in batches to avoid loading everything into memory
        batch_size = 1000
        offset = 0
        
        while True:
            old_cursor.execute(f"SELECT {columns_str} FROM replay_status LIMIT {batch_size} OFFSET {offset}")
            rows = old_cursor.fetchall()
            if not rows:
                break
                
            # Insert batch into new database
            new_cursor.executemany(f"INSERT INTO replay_status ({columns_str}) VALUES ({placeholders})", rows)
            new_conn.commit()
            
            offset += batch_size
            logger.info(f"Copied {offset} rows so far...")
        
        # Copy db_migration data
        logger.info("Copying db_migration data...")
        old_cursor.execute("SELECT * FROM db_migration")
        for row in old_cursor.fetchall():
            new_cursor.execute("INSERT INTO db_migration VALUES (?, ?, ?, ?)", tuple(row))
        
        # Close connections
        old_conn.close()
        
        # Vacuum the new database to optimize space
        logger.info("Vacuuming new database...")
        new_conn.execute("VACUUM")
        new_conn.commit()
        new_conn.close()
        
        # Replace old DB with new DB
        logger.info("Replacing old database with repaired version...")
        os.remove(DB_PATH)
        os.rename(new_db_path, DB_PATH)
        
        logger.info("Database repair and optimization complete!")
        return True
        
    except Exception as e:
        logger.error(f"Error repairing database: {str(e)}")
        logger.info(f"You can restore from backup at {backup_path}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Manage Showdown replay data state.')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Reset format command
    reset_parser = subparsers.add_parser('reset', help='Reset state for a specific format')
    reset_parser.add_argument('format_id', help='Format ID to reset (e.g., gen9randombattle)')
    
    # Vacuum database command
    vacuum_parser = subparsers.add_parser('vacuum', help='Repair and optimize the database')
    
    args = parser.parse_args()
    
    if args.command == 'reset':
        reset_format_state(args.format_id)
    elif args.command == 'vacuum':
        vacuum_database()
    else:
        parser.print_help() 