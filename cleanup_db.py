#!/usr/bin/env python3
"""
Script to clean up the SQLite database by removing backup tables and analyzing database size.
"""
import os
import sys
import sqlite3
import argparse
import logging
import re
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database path
DATA_DIR = os.path.join(os.getcwd(), 'data')
DB_PATH = os.path.join(DATA_DIR, 'replay_metadata.db')

def analyze_db():
    """
    Analyze the database size and structure.
    """
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    logger.info(f"Found {len(tables)} tables in database")
    
    # Get file size
    db_size_bytes = os.path.getsize(DB_PATH)
    db_size_mb = db_size_bytes / (1024 * 1024)
    logger.info(f"Database file size: {db_size_mb:.2f} MB")
    
    # Analyze table counts
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM '{table_name}'")
        count = cursor.fetchone()[0]
        logger.info(f"Table '{table_name}' has {count} rows")
    
    # Check for backup tables
    backup_tables = [t[0] for t in tables if t[0].startswith('backup_')]
    if backup_tables:
        logger.info(f"Found {len(backup_tables)} backup tables: {', '.join(backup_tables)}")
    
    # Check database integrity
    cursor.execute("PRAGMA integrity_check")
    integrity = cursor.fetchone()[0]
    if integrity == "ok":
        logger.info("Database integrity check: OK")
    else:
        logger.warning(f"Database integrity check failed: {integrity}")
    
    conn.close()

def cleanup_backup_tables(dry_run=True):
    """
    Remove backup tables from the database.
    
    Args:
        dry_run: If True, only show what would be deleted but don't actually delete
    """
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'backup_%'")
    backup_tables = cursor.fetchall()
    
    if not backup_tables:
        logger.info("No backup tables found")
        conn.close()
        return
    
    logger.info(f"Found {len(backup_tables)} backup tables to remove")
    
    for table in backup_tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM '{table_name}'")
        count = cursor.fetchone()[0]
        
        if dry_run:
            logger.info(f"Would drop table '{table_name}' with {count} rows")
        else:
            try:
                cursor.execute(f"DROP TABLE '{table_name}'")
                logger.info(f"Dropped table '{table_name}' with {count} rows")
            except sqlite3.Error as e:
                logger.error(f"Error dropping table '{table_name}': {e}")
    
    if not dry_run:
        conn.commit()
        # Vacuum to reclaim space
        cursor.execute("VACUUM")
        logger.info("Database vacuumed to reclaim space")
    
    conn.close()
    
    if dry_run:
        logger.info("This was a dry run. No tables were actually dropped.")
        logger.info("Run with --execute to actually perform the cleanup.")

def remove_duplicate_records(dry_run=True):
    """
    Find and remove duplicate records from the replay_status table.
    
    Args:
        dry_run: If True, only show duplicates but don't delete them
    """
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Find duplicate replay_ids
    cursor.execute("""
    SELECT replay_id, format_id, COUNT(*) as count
    FROM replay_status
    GROUP BY replay_id, format_id
    HAVING COUNT(*) > 1
    """)
    
    duplicates = cursor.fetchall()
    
    if not duplicates:
        logger.info("No duplicate records found in replay_status table")
        conn.close()
        return
    
    logger.info(f"Found {len(duplicates)} sets of duplicate records")
    
    if not dry_run:
        # Create a temporary table with unique records
        cursor.execute("""
        CREATE TEMPORARY TABLE temp_replay_status AS
        SELECT *
        FROM replay_status
        GROUP BY replay_id, format_id
        """)
        
        # Get the count of records in the original table
        cursor.execute("SELECT COUNT(*) FROM replay_status")
        original_count = cursor.fetchone()[0]
        
        # Get the count of records in the temporary table
        cursor.execute("SELECT COUNT(*) FROM temp_replay_status")
        unique_count = cursor.fetchone()[0]
        
        logger.info(f"Original table has {original_count} records, after deduplication: {unique_count}")
        
        # Replace the original table with the deduplicated one
        cursor.execute("DROP TABLE replay_status")
        cursor.execute("""
        CREATE TABLE replay_status AS
        SELECT * FROM temp_replay_status
        """)
        
        # Recreate indexes
        cursor.execute("CREATE INDEX idx_replay_format ON replay_status(format_id)")
        cursor.execute("CREATE INDEX idx_replay_uploadtime ON replay_status(uploadtime)")
        cursor.execute("CREATE INDEX idx_replay_downloaded ON replay_status(is_downloaded)")
        cursor.execute("CREATE INDEX idx_replay_compacted ON replay_status(is_compacted)")
        
        cursor.execute("DROP TABLE temp_replay_status")
        
        conn.commit()
        logger.info(f"Removed {original_count - unique_count} duplicate records")
        
        # Vacuum to reclaim space
        cursor.execute("VACUUM")
        logger.info("Database vacuumed to reclaim space")
    else:
        for dup in duplicates[:10]:  # Show only first 10
            logger.info(f"Would deduplicate: replay_id={dup[0]}, format_id={dup[1]}, count={dup[2]}")
        
        if len(duplicates) > 10:
            logger.info(f"... and {len(duplicates) - 10} more sets of duplicates")
        
        logger.info("This was a dry run. No records were actually deleted.")
        logger.info("Run with --execute to actually perform the deduplication.")
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Database cleanup and analysis tools')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze database size and structure')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Remove backup tables')
    cleanup_parser.add_argument('--execute', action='store_true', 
                               help='Actually perform the cleanup (default is dry run)')
    
    # Deduplicate command
    dedup_parser = subparsers.add_parser('deduplicate', help='Remove duplicate records')
    dedup_parser.add_argument('--execute', action='store_true',
                             help='Actually perform the deduplication (default is dry run)')
    
    args = parser.parse_args()
    
    if args.command == 'analyze':
        analyze_db()
    elif args.command == 'cleanup':
        cleanup_backup_tables(dry_run=not args.execute)
    elif args.command == 'deduplicate':
        remove_duplicate_records(dry_run=not args.execute)
    else:
        parser.print_help() 