import sys
import os

# Add the current directory to the Python path so we can import our modules
sys.path.append('/opt/airflow')

# Import our database module
from dags.showdown_replay_etl.db import initialize_db, get_db_connection

print("Initializing database...")
initialize_db()

# Verify tables after initialization
conn = get_db_connection()
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row['name'] for row in cursor.fetchall()]
print(f"Tables after initialization: {', '.join(tables)}")

# Check if migration was successful
cursor.execute("SELECT * FROM db_migration ORDER BY version DESC LIMIT 1")
migration = cursor.fetchone()
if migration:
    print(f"Latest migration: version {migration['version']} - {migration['description']}")
    print(f"Applied at: {migration['migrated_at']}")
else:
    print("No migrations found in the database")

# Check replay_status table if it exists
if 'replay_status' in tables:
    cursor.execute("SELECT COUNT(*) as count FROM replay_status")
    count = cursor.fetchone()['count']
    print(f"Found {count} records in replay_status table")
    
    # Sample a few records to verify structure
    cursor.execute("""
    SELECT replay_id, format_id, is_downloaded, is_compacted 
    FROM replay_status LIMIT 5
    """)
    sample = cursor.fetchall()
    print("\nSample records from replay_status:")
    for row in sample:
        print(f"  {row['replay_id']} - Downloaded: {row['is_downloaded']}, Compacted: {row['is_compacted']}")

conn.close()
print("\nDatabase initialization complete!") 