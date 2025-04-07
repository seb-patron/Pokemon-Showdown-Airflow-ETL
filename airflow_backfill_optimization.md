# Airflow Backfill Performance Optimization

## Current Status
As of April 6, 2025, the backfill job is successfully adding records to the SQLite database. We confirmed this by checking:
- Total records in database: 205,275
- Records pending download: 21,624

The backfill process is working but running slower than optimal. Below are the identified issues and suggestions for improvement.

## Identified Performance Issues

### 1. Sequential API Requests
- **Issue**: The job processes one page at a time, making a single API request and waiting for it to complete before moving to the next.
- **Impact**: High latency, underutilized network resources.

### 2. Fixed Delay Between Requests
- **Issue**: There's a hardcoded 0.5-second delay between page fetches (`time.sleep(0.5)`) regardless of actual API response time.
- **Impact**: Unnecessary waiting time between requests.

### 3. Database Bottlenecks
- **Issue**: For each page, the code queries the database to check if replays already exist, which takes several seconds per batch.
- **Impact**: Database operations becoming a major bottleneck.

### 4. Redundant Database Operations
- **Issue**: Separate database queries to check existence and record batches.
- **Impact**: Extra database roundtrips increasing processing time.

### 5. Batch Size Limitations
- **Issue**: Fixed batch size of 50 replays for database operations.
- **Impact**: Potentially suboptimal for specific database/hardware configuration.

### 6. Request Timeouts
- **Issue**: Conservative API request timeouts (5s connect, 30s read).
- **Impact**: Longer than necessary waits for slow responses.

### 7. SQLite Performance
- **Issue**: SQLite has limitations with concurrent operations compared to other databases.
- **Impact**: Database becomes a bottleneck during heavy concurrent access.

### 8. No Input Data Size Estimation
- **Issue**: No attempt to estimate total amount of data to be processed.
- **Impact**: Inability to predict completion time or adjust processing parameters.

## Optimization Suggestions

### 1. Implement Parallel API Requests
```python
# Example pseudocode for parallel API requests
with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_timestamp = {
        executor.submit(fetch_replay_page, format_id, timestamp): timestamp 
        for timestamp in timestamps_to_fetch
    }
    for future in as_completed(future_to_timestamp):
        process_results(future.result())
```

### 2. Implement Adaptive Rate Limiting
```python
# Example pseudocode for adaptive rate limiting
last_request_time = time.time()
min_request_interval = 0.1  # Minimum time between requests

def fetch_with_rate_limit():
    global last_request_time
    current_time = time.time()
    elapsed = current_time - last_request_time
    
    # Only sleep if we need to slow down
    if elapsed < min_request_interval:
        time.sleep(min_request_interval - elapsed)
    
    result = fetch_replay_page(format_id, before_ts)
    last_request_time = time.time()
    return result
```

### 3. Optimize Database Operations
- Add proper indexes:
```sql
CREATE INDEX IF NOT EXISTS idx_replay_id_format ON replay_status(replay_id, format_id);
CREATE INDEX IF NOT EXISTS idx_format_downloaded ON replay_status(format_id, is_downloaded);
```

- Use a connection pool:
```python
# Pseudocode for connection pooling
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine('sqlite:///data/replay_metadata.db', 
                     poolclass=QueuePool, 
                     pool_size=10, 
                     max_overflow=20)
```

### 4. Reduce Database Roundtrips
```python
# Example of combining existence check and insert
def efficiently_record_replays(replays, format_id, batch_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get IDs as a tuple for SQL IN clause
    replay_ids = tuple(r['id'] for r in replays)
    
    # Find existing IDs in one query
    cursor.execute(f"""
        SELECT replay_id FROM replay_status 
        WHERE replay_id IN ({','.join(['?']*len(replay_ids))}) 
        AND format_id = ?
    """, replay_ids + (format_id,))
    
    existing_ids = {row['replay_id'] for row in cursor.fetchall()}
    
    # Only insert non-existing replays
    new_replays = [r for r in replays if r['id'] not in existing_ids]
    
    # Bulk insert
    if new_replays:
        # Use executemany for bulk insert
        # ...
```

### 5. Optimize Batch Sizes
- Experiment with different batch sizes (25, 50, 100, 200) to find optimal performance
- Consider adaptive batch sizing:
```python
# Pseudocode for adaptive batch sizing
def determine_optimal_batch_size(recent_performance_metrics):
    if avg_db_operation_time > 5.0:
        return max(10, current_batch_size // 2)  # Reduce if too slow
    elif avg_db_operation_time < 1.0:
        return min(200, current_batch_size * 2)  # Increase if fast
    return current_batch_size  # Keep same if reasonable
```

### 6. Fix Database Connection Setup
- Investigate and fix the SQLiteHook connection issue:
```python
# Check if the connection string is properly formatted
# Instead of: "sqlite:///opt/airflow/data/replay_metadata.db"
# Use: "sqlite:////opt/airflow/data/replay_metadata.db" (note the 4 slashes)
```

### 7. Implement Progress Estimation
```python
# Example progress estimation code
def estimate_total_pages():
    # Make a request to get the latest replay timestamp
    latest_replays = fetch_replay_page(format_id, None)
    if not latest_replays:
        return None
    
    latest_ts = latest_replays[0]["uploadtime"]
    
    # Make a request with a very old timestamp
    oldest_possible_ts = 1420070400  # Jan 1, 2015
    oldest_replays = fetch_replay_page(format_id, oldest_possible_ts)
    if not oldest_replays or len(oldest_replays) < 51:
        return None
    
    oldest_ts = oldest_replays[-1]["uploadtime"]
    
    # Estimate total pages based on time difference and average items per page
    time_diff = latest_ts - oldest_ts
    avg_time_per_page = time_diff / 10  # Sample over 10 pages
    
    return int(time_diff / avg_time_per_page) + 1
```

### 8. Pagination Optimization
- Investigate if the API supports more efficient pagination methods:
```python
# Example of more efficient pagination if API supports it
def fetch_with_cursor_pagination():
    cursor = None
    while True:
        params = {"format": format_id, "limit": 100}
        if cursor:
            params["cursor"] = cursor
            
        response = requests.get(SEARCH_API_URL, params=params)
        data = response.json()
        
        yield data["results"]
        
        if not data.get("next_cursor"):
            break
            
        cursor = data["next_cursor"]
```

## Implementation Priority

1. **Parallel API Requests** - Highest impact for performance 
2. **Database Optimization** - Critical for handling larger datasets
3. **Reduce Database Roundtrips** - Quick win for performance improvement
4. **Fix Connection Setup** - Address warning messages and ensure optimal connection
5. **Adaptive Rate Limiting** - Better utilization of API limits
6. **Batch Size Optimization** - Fine-tuning after major improvements
7. **Progress Estimation** - Better monitoring and user experience
8. **Pagination Optimization** - If API supports more efficient methods

## Conclusion

The backfill job is successfully adding records to the database but could be significantly optimized. The primary bottlenecks are sequential API requests and database operations. Implementing parallel requests and optimizing database access would yield the most substantial performance improvements. 