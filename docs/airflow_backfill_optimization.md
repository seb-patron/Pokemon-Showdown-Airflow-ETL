# Airflow Backfill Performance Optimization

## Current Status
As of April 6, 2025, the backfill job is successfully adding records to the SQLite database with optimized database operations. We confirmed this by checking:
- Total records in database: 205,275
- Records pending download: 21,624

The following optimizations have already been implemented:
- ✅ Efficient database operations using bulk queries
- ✅ Reduction of database roundtrips by combining existence checks and inserts
- ✅ Optimized batch database operations with proper transaction management
- ✅ Improved filtering of already processed items

## Remaining Performance Issues

### 1. Sequential API Requests
- **Issue**: The job processes one page at a time, making a single API request and waiting for it to complete before moving to the next.
- **Impact**: High latency, underutilized network resources.

### 2. Fixed Delay Between Requests
- **Issue**: There's a hardcoded 0.1-second delay between page fetches (`time.sleep(0.1)`) regardless of actual API response time.
- **Impact**: Unnecessary waiting time between requests.

### 3. Request Timeouts
- **Issue**: Conservative API request timeouts (5s connect, 30s read).
- **Impact**: Longer than necessary waits for slow responses.

### 4. SQLite Performance Limitations
- **Issue**: SQLite has limitations with concurrent operations compared to other databases.
- **Impact**: Database becomes a bottleneck during heavy concurrent access.

### 5. No Input Data Size Estimation
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

### 3. Optimize SQLite Configuration
- Use WAL mode for better concurrency:
```python
def optimize_sqlite_connection():
    conn = get_db_connection()
    conn.execute('PRAGMA journal_mode = WAL')
    conn.execute('PRAGMA synchronous = NORMAL')
    conn.execute('PRAGMA cache_size = 10000')
    conn.execute('PRAGMA temp_store = MEMORY')
    conn.commit()
    conn.close()
```

### 4. Implement Progress Estimation
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

### 5. Pagination Optimization
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
2. **Adaptive Rate Limiting** - Better utilization of API limits
3. **Progress Estimation** - Better monitoring and user experience
4. **SQLite Configuration Optimization** - Fine-tuning database performance
5. **Pagination Optimization** - If API supports more efficient methods

## Conclusion

The backfill job has been significantly optimized for database operations, but the primary remaining bottleneck is sequential API requests. Implementing parallel requests would yield the most substantial performance improvement going forward. The adaptive rate limiting would then further optimize API usage while maintaining good relations with the service provider. 