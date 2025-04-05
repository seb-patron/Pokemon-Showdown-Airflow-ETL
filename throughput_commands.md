# Throughput Measurement Commands


## 1. Get the start and end times for the download task:

```bash
docker-compose exec airflow-scheduler grep -E "Task exited|Starting attempt" "/opt/airflow/logs/dag_id=showdown_replay_backfill_etl/run_id=manual__2025-04-05T01:19:24+00:00/task_id=download_replays/attempt=1.log"
```

## 2. Get the total number of replays downloaded:

```bash
docker-compose exec airflow-scheduler grep "Download summary:" "/opt/airflow/logs/dag_id=showdown_replay_backfill_etl/run_id=manual__2025-04-05T01:19:24+00:00/task_id=download_replays/attempt=1.log"
```

## 3. Calculate throughput:

- Parse timestamps to extract the start and end time
- Calculate time difference in seconds
- Divide total number of downloaded replays by the time difference
