"""
DAG for extracting, transforming, and loading Pokemon Showdown replays.

This DAG:
1. Fetches replay IDs for a specified format
2. Downloads replay data for each ID
3. Retries any failed downloads
4. Compacts daily replays into single files for easier analysis
"""
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from showdown_replay_etl.constants import DEFAULT_FORMAT, DEFAULT_MAX_PAGES
from showdown_replay_etl.tasks.discovery import get_replay_ids
from showdown_replay_etl.tasks.download import download_replays
from showdown_replay_etl.tasks.retry import retry_failed_replays
from showdown_replay_etl.tasks.compaction import compact_daily_replays

# Define default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
with DAG(
    'showdown_replay_etl',
    default_args=default_args,
    description='ETL process for Pokemon Showdown replays',
    schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    params={
        'format_id': DEFAULT_FORMAT,
        'max_pages': DEFAULT_MAX_PAGES,
        'ignore_history': False,  # Set to True to force processing all replays for testing
    },
) as dag:
    
    # Task 1: Fetch replay IDs
    get_replay_ids_task = PythonOperator(
        task_id='get_replay_ids',
        python_callable=get_replay_ids,
        provide_context=True,
    )
    
    # Task 2: Download replays
    download_replays_task = PythonOperator(
        task_id='download_replays',
        python_callable=download_replays,
        provide_context=True,
    )
    
    # Task 3: Retry failed replays
    retry_failed_replays_task = PythonOperator(
        task_id='retry_failed_replays',
        python_callable=retry_failed_replays,
        provide_context=True,
    )
    
    # Task 4: Compact daily replays
    compact_daily_replays_task = PythonOperator(
        task_id='compact_daily_replays',
        python_callable=compact_daily_replays,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    # Define task dependencies
    get_replay_ids_task >> download_replays_task >> retry_failed_replays_task >> compact_daily_replays_task 