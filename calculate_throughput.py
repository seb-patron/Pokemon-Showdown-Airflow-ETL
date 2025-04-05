#!/usr/bin/env python3
"""
Script to calculate the throughput of replay downloads from Airflow task logs.
This can be run after a DAG run to analyze the performance of parallel downloads.
"""

import re
import sys
import subprocess
from datetime import datetime
import argparse

def run_command(cmd):
    """Run a shell command and return the output"""
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"Error executing command: {cmd}")
        print(f"Error: {result.stderr}")
        return None
    return result.stdout.strip()

def parse_timestamp(timestamp_str):
    """Parse a timestamp string like [2025-04-05T01:22:10.812+0000] into a datetime object"""
    # Remove the brackets and timezone
    timestamp_str = timestamp_str.strip('[]')
    # Parse with microseconds
    try:
        return datetime.strptime(timestamp_str.split('+')[0], '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        # Try without microseconds
        return datetime.strptime(timestamp_str.split('+')[0], '%Y-%m-%dT%H:%M:%S')

def extract_log_info(log_path, container="airflow-scheduler"):
    """Extract relevant information from Airflow logs"""
    # Get start and end times
    timestamp_cmd = f"docker-compose exec {container} grep -E \"Task exited|Starting attempt\" \"{log_path}\""
    timestamps_output = run_command(timestamp_cmd)
    
    if not timestamps_output:
        print(f"Could not find start/end timestamps in log: {log_path}")
        return None
    
    # Extract timestamps
    start_time = None
    end_time = None
    for line in timestamps_output.splitlines():
        if "Starting attempt" in line:
            timestamp_str = line.split("{taskinstance.py")[0]
            start_time = parse_timestamp(timestamp_str)
        elif "Task exited" in line:
            timestamp_str = line.split("{local_task_job_runner")[0]
            end_time = parse_timestamp(timestamp_str)
    
    if not start_time or not end_time:
        print("Could not extract start or end timestamps")
        return None
    
    # Get download summary
    summary_cmd = f"docker-compose exec {container} grep \"Download summary:\" \"{log_path}\""
    summary_output = run_command(summary_cmd)
    
    if not summary_output:
        print(f"Could not find download summary in log: {log_path}")
        return None
    
    # Extract download counts
    match = re.search(r'(\d+) downloaded, (\d+) failed, (\d+) skipped out of (\d+) total', summary_output)
    if not match:
        print(f"Could not parse download summary: {summary_output}")
        return None
    
    downloaded = int(match.group(1))
    failed = int(match.group(2))
    skipped = int(match.group(3))
    total = int(match.group(4))
    
    # Extract parallel worker info
    workers_cmd = f"docker-compose exec {container} grep \"Using .* concurrent workers for downloads\" \"{log_path}\" | head -1"
    workers_output = run_command(workers_cmd)
    
    max_workers = None
    if workers_output:
        worker_match = re.search(r'Using (\d+) concurrent workers', workers_output)
        if worker_match:
            max_workers = int(worker_match.group(1))
    
    # Get mini-batch count
    batch_cmd = f"docker-compose exec {container} grep \"Mini-batch progress:\" \"{log_path}\" | wc -l"
    batch_output = run_command(batch_cmd)
    
    mini_batches = None
    if batch_output:
        mini_batches = int(batch_output.strip())
    
    return {
        'start_time': start_time,
        'end_time': end_time,
        'downloaded': downloaded,
        'failed': failed,
        'skipped': skipped,
        'total': total,
        'max_workers': max_workers,
        'mini_batches': mini_batches
    }

def calculate_metrics(info):
    """Calculate performance metrics from the extracted log information"""
    duration = (info['end_time'] - info['start_time']).total_seconds()
    
    return {
        'duration_seconds': duration,
        'duration_formatted': f"{int(duration // 60)} minutes, {int(duration % 60)} seconds",
        'throughput': info['downloaded'] / duration if duration > 0 else 0,
        'success_rate': (info['downloaded'] / info['total']) * 100 if info['total'] > 0 else 0,
        'average_time_per_download': duration / info['downloaded'] if info['downloaded'] > 0 else 0,
        'average_batch_time': duration / info['mini_batches'] if info['mini_batches'] > 0 else 0
    }

def format_results(info, metrics):
    """Format the results for display"""
    result = "\n" + "="*50 + "\n"
    result += "REPLAY DOWNLOAD PERFORMANCE METRICS\n"
    result += "="*50 + "\n\n"
    
    result += "TASK INFORMATION:\n"
    result += f"  Start Time: {info['start_time']}\n"
    result += f"  End Time: {info['end_time']}\n"
    result += f"  Duration: {metrics['duration_formatted']} ({metrics['duration_seconds']:.2f} seconds)\n"
    result += f"  Concurrent Workers: {info['max_workers']}\n"
    result += f"  Mini-Batches Processed: {info['mini_batches']}\n\n"
    
    result += "DOWNLOAD STATISTICS:\n"
    result += f"  Total Replays: {info['total']}\n"
    result += f"  Successfully Downloaded: {info['downloaded']} ({metrics['success_rate']:.2f}%)\n"
    result += f"  Failed: {info['failed']}\n"
    result += f"  Skipped: {info['skipped']}\n\n"
    
    result += "PERFORMANCE METRICS:\n"
    result += f"  Overall Throughput: {metrics['throughput']:.2f} replays/second\n"
    result += f"  Average Time per Replay: {metrics['average_time_per_download']*1000:.2f} ms\n"
    result += f"  Average Time per Batch: {metrics['average_batch_time']:.2f} seconds\n"
    
    result += "\n" + "="*50 + "\n"
    return result

def main():
    parser = argparse.ArgumentParser(description="Calculate download throughput from Airflow logs")
    parser.add_argument("dag_id", help="DAG ID (e.g., showdown_replay_backfill_etl)")
    parser.add_argument("run_id", help="Run ID (e.g., manual__2025-04-05T01:19:24+00:00)")
    parser.add_argument("--task", default="download_replays", help="Task ID (default: download_replays)")
    parser.add_argument("--attempt", default="1", help="Attempt number (default: 1)")
    parser.add_argument("--container", default="airflow-scheduler", help="Docker container name (default: airflow-scheduler)")
    parser.add_argument("--output", help="Output file (optional, default is stdout)")
    
    args = parser.parse_args()
    
    log_path = f"/opt/airflow/logs/dag_id={args.dag_id}/run_id={args.run_id}/task_id={args.task}/attempt={args.attempt}.log"
    
    # Extract information from logs
    info = extract_log_info(log_path, args.container)
    if not info:
        sys.exit(1)
    
    # Calculate metrics
    metrics = calculate_metrics(info)
    
    # Format and output results
    results = format_results(info, metrics)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(results)
        print(f"Results written to {args.output}")
    else:
        print(results)

if __name__ == "__main__":
    main() 