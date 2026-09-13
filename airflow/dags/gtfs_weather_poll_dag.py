"""
Stage 8 — weather polling DAG.

Separate DAG from gtfs_realtime_poll rather than folded into it: weather
and transit data have fundamentally different natural update cadences
(hourly-ish station observations vs. 1-minute GTFS-RT), and coupling
them would force one to compromise for the other. 15 minutes here is a
compromise between catching a new station observation reasonably
promptly and not hammering the NWS API for data that mostly hasn't
changed.

Reuses the same alert_on_failure callback and alerts.log as every other
DAG in this project -- one place to check regardless of which pipeline
had the problem.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting import alert_on_failure

# --- EDIT THESE TWO PATHS FOR YOUR MACHINE -----------------------------
PROJECT_DIR = "/mnt/c/Users/ohima/transit-project"
VENV_PYTHON = "/root/transit-venv/bin/python"
# -------------------------------------------------------------------------

default_args = {
    "owner": "transit-project",
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="gtfs_weather_poll",
    description="Poll NWS station observations for Austin (KAUS) every 15 minutes",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather"],
) as dag:

    poll_weather = BashOperator(
        task_id="poll_weather",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} poll_weather.py",
        execution_timeout=timedelta(seconds=30),
        retries=2,
        # NWS API hiccups are typically transient network issues, not lock
        # contention -- give it a bit more room than the DuckDB-writing
        # tasks elsewhere in this project.
        retry_delay=timedelta(minutes=2),
    )
