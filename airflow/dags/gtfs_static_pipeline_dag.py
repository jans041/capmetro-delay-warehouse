"""
Stage 6 — daily static pipeline.
Stage 7 — alerting callback + per-task retry tuning.

Chain: ingest_gtfs_static.py -> dbt snapshot -> dbt run -> dbt test

dbt snapshot runs right after ingestion (not after dbt run) so that
SCD2 change-detection on routes_snapshot always sees the freshest
raw_routes data before any downstream models touch it — this is the
piece that was previously run manually to close the loop on Stage 4.

Retry design (Stage 7): this project has two independent writers
hitting the same warehouse.duckdb file (this DAG's dbt tasks, and the
separate realtime DAGs), and DuckDB is single-writer — so "database is
locked" transient failures are a near-certain real outcome, not a
hypothetical one. DuckDB-writing tasks (dbt_snapshot, dbt_run) get
short retry delays, since a lock is likely to clear within seconds.
The network-facing task (ingest_gtfs_static) gets a longer delay,
suited to transient network/upstream-feed issues rather than lock
contention. dbt_test gets only 1 retry with a short delay: a real test
failure is a data-quality problem, not a transient one, so retrying
more just delays the alert without fixing anything.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting import alert_on_failure

# Project root is two levels up from airflow/dags/. Venv location can be
# overridden with TRANSIT_VENV for machines that keep it elsewhere.
PROJECT_DIR = Path(__file__).resolve().parents[2]
VENV_DIR = os.environ.get("TRANSIT_VENV", "/root/transit-venv")
VENV_PYTHON = f"{VENV_DIR}/bin/python"
VENV_DBT = f"{VENV_DIR}/bin/dbt"

# Base default_args: every task gets the failure callback. Retries are
# overridden per-task below, since a network download and a DuckDB write
# fail for different reasons and should retry differently.
default_args = {
    "owner": "transit-project",
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="gtfs_static_pipeline",
    description="Daily GTFS static ingestion + dbt build",
    default_args=default_args,
    schedule_interval="0 8 * * *",  # 08:00 UTC = 02:00/03:00 America/Chicago
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["gtfs", "static", "dbt"],
) as dag:

    ingest_static = BashOperator(
        task_id="ingest_gtfs_static",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} ingest_gtfs_static.py",
        retries=3,
        retry_delay=timedelta(minutes=5),  # network/upstream feed issue — give it time
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {PROJECT_DIR} && {VENV_DBT} snapshot --profiles-dir .",
        retries=3,
        retry_delay=timedelta(seconds=15),  # DuckDB lock — clears fast
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR} && {VENV_DBT} run --profiles-dir .",
        retries=3,
        retry_delay=timedelta(seconds=15),  # DuckDB lock — clears fast
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR} && {VENV_DBT} test --profiles-dir .",
        retries=1,
        # Only 1 retry: a real test failure is a data-quality problem, not a
        # transient one — retrying more just delays the alert without fixing
        # anything.
        retry_delay=timedelta(seconds=15),
    )

    ingest_static >> dbt_snapshot >> dbt_run >> dbt_test
