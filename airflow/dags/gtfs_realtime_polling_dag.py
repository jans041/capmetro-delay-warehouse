"""
Stage 6 — realtime polling.
Stage 7 — alerting callback, per-task retry tuning, and late-arriving-
fact detection (dbt test + warning routing on gtfs_realtime_dbt_build).

Known tradeoff, worth a line in the postmortem: the original manual
cadence for poll_gtfs_realtime.py was 30-60s. Airflow's scheduler isn't
built for sub-minute cron intervals -- 1 minute is the practical floor
for a DAG-per-run design like this one. So this DAG polls every 1
minute instead of every 30-60s. For a true 30-60s cadence you'd run
poll_gtfs_realtime.py as a long-lived loop/service (e.g. under
systemd or a simple `while true; do ...; sleep 30; done`) outside
Airflow entirely, and let Airflow own only the slower-moving pieces
(this file's second DAG). Kept as Airflow-managed here for a single
consistent orchestration story; call out the tradeoff explicitly if
asked about it.

Two DAGs in this file:
  gtfs_realtime_poll     - every 1 min, appends raw vehicle/trip-update rows
  gtfs_realtime_dbt_build - every 5 min: rebuilds fct_realtime_delays,
                            runs its tests (including the late-arriving-
                            rate check), then checks run_results.json for
                            any warn-severity result and routes it into
                            alerts.log.

Retry design (Stage 7): same reasoning as the static DAG. poll_gtfs_realtime
gets a short retry delay because the next scheduled run is only a minute
away anyway -- a long delay would just get skipped. The dbt tasks write to
warehouse.duckdb, so they get a short delay tuned for lock contention
clearing (this project has two independent writers hitting the same
DuckDB file, and DuckDB is single-writer).
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from alerting import alert_on_failure, alert_on_dbt_test_warning

# Project root is two levels up from airflow/dags/. Venv location can be
# overridden with TRANSIT_VENV for machines that keep it elsewhere.
PROJECT_DIR = Path(__file__).resolve().parents[2]
VENV_DIR = os.environ.get("TRANSIT_VENV", "/root/transit-venv")
VENV_PYTHON = f"{VENV_DIR}/bin/python"
VENV_DBT = f"{VENV_DIR}/bin/dbt"

default_args = {
    "owner": "transit-project",
    "on_failure_callback": alert_on_failure,
}


def check_dbt_test_warnings(**context):
    """
    dbt exits 0 for a severity=warn test result by design -- warns
    shouldn't block a run. That also means they never reach
    on_failure_callback on their own. This task explicitly reads
    target/run_results.json right after `dbt test` and routes any warn
    result into the same alerts.log, so a rising late-arriving rate is
    visible in one place instead of only living in dbt's own logs.
    """
    run_results_path = f"{PROJECT_DIR}/target/run_results.json"
    with open(run_results_path) as f:
        run_results = json.load(f)

    for result in run_results.get("results", []):
        if result.get("status") == "warn":
            test_name = result.get("unique_id", "unknown_test")
            message = (result.get("message") or "dbt test warned").strip()
            alert_on_dbt_test_warning(
                dag_id=context["dag"].dag_id,
                task_id="check_dbt_test_warnings",
                run_id=context["run_id"],
                test_name=test_name,
                message=message,
            )


with DAG(
    dag_id="gtfs_realtime_poll",
    description="Poll GTFS-RT vehicle positions + trip updates every minute",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,  # don't let a slow poll overlap the next scheduled one
    tags=["gtfs", "realtime"],
) as poll_dag:

    poll_realtime = BashOperator(
        task_id="poll_gtfs_realtime",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} poll_gtfs_realtime.py",
        execution_timeout=timedelta(seconds=50),  # kill before the next run is due
        retries=1,
        # Short delay — the next scheduled run is only a minute away, so a
        # long retry delay would just get skipped anyway.
        retry_delay=timedelta(seconds=15),
    )

with DAG(
    dag_id="gtfs_realtime_dbt_build",
    description="Rebuild fct_realtime_delays, test it, and surface any warnings",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "realtime", "dbt"],
) as build_dag:

    rebuild_delays = BashOperator(
        task_id="dbt_run_realtime_delays",
        bash_command=(
            f"cd {PROJECT_DIR} && {VENV_DBT} run --profiles-dir . "
            f"--select fct_realtime_delays"
        ),
        retries=3,
        retry_delay=timedelta(seconds=15),  # DuckDB lock — clears fast
    )

    test_delays = BashOperator(
        task_id="dbt_test_realtime_delays",
        bash_command=(
            f"cd {PROJECT_DIR} && {VENV_DBT} test --profiles-dir . "
            f"--select fct_realtime_delays"
        ),
        retries=1,
        retry_delay=timedelta(seconds=15),
    )

    check_warnings = PythonOperator(
        task_id="check_dbt_test_warnings",
        python_callable=check_dbt_test_warnings,
    )

    rebuild_delays >> test_delays >> check_warnings