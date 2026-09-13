"""
Stage 7 — feed-staleness detection.

Why this is a separate DAG rather than an extra task tacked onto
gtfs_realtime_poll: a task can succeed (clean HTTP 200, no exception)
while the *feed itself* has gone stale upstream -- CapMetro returning
an unchanged or empty response without erroring. That failure mode is
invisible to Airflow's own task-success bookkeeping, since nothing
actually threw. Checking the data itself, on its own schedule,
independent of whether poll_gtfs_realtime.py reported success, is the
only way to catch it.

Threshold: 5 minutes, chosen against the 1-minute poll cadence -- long
enough to absorb a couple of missed/retried polls without false-
alarming, short enough to catch a genuinely stalled feed quickly.

Reuses alert_on_failure from alerting.py: raising a descriptive
exception here lands in the same alerts.log as every other Stage 7
failure, rather than inventing a second alerting path.
"""
from datetime import datetime, timedelta, timezone

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator

from alerting import alert_on_failure

# --- EDIT THIS PATH FOR YOUR MACHINE -------------------------------------
WAREHOUSE_PATH = "/mnt/c/Users/ohima/transit-project/data/warehouse.duckdb"
# -------------------------------------------------------------------------

STALENESS_THRESHOLD_MINUTES = 5

default_args = {
    "owner": "transit-project",
    "on_failure_callback": alert_on_failure,
    "retries": 0,  # a stale feed doesn't get less stale by retrying immediately
}


def check_feed_staleness(**context):
    # read_only=True: this DAG must never contend for the write lock that
    # gtfs_realtime_poll and the dbt tasks are already fighting over.
    con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    try:
        vp_latest = con.execute(
            "SELECT MAX(polled_at) FROM raw_vehicle_positions"
        ).fetchone()[0]
        tu_latest = con.execute(
            "SELECT MAX(polled_at) FROM raw_trip_updates"
        ).fetchone()[0]
    finally:
        con.close()

    now = datetime.now(timezone.utc)
    threshold = timedelta(minutes=STALENESS_THRESHOLD_MINUTES)

    stale_sources = []
    parsed = {}
    for label, latest in (("raw_vehicle_positions", vp_latest), ("raw_trip_updates", tu_latest)):
        if latest is None:
            stale_sources.append(f"{label}: no rows at all")
            continue
        # polled_at is stored as VARCHAR in these raw tables, not a native
        # DuckDB TIMESTAMP, so MAX() returns a plain str -- parse it before
        # doing any datetime arithmetic on it.
        if isinstance(latest, str):
            latest = datetime.fromisoformat(latest)
        # Normalize to UTC-aware before subtracting, in case the parsed
        # value (or a native datetime DuckDB returned) is naive.
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        parsed[label] = latest  # save the parsed version for the summary print below
        age = now - latest
        if age > threshold:
            stale_sources.append(f"{label}: last row {age} ago")

    if stale_sources:
        raise RuntimeError(
            f"GTFS-RT feed appears stale (threshold {STALENESS_THRESHOLD_MINUTES}m): "
            + "; ".join(stale_sources)
        )

    print(
        f"Feed fresh: vehicle_positions {now - parsed['raw_vehicle_positions']} ago, "
        f"trip_updates {now - parsed['raw_trip_updates']} ago"
    )


with DAG(
    dag_id="gtfs_feed_staleness_check",
    description="Alert if the GTFS-RT feed hasn't produced new rows recently",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "realtime", "alerting"],
) as dag:

    staleness_check = PythonOperator(
        task_id="check_feed_staleness",
        python_callable=check_feed_staleness,
    )