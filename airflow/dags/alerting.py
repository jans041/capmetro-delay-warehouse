"""
Stage 7 — shared failure-alerting callback for all DAGs.

Deliberately simple: writes to a local JSON-lines log file rather than
email/Slack. This makes it testable without real credentials, and the
log itself becomes a natural input for a "pipeline health" panel in
the Stage 8 dashboard later.

Two entry points now:
  alert_on_failure         - Airflow on_failure_callback, for actual
                              task failures (unchanged from the original).
  alert_on_dbt_test_warning - called explicitly by a task that parses
                              dbt's run_results.json, for severity=warn
                              dbt test results. dbt warns exit 0 by
                              design (they shouldn't block a run), so
                              they never reach on_failure_callback on
                              their own -- this is the deliberate second
                              path that keeps them from being silently
                              buried in logs nobody reads.

Both funnel into the same alerts.log, so there's one place to check
regardless of whether something failed outright or just warned.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

# Same file every DAG writes to, so all failures and warnings land in
# one place.
ALERT_LOG_PATH = Path(__file__).resolve().parents[1] / "alerts.log"


def write_alert(alert: dict):
    """
    Shared by both alert entry points below. Prints to the task's own
    Airflow log (so it's visible without cross-referencing another file)
    and appends to the shared alerts.log, tolerating write failures so
    the alerting mechanism itself can never crash whatever called it.
    """
    print(f"[ALERT] {json.dumps(alert)}")
    try:
        with open(ALERT_LOG_PATH, "a") as f:
            f.write(json.dumps(alert) + "\n")
    except Exception as write_err:
        print(f"[ALERT] Failed to write to alert log: {write_err}")


def alert_on_failure(context):
    """
    Airflow on_failure_callback signature: receives a single `context`
    dict (Airflow's standard task-execution context, with task_instance,
    exception, run_id, etc. already populated).
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")

    alert = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "severity": "failure",
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "run_id": context.get("run_id"),
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
        "error": str(exception) if exception else "unknown",
        "log_url": task_instance.log_url,
    }

    write_alert(alert)


def alert_on_dbt_test_warning(dag_id, task_id, run_id, test_name, message):
    """
    Called explicitly (not as an Airflow callback) by a task that has
    already parsed dbt's run_results.json and found a severity=warn
    result. Kept as a plain function rather than another
    on_failure_callback, since a warn result is by definition not a task
    failure -- the task that calls this should still exit successfully.
    """
    alert = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "severity": "warn",
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "test_name": test_name,
        "error": message,
    }

    write_alert(alert)