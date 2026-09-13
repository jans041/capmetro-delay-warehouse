{{ config(severity = 'warn') }}

-- Stage 7 -- late-arriving facts.
-- Warns (does not fail the run) if more than 5% of rows currently in
-- fct_realtime_delays were captured after the moment they were
-- predicting had already passed. A late feed is a data-quality signal
-- worth watching over time, not a broken pipeline -- so this is
-- deliberately severity=warn, not the default error. dbt exits 0 on a
-- warn by default, so this never blocks dbt_run_realtime_delays; it's
-- picked up separately by check_dbt_test_warnings in the Airflow DAG,
-- which routes any warn result into alerts.log alongside real failures.

with rate as (
    select
        count(*) as total_rows,
        sum(case when is_late_arriving then 1 else 0 end) as late_rows,
        sum(case when is_late_arriving then 1 else 0 end)::float
            / nullif(count(*), 0) as late_rate
    from {{ ref('fct_realtime_delays') }}
)

select *
from rate
where late_rate > 0.05
