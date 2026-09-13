"""
check_route_history.py

Run this AFTER: simulate_route_change.py, then dbt snapshot --profiles-dir .

Shows every historical version of route '1' - if SCD2 is working, you
should see 2 rows: the old name (closed out with a dbt_valid_to
timestamp) and the new name (still open, dbt_valid_to is NULL).
"""

import duckdb

con = duckdb.connect("data/warehouse.duckdb")

rows = con.execute("""
    SELECT
        route_id,
        route_long_name,
        dbt_valid_from,
        dbt_valid_to
    FROM snapshots.routes_snapshot
    WHERE route_id = '1'
    ORDER BY dbt_valid_from
""").fetchall()

print(f"Found {len(rows)} historical version(s) of route '1':\n")
for route_id, name, valid_from, valid_to in rows:
    status = "CURRENT" if valid_to is None else "closed"
    print(f"  [{status}] {name}")
    print(f"      valid_from: {valid_from}")
    print(f"      valid_to:   {valid_to}")
    print()

if len(rows) >= 2:
    print("SCD2 is working: history was preserved instead of overwritten.")
else:
    print("Only 1 row found - did you run simulate_route_change.py and")
    print("dbt snapshot --profiles-dir . before this script?")
