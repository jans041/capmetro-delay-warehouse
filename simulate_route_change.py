"""
simulate_route_change.py

For demonstration purposes only. Real transit agencies rename or
restructure routes a few times a year - we don't want to wait months to
see SCD2 actually do something, so this script mimics that kind of
change happening RIGHT NOW so you can immediately verify your snapshot
correctly preserves history instead of overwriting it.

Run this, then run `dbt snapshot --profiles-dir .` again, then check
the results with check_route_history.py.
"""

import duckdb

con = duckdb.connect("data/warehouse.duckdb")

# Show what route '1' looks like before the change
before = con.execute("""
    SELECT route_id, route_long_name
    FROM raw_routes
    WHERE route_id = '1'
""").fetchone()
print(f"Before: route '1' long name = {before[1]}")

# Simulate CapMetro renaming it
con.execute("""
    UPDATE raw_routes
    SET route_long_name = 'North Lamar/South Congress (TEST RENAME)'
    WHERE route_id = '1'
""")

after = con.execute("""
    SELECT route_id, route_long_name
    FROM raw_routes
    WHERE route_id = '1'
""").fetchone()
print(f"After:  route '1' long name = {after[1]}")

con.close()
print("\nDone. Now run: dbt snapshot --profiles-dir .")
