"""
explore_gtfs.py

Stage 2: understand how the GTFS tables actually relate to each other
before writing any dbt models. GTFS's structure isn't obvious from the
file names alone, so this script walks the relationships end-to-end:

    routes --> trips --> stop_times --> stops

A "route" (e.g. Bus 1) has many "trips" (each scheduled run of that bus
today). Each trip has many "stop_times" (each stop it makes, in order,
with a scheduled time). Each stop_time points at one "stop" (a physical
location).
"""

import duckdb

con = duckdb.connect("data/warehouse.duckdb")


def section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ---------------------------------------------------------------
section("1. How many rows in each table?")
# ---------------------------------------------------------------
tables = con.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_name LIKE 'raw_%'
    ORDER BY table_name
""").fetchall()

for (table_name,) in tables:
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  {table_name:<25} {count:>10,} rows")


# ---------------------------------------------------------------
section("2. A sample route")
# ---------------------------------------------------------------
route = con.execute("""
    SELECT route_id, route_short_name, route_long_name, route_type
    FROM raw_routes
    LIMIT 1
""").fetchone()
print(f"  route_id: {route[0]}")
print(f"  short name: {route[1]}")
print(f"  long name: {route[2]}")
print(f"  route_type (0=tram, 1=subway, 2=rail, 3=bus): {route[3]}")

route_id = route[0]


# ---------------------------------------------------------------
section(f"3. Trips belonging to route '{route_id}'")
# ---------------------------------------------------------------
trip_count = con.execute("""
    SELECT COUNT(*) FROM raw_trips WHERE route_id = ?
""", [route_id]).fetchone()[0]
print(f"  {trip_count} trips run on this route")

sample_trip = con.execute("""
    SELECT trip_id, service_id, trip_headsign
    FROM raw_trips
    WHERE route_id = ?
    LIMIT 1
""", [route_id]).fetchone()
print(f"  sample trip_id: {sample_trip[0]}")
print(f"  headsign (rider-facing destination text): {sample_trip[2]}")

trip_id = sample_trip[0]


# ---------------------------------------------------------------
section(f"4. Stop times for trip '{trip_id}'")
# ---------------------------------------------------------------
stop_times = con.execute("""
    SELECT stop_sequence, stop_id, arrival_time, departure_time
    FROM raw_stop_times
    WHERE trip_id = ?
    ORDER BY stop_sequence
    LIMIT 10
""", [trip_id]).fetchall()

print(f"  This trip makes {len(stop_times)}+ stops. First few:")
for seq, stop_id, arr, dep in stop_times:
    print(f"    stop #{seq}: stop_id={stop_id}  arrives {arr}  departs {dep}")


# ---------------------------------------------------------------
section("5. Joining everything: route -> trip -> stop -> stop name")
# ---------------------------------------------------------------
# This is the query shape your dbt fact table will eventually be built on.
joined = con.execute("""
    SELECT
        r.route_short_name,
        t.trip_headsign,
        st.stop_sequence,
        s.stop_name,
        st.arrival_time
    FROM raw_routes r
    JOIN raw_trips t ON t.route_id = r.route_id
    JOIN raw_stop_times st ON st.trip_id = t.trip_id
    JOIN raw_stops s ON s.stop_id = st.stop_id
    WHERE r.route_id = ? AND t.trip_id = ?
    ORDER BY st.stop_sequence
    LIMIT 10
""", [route_id, trip_id]).fetchall()

for row in joined:
    print(f"  Route {row[0]} -> {row[1]} | stop #{row[2]}: {row[3]} at {row[4]}")


# ---------------------------------------------------------------
section("6. Data quality check: any orphaned stop_times?")
# ---------------------------------------------------------------
# Real data quality question: does every stop_time point to a trip
# that actually exists? If not, that's exactly the kind of messiness
# worth flagging rather than silently ignoring.
orphans = con.execute("""
    SELECT COUNT(*)
    FROM raw_stop_times st
    LEFT JOIN raw_trips t ON t.trip_id = st.trip_id
    WHERE t.trip_id IS NULL
""").fetchone()[0]
print(f"  stop_times with no matching trip: {orphans}")

orphan_stops = con.execute("""
    SELECT COUNT(*)
    FROM raw_stop_times st
    LEFT JOIN raw_stops s ON s.stop_id = st.stop_id
    WHERE s.stop_id IS NULL
""").fetchone()[0]
print(f"  stop_times with no matching stop: {orphan_stops}")

con.close()