"""
poll_gtfs_realtime.py

Stage 5: pulls CapMetro's live GTFS-Realtime feeds (vehicle positions +
trip updates) ONE TIME per run and appends the parsed results into
DuckDB. Run this repeatedly (every 30-60s) to build up a history of
actual vehicle movements you can later compare against the *scheduled*
times in fct_trip_stop_events to compute real delays.

Unlike the static feed (a schedule, downloaded once), this is protobuf-
encoded and changes every time you poll it - that's the nature of
"realtime" data. Each poll gets its own timestamp so you build up a
time series instead of overwriting the same row.

Two feeds:
  - Vehicle Positions: where each bus physically is right now
  - Trip Updates: predicted arrival/departure times per stop, which is
    what actually lets us compute a delay (scheduled vs predicted)
"""

import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2

VEHICLE_POSITIONS_URL = "https://data.texas.gov/download/eiei-9rpf/application/octet-stream"
TRIP_UPDATES_URL = "https://data.texas.gov/download/rmk2-acnw/application/octet-stream"

DB_PATH = Path("data/warehouse.duckdb")


def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    """Download and parse one GTFS-RT protobuf feed."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def parse_vehicle_positions(feed: gtfs_realtime_pb2.FeedMessage, polled_at: str) -> list[dict]:
    """
    Extract one row per vehicle currently reporting a position.
    Skips entities that aren't actually vehicle position updates, and
    skips vehicles with no assigned trip (out of service).
    """
    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        if not v.trip.trip_id:
            continue  # vehicle not currently assigned to a trip

        rows.append({
            "polled_at": polled_at,
            "vehicle_id": v.vehicle.id,
            "trip_id": v.trip.trip_id,
            "route_id": v.trip.route_id,
            "latitude": v.position.latitude,
            "longitude": v.position.longitude,
            "vehicle_timestamp": v.timestamp,
        })
    return rows


def parse_trip_updates(feed: gtfs_realtime_pb2.FeedMessage, polled_at: str) -> list[dict]:
    """
    Extract one row per (trip, stop) prediction. This is the piece that
    eventually lets us compute delay = predicted_time - scheduled_time.
    """
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip_id = tu.trip.trip_id
        route_id = tu.trip.route_id

        for stu in tu.stop_time_update:
            # a stop_time_update can have arrival, departure, or both
            predicted_arrival = stu.arrival.time if stu.HasField("arrival") else None
            predicted_departure = stu.departure.time if stu.HasField("departure") else None

            # IMPORTANT: check HasField on the inner 'delay' field itself,
            # not just on the parent 'arrival' message. If delay was never
            # sent by the source feed, protobuf silently defaults it to 0 -
            # which would be wrongly read as "confirmed on time" instead of
            # "we don't actually know". This bug produced entirely-zero
            # delay results the first time this script ran.
            if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                delay_seconds = stu.arrival.delay
            elif stu.HasField("departure") and stu.departure.HasField("delay"):
                delay_seconds = stu.departure.delay
            else:
                delay_seconds = None

            rows.append({
                "polled_at": polled_at,
                "trip_id": trip_id,
                "route_id": route_id,
                "stop_id": stu.stop_id,
                "stop_sequence": stu.stop_sequence,
                "predicted_arrival": predicted_arrival,
                "predicted_departure": predicted_departure,
                "delay_seconds": delay_seconds,
            })
    return rows


def append_rows(con: duckdb.DuckDBPyConnection, table_name: str, rows: list[dict]) -> None:
    """
    Append parsed rows to a DuckDB table, creating it on first run.
    Uses a pandas DataFrame as an intermediate step so DuckDB can infer
    correct column types automatically, rather than building fragile
    hand-written SQL.
    """
    if not rows:
        print(f"  No rows parsed for {table_name} - skipping")
        return

    df = pd.DataFrame(rows)

    table_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?
    """, [table_name]).fetchone()[0] > 0

    if table_exists:
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    else:
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    print(f"  Appended {len(rows)} rows to {table_name}")


def main():
    polled_at = datetime.now(timezone.utc).isoformat()
    con = duckdb.connect(str(DB_PATH))

    print(f"Polling GTFS-Realtime feeds at {polled_at}")

    print("Fetching vehicle positions...")
    vp_feed = fetch_feed(VEHICLE_POSITIONS_URL)
    vp_rows = parse_vehicle_positions(vp_feed, polled_at)
    append_rows(con, "raw_vehicle_positions", vp_rows)

    print("Fetching trip updates...")
    tu_feed = fetch_feed(TRIP_UPDATES_URL)
    tu_rows = parse_trip_updates(tu_feed, polled_at)
    append_rows(con, "raw_trip_updates", tu_rows)

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()