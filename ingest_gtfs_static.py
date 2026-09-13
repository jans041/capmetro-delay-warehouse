"""
ingest_gtfs_static.py

Downloads a GTFS static feed (a zip of CSV files), saves a timestamped
raw copy, and loads each file into DuckDB as a table.

Why save a timestamped raw copy at all? Because GTFS static feeds change
over time (routes get added/removed, schedules shift). Keeping every
snapshot means you can later build a Slowly Changing Dimension on top
of this instead of only ever seeing "today's" version of the schedule.
"""

import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

# CapMetro's official GTFS Schedule feed (Austin, TX).
# Swap this for any other agency's feed URL from mobilitydatabase.org.
GTFS_URL = "https://data.texas.gov/download/r4v4-vz24/application/zip"

RAW_DIR = Path("data/raw/gtfs_static")
DB_PATH = Path("data/warehouse.duckdb")


def download_gtfs_zip(url: str) -> bytes:
    """Download the GTFS zip and return its raw bytes."""
    print(f"Downloading GTFS feed from {url} ...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()  # fail loudly if the download didn't work
    print(f"Downloaded {len(response.content):,} bytes")
    return response.content


def save_raw_snapshot(zip_bytes: bytes) -> Path:
    """
    Save the raw zip with a timestamp in the filename.
    This is your 'landing layer' — untouched, exactly as received.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_path = RAW_DIR / f"gtfs_{timestamp}.zip"
    snapshot_path.write_bytes(zip_bytes)
    print(f"Saved raw snapshot to {snapshot_path}")
    return snapshot_path


def load_into_duckdb(zip_bytes: bytes, snapshot_path: Path) -> None:
    """
    Extract each .txt file from the GTFS zip and load it into DuckDB
    as a table named raw_<filename>, e.g. raw_stops, raw_routes.
    Also tags every row with which snapshot it came from, so multiple
    downloads over time don't overwrite each other.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    snapshot_id = snapshot_path.stem  # e.g. "gtfs_20260817T140000Z"

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        txt_files = [n for n in z.namelist() if n.endswith(".txt")]
        print(f"Found {len(txt_files)} GTFS files: {txt_files}")

        for filename in txt_files:
            table_name = "raw_" + filename.replace(".txt", "")

            # Extract this one file to a temp path so DuckDB can read it
            # directly with its fast native CSV reader.
            extracted_path = RAW_DIR / f"_tmp_{filename}"
            with z.open(filename) as source, open(extracted_path, "wb") as dest:
                dest.write(source.read())

            # ignore_errors handles rows with unexpected extra/missing
            # columns instead of crashing the whole load - this is the
            # kind of "real data is messy" handling worth having early.
            #
            # IMPORTANT: force arrival_time/departure_time to stay as
            # VARCHAR instead of letting DuckDB auto-infer them as TIME.
            # GTFS allows scheduled times like "25:10:00" for trips that
            # run past midnight (still counted as the previous service
            # day) - DuckDB's TIME type caps at 23:59:59 and cannot
            # represent this. Worse, DuckDB's type sniffer only samples
            # part of the file, so on a large feed it can infer TIME from
            # daytime rows early in the file, then silently NULL out any
            # late-night rows it encounters later (ignore_errors=true
            # swallows the failure instead of raising it). Keeping this
            # column as text avoids that silent data loss entirely.
            type_overrides = ""
            if filename == "stop_times.txt":
                type_overrides = ", types={'arrival_time': 'VARCHAR', 'departure_time': 'VARCHAR'}"

            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *, '{snapshot_id}' AS _snapshot_id
                FROM read_csv_auto('{extracted_path}', ignore_errors=true{type_overrides})
            """)

            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  Loaded {table_name}: {row_count:,} rows")

            extracted_path.unlink()  # clean up the temp file

    con.close()
    print(f"\nDone. Data loaded into {DB_PATH}")


def main():
    zip_bytes = download_gtfs_zip(GTFS_URL)
    snapshot_path = save_raw_snapshot(zip_bytes)
    load_into_duckdb(zip_bytes, snapshot_path)


if __name__ == "__main__":
    main()