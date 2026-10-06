# Transit Delay & Weather Warehouse

A data pipeline and dashboard tracking Austin CapMetro bus performance, built
to answer a simple question: **does weather actually make buses run late,
and by how much?**

Real, messy public transit data flows in continuously, gets cleaned and
modeled with dbt, is watched by its own monitoring layer, and lands in a
Power BI dashboard: the kind of setup a small data engineering team would
actually run, not a one-off script against a clean CSV.

---

## Key findings

Based on the current rolling window of live-polled trip data: **64.5%** of
trips run on-time, **20.8%** run early, and **14.7%** run late. The
weather side of the question isn't answerable with confidence yet, the
data window hasn't had enough day-to-day weather variety (or any measurable
rain) to draw real conclusions from, so those charts exist and work, but
their findings are intentionally withheld until there's enough data behind
them. Full writeup, including the dashboard screenshot and what would make
the weather findings trustworthy: [FINDINGS.md](FINDINGS.md).

---

## Why this project

I live in Texas, and Austin's transit data happens to be public and well
documented (also because I just like visiting the city), which made it a
good real world dataset to build something around. The question I actually
wanted to answer was simple: does weather have anything to do with why
buses run late, and can I actually find that out from the data CapMetro
publishes?

Once I started pulling the live feeds, it turned into a bigger project than
I expected. Live vehicle data doesn't just sit there waiting to be
analyzed. It has to be caught in the moment, cleaned up, and joined against
a schedule that's also quietly changing underneath you. That turned out to
be a genuinely interesting problem, and I ended up building this less like
a single analysis and more like something meant to keep running: scheduled
ingestion, its own monitoring, a dashboard that updates as new data comes
in. That's closer to how I imagine this kind of work actually happens on a
real team, and I wanted to see if I could build and operate something at
that level, not just analyze a dataset once and call it done.

A few things that came out of building it this way:

- Working with CapMetro's real feeds meant real data quality problems to
  solve, not a pre-cleaned file
- A proper dimensional model (facts and dimensions) instead of flat tables
- A live polling layer alongside the static schedule
- Scheduled orchestration with retries, instead of running scripts by hand
- Monitoring that catches silent failures, not just whether a task
  technically succeeded
- One real incident along the way, including actual data loss and
  recovering from it, documented rather than glossed over
- An honest answer to the weather question, including admitting where the
  data isn't conclusive yet

---

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Language | Python | Ingestion scripts (schedule, realtime, weather pollers) |
| Warehouse | DuckDB | No server to manage, fast, behaves like a real analytical warehouse locally |
| Transformation | dbt (dbt-duckdb) | Industry-standard transformation tool; same skills transfer directly to Snowflake/BigQuery later |
| Orchestration | Airflow | Schedules ingestion and dbt runs; handles retries and failure callbacks |
| Data sources | CapMetro GTFS Static + GTFS-Realtime | Austin's public transit schedule and live vehicle/trip data |
| Weather | NWS API (station KAUS) | Free, no API key required |
| Dashboard | Power BI | Delay trends, on-time performance, weather correlation |

---

## How the data model works

GTFS data isn't one flat table: it's several related pieces:
- `routes` : the idea of a bus line (e.g. "Route 1"), rarely changes
- `trips` : one specific scheduled run of a route (the 4:36am bus)
- `stop_times` : for one trip, the ordered schedule of stops and times
- `stops` : physical locations, reused across many routes

A "stop" in GTFS means a **bus stop location**, not a physical pause in
traffic.

**Staging vs. marts, in dbt:**
- *Staging models* (`stg_*`) do light cleanup only: renaming, type casting,
  selecting relevant columns. No joins, no business logic. One staging model
  per raw source table. If a staging model needs an explanation beyond "the
  raw type/name was wrong," it's doing too much.
- *Marts* (`fct_*`, `dim_*`) are where the real joins and judgment calls
  live. This is where delay is actually calculated, where weather gets
  matched to trips, and where the project's real logic lives.

**The core fact tables:**
- `fct_trip_stop_events`: joins routes, trips, stop_times, and stops into
  one row per scheduled stop event, turning four raw tables that
  individually answer nothing into something readable, like *"Route 1
  northbound leaves Bluff Springs/William Cannon at 4:36am."*
- `fct_realtime_delays`: joins live vehicle/trip data against the scheduled
  events to compute actual delay per trip-stop, plus derived fields like
  data lag and whether a row arrived after the moment it was predicting had
  already passed.
- `fct_delays_with_weather`: joins delay data to hourly weather
  observations, so delay and weather can be analyzed together.

**Automated data quality, via dbt tests**, replacing what would otherwise be
manual spot-checks: does every `stop_time` point to a `trip` that actually
exists (a `relationships` test), are primary keys actually unique, and, for
the realtime data, does the rate of "late-arriving" facts stay within a
sane bound (a custom test that warns rather than hard-fails, since some late
arrival is expected and shouldn't block the pipeline).

Verified against the real CapMetro static feed: 71 routes, 25,309 trips,
2,348 stops, 860,617 `stop_times` rows, with zero orphaned rows found in the
join relationships.

---

## Ingestion

Three independent pollers, each on its own Airflow schedule:
- **GTFS-Realtime** (every minute): live vehicle positions and trip
  updates, the highest-frequency and most operationally important feed
- **GTFS static** (daily): the underlying schedule, which changes far less
  often
- **Weather** (every 15 minutes): NWS observations for Austin-Bergstrom;
  deliberately not polled as frequently as the realtime feed, since the
  station itself only reports roughly hourly

Each raw table is a **landing layer**: unprocessed, exactly as received.
The static feed additionally tags each load with a `_snapshot_id`, since the
schedule itself changes over time as CapMetro restructures routes.

---

## Orchestration & reliability

Airflow schedules all ingestion and dbt runs, with per-task retry tuning
(short delays for warehouse-lock contention, longer delays for
network-facing tasks). On top of basic scheduling, the pipeline has its own
monitoring layer, built specifically because a task reporting "success"
doesn't guarantee the underlying data is actually healthy:

- **Failure alerting**: a custom callback logs any task failure, plus a
  separate path for dbt test warnings, which dbt itself exits 0 on and
  would otherwise go unnoticed.
- **Feed staleness detection**: a dedicated DAG, deliberately decoupled
  from the poller itself, checking the actual age of the newest row rather
  than trusting the poller's own exit code. This catches the case where a
  feed silently stops updating even though the polling task keeps
  "succeeding."
- **A real production incident**: the DuckDB warehouse file was renamed
  while a DAG was still actively writing to it, causing genuine data loss
  (SCD history and prior delay rows). It was recovered by rebuilding from
  raw static data plus a fresh snapshot, and the incident is documented in
  full, including what made it possible and how the fix (pausing DAGs
  before touching the warehouse file, rather than reinitializing the
  database) actually addresses the root cause.

---

## Dashboard

Power BI connects to DuckDB via a Python script data source (DuckDB has no
native Power BI connector), pulling a rolling 7-day window rather than the
full multi-million-row history. It shows on-time percentage, a delay-status
breakdown, average delay by route, average delay by weather condition, and
a temperature-vs-delay scatter.

A rain-vs-no-rain comparison was deliberately **left out** of the current
dashboard: the data window so far hasn't included measurable rainfall, and
shipping that chart would imply a finding the data can't actually support
yet. It exists as a ready-to-enable visual, not a live claim.

See [FINDINGS.md](FINDINGS.md) for the full writeup, including a dashboard
screenshot.

---

## Known limitations

- A midnight-boundary bug in service-date attribution produces outlier
  delay values (trips crossing midnight can get misattributed to the wrong
  service date). This is currently filtered around downstream rather than
  fixed at the source, a deliberate stopgap, tracked as follow-up work.
- Weather correlation (both temperature and precipitation) has too little
  variance in the data collected so far to draw real conclusions from.
  Charts exist, but conclusions are intentionally withheld until more days
  of data accumulate.
- `polled_at` is stored as text through much of the pipeline and cast to a
  proper timestamp downstream on an as-needed basis, rather than typed
  correctly at the source.

---

## A few notable bugs, and why they're worth mentioning

Debugging real, messy data surfaces problems a clean tutorial dataset never
would. A few that shaped the current design:

- DuckDB auto-creates an empty file if nothing exists at a given path when
  a connection opens, meaning a mistaken rename or move doesn't fail
  loudly, it just silently starts a new, empty warehouse. This is what
  caused the real data-loss incident above.
- A timestamp column came back from DuckDB as a plain string rather than a
  real datetime type, silently breaking comparisons in two separate places
  until explicitly parsed.
- A weather-join bug compared a naive local timestamp against a UTC-based
  one, silently producing a ~6% match rate between delay and weather data
  until the timezone handling was fixed (up to ~40% afterward, the
  remainder being, correctly, history that predates weather polling).
- A categorical column cast to an integer type silently turned a
  category-axis bar chart into a broken continuous-scale scatter plot in
  the dashboard, a reminder that type decisions made early in a pipeline
  surface in unexpected places much later.

---

## Project structure

```
transit-project/
├── data/
│   ├── raw/gtfs_static/              # timestamped raw zip snapshots
│   └── warehouse.duckdb              # the DuckDB database file
├── docs/
│   └── images/
│       └── dashboard-overview.png    # screenshot used in FINDINGS.md
├── models/
│   ├── staging/
│   │   ├── _sources.yml              # declares raw_* tables as dbt sources
│   │   ├── _staging_schema.yml       # data quality tests
│   │   ├── stg_routes.sql
│   │   ├── stg_trips.sql
│   │   ├── stg_stops.sql
│   │   ├── stg_stop_times.sql
│   │   ├── stg_trip_updates.sql
│   │   └── stg_weather.sql
│   └── marts/
│       ├── fct_trip_stop_events.sql
│       ├── fct_realtime_delays.sql
│       └── fct_delays_with_weather.sql
├── snapshots/
│   └── routes_snapshot.sql           # SCD Type 2 history of routes
├── tests/
│   └── assert_late_arriving_rate_reasonable.sql
├── airflow/
│   └── dags/
│       ├── alerting.py
│       ├── gtfs_static_pipeline_dag.py
│       ├── gtfs_realtime_polling_dag.py
│       ├── gtfs_feed_staleness_dag.py
│       └── gtfs_weather_poll_dag.py
├── dbt_project.yml
├── profiles.yml
├── ingest_gtfs_static.py
├── poll_gtfs_realtime.py
├── poll_weather.py
├── explore_gtfs.py
├── simulate_route_change.py          # demo: fakes a route rename to exercise SCD2
├── check_route_history.py            # demo: shows the snapshot kept both versions
├── FINDINGS.md
└── requirements.txt
```

---

## Running it locally

Airflow doesn't run natively on Windows, so the whole pipeline runs under
WSL (Ubuntu, Python 3.10). The DAGs work out the project location from their
own path, so no paths need editing. If your WSL virtualenv isn't at
`/root/transit-venv`, set `TRANSIT_VENV` to its location.

```bash
# Inside WSL, from the project directory
python3 -m venv /root/transit-venv
source /root/transit-venv/bin/activate
pip install -r requirements.txt
pip install "apache-airflow==2.10.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.10.txt"

# One-off static ingestion and exploration
python ingest_gtfs_static.py
python explore_gtfs.py

# Build and test the dbt models
dbt snapshot --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .

# Run the full pipeline continuously
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
```

Leave the Airflow terminal open. Closing it stops all scheduled ingestion,
transformation, and monitoring.

---

## Data sources

- CapMetro (Austin, TX) GTFS Schedule and GTFS-Realtime feeds, via
  [data.texas.gov](https://data.texas.gov/download/r4v4-vz24/application/zip)
- National Weather Service API (`api.weather.gov`), station KAUS
  (Austin-Bergstrom)
- GTFS spec reference: [gtfs.org](https://gtfs.org/)
