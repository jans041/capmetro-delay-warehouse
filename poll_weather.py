"""
Stage 8 — weather polling.

Pulls the latest observation from Austin-Bergstrom Airport (station
KAUS) via the National Weather Service API. Chosen over a commercial
provider (OpenWeatherMap, etc.) specifically because it's free and
requires no API key or account -- consistent with the rest of this
project (CapMetro's GTFS-RT feed is also free/keyless).

NWS observations update roughly hourly at this station, not
continuously -- polling more often than that just re-fetches the same
observation until the station reports a new one. That's fine: the
'timestamp' field in the response is the station's own observation
time, not our poll time, so downstream joins should key off that, not
off polled_at, if precise timing matters.

NWS API usage policy requires a descriptive User-Agent identifying the
app and a contact method -- update WEATHER_USER_AGENT below with a real
contact if you want to be a good API citizen (not required for this
low-volume personal-project use, but polite).
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

WAREHOUSE_PATH = str(Path(__file__).resolve().parent / "data" / "warehouse.duckdb")

STATION_ID = "KAUS"  # Austin-Bergstrom International Airport
OBSERVATIONS_URL = f"https://api.weather.gov/stations/{STATION_ID}/observations/latest"
WEATHER_USER_AGENT = "(transit-project, contact@example.com)"


def c_to_f(celsius):
    if celsius is None:
        return None
    return celsius * 9 / 5 + 32


def kmh_to_mph(kmh):
    if kmh is None:
        return None
    return kmh / 1.60934


def fetch_latest_observation():
    response = requests.get(
        OBSERVATIONS_URL,
        headers={"User-Agent": WEATHER_USER_AGENT, "Accept": "application/geo+json"},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def main():
    polled_at = datetime.now(timezone.utc).isoformat()

    data = fetch_latest_observation()
    props = data.get("properties", {})

    observation_time = props.get("timestamp")  # station's own observation time
    temperature_f = c_to_f((props.get("temperature") or {}).get("value"))
    wind_speed_mph = kmh_to_mph((props.get("windSpeed") or {}).get("value"))
    relative_humidity_pct = (props.get("relativeHumidity") or {}).get("value")
    precip_last_hour_mm = (props.get("precipitationLastHour") or {}).get("value")
    condition_text = props.get("textDescription")

    con = duckdb.connect(WAREHOUSE_PATH)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_weather (
                polled_at VARCHAR,
                observation_time VARCHAR,
                station_id VARCHAR,
                temperature_f DOUBLE,
                wind_speed_mph DOUBLE,
                relative_humidity_pct DOUBLE,
                precip_last_hour_mm DOUBLE,
                condition_text VARCHAR
            )
            """
        )
        con.execute(
            """
            INSERT INTO raw_weather
            (polled_at, observation_time, station_id, temperature_f,
             wind_speed_mph, relative_humidity_pct, precip_last_hour_mm,
             condition_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                polled_at,
                observation_time,
                STATION_ID,
                temperature_f,
                wind_speed_mph,
                relative_humidity_pct,
                precip_last_hour_mm,
                condition_text,
            ],
        )
    finally:
        con.close()

    print(
        f"Polled weather at {polled_at}: station observation from "
        f"{observation_time}, {temperature_f}F, {condition_text}"
    )


if __name__ == "__main__":
    main()
