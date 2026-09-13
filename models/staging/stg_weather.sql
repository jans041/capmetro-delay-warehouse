-- Cleaned view over the raw weather poll history.
-- One row per poll, same granularity as the source table. NWS station
-- observations only update roughly hourly, so most polls in a given hour
-- return the same observation_time repeated -- dedup happens downstream
-- in the mart that joins this to delay data, not here, to keep this
-- layer a straightforward typed pass-through like the other staging
-- models.
select
    cast(polled_at as timestamptz) as polled_at,
    cast(observation_time as timestamptz) as observation_time,
    station_id,
    temperature_f,
    wind_speed_mph,
    relative_humidity_pct,
    precip_last_hour_mm,
    condition_text
from {{ source('weather_raw', 'raw_weather') }}
