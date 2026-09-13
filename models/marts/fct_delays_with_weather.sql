{#
    Stage 8 -- delay/weather correlation.

    Joins every delay prediction in fct_realtime_delays to the weather
    conditions in effect at that hour, so the two can be plotted
    together (e.g. "does temperature or precipitation correlate with
    delay").

    DEDUPLICATION IS REQUIRED HERE, not optional: raw_weather is polled
    every 15 minutes but NWS's station only reports a new observation
    roughly hourly, so a given hour typically contains ~4 raw_weather
    rows that are all identical copies of the same observation. Joining
    fct_realtime_delays directly against stg_weather on a shared hour
    bucket without deduplicating first would silently multiply every
    delay row by however many duplicate weather polls happened to land
    in that hour -- a classic fan-out bug that looks fine at a glance
    (the query runs, the numbers are plausible) but quietly inflates
    every downstream count and average. weather_hourly below collapses
    each hour down to exactly one row before the join ever happens.

    LEFT JOIN, not inner: delay data existed before weather polling
    started, and will keep existing during any future gap in weather
    collection (NWS outage, poller down, etc.) without that gap
    silently deleting delay rows from this model.
#}

with weather_hourly as (
    select
        -- predicted_arrival_local (joined against below) is a naive
        -- Chicago wall-clock timestamp, not UTC -- observation_time must
        -- be converted the same way before truncating, or this silently
        -- compares two different moments 5 hours apart and never matches.
        date_trunc('hour', observation_time at time zone 'America/Chicago') as weather_hour,
        temperature_f,
        wind_speed_mph,
        relative_humidity_pct,
        precip_last_hour_mm,
        condition_text
    from {{ ref('stg_weather') }}
    qualify row_number() over (
        partition by date_trunc('hour', observation_time)
        order by polled_at desc
    ) = 1
)

select
    d.polled_at,
    d.trip_id,
    d.route_id,
    d.route_short_name,
    d.trip_headsign,
    d.stop_id,
    d.stop_name,
    d.scheduled_arrival_local,
    d.predicted_arrival_local,
    d.delay_seconds,
    d.delay_minutes,
    d.delay_status,
    w.temperature_f,
    w.wind_speed_mph,
    w.relative_humidity_pct,
    w.precip_last_hour_mm,
    w.condition_text
from {{ ref('fct_realtime_delays') }} d
left join weather_hourly w
    on date_trunc('hour', d.predicted_arrival_local) = w.weather_hour