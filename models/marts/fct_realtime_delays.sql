{#
    This is the payoff model: one row per real-time delay, computed by
    comparing predicted arrival times against the scheduled times, joined
    with enough context (route name, stop name) to actually be useful in
    a dashboard or query.

    IMPORTANT: CapMetro's feed sends predicted arrival TIMESTAMPS but does
    not populate the feed's own 'delay' field, so we compute delay
    ourselves: delay = predicted_arrival - scheduled_arrival.

    This requires matching a Unix timestamp (predicted_arrival, absolute
    UTC moment) against a GTFS scheduled time (a "HH:MM:SS since midnight"
    string with NO date attached, and which can exceed 24:00:00 for
    trips that run past midnight). We do this by:
      1. Converting predicted_arrival to Central time to find the correct
         local calendar date
      2. Adding the scheduled HH:MM:SS as an interval on top of that
         date's midnight (this naturally handles GTFS's >24:00:00
         encoding for late-night trips, since adding e.g. 25 hours just
         rolls into the next day correctly)
      3. Taking the difference in seconds

    KNOWN LIMITATION: for trips that start before midnight and continue
    after it, the service date should technically be based on when the
    TRIP started, not when this particular stop event was recorded. This
    model instead infers the service date from the stop event's own
    timestamp, which is correct for the vast majority of trips but can
    misattribute the service date for the small number of very-late-night
    routes near the midnight boundary. Worth revisiting if precise
    overnight-route accuracy becomes important.

    delay_status buckets:
      early:    delay_seconds < -60   (more than a minute ahead)
      on_time:  -60 to 300 seconds    (industry-standard on-time window)
      late:     delay_seconds > 300   (more than 5 minutes behind)

    Stage 7 -- late-arriving facts:
    This model is a full rebuild every run (see dbt_project.yml, marts
    default to materialized='table'), not incremental, so a raw row that
    shows up late is never silently dropped -- it just gets picked up on
    the next rebuild. But "never lost" isn't the same as "never late":
    data_lag_seconds and is_late_arriving capture, per row, whether this
    specific prediction was already captured for a moment that had
    already passed by the time our poller wrote it (polled_at is a
    per-row capture timestamp, so this is a stable historical fact about
    that row, not something that drifts as time passes at query time).
    A rising late-arriving rate is a real symptom of feed lag or gaps in
    the poll cycle, and is watched by a dedicated dbt test rather than
    silently living only in this table.
#}

with realtime as (
    select
        polled_at,
        trip_id,
        stop_id,
        stop_sequence,
        predicted_arrival,
        to_timestamp(predicted_arrival) at time zone 'America/Chicago' as predicted_arrival_local,
        -- Both sides of this subtraction are absolute UTC instants:
        -- predicted_arrival is already a raw Unix epoch from the feed, and
        -- casting polled_at (an ISO-8601 string) to TIMESTAMPTZ then
        -- through epoch() gives the same units. Positive = this row was
        -- captured after the moment it was predicting had already passed.
        epoch(cast(polled_at as timestamptz)) - predicted_arrival as data_lag_seconds
    from {{ ref('stg_trip_updates') }}
    where predicted_arrival is not null
),

scheduled as (
    select
        trip_id,
        stop_id,
        arrival_time as scheduled_arrival_time
    from {{ ref('stg_stop_times') }}
),

joined as (
    select
        rt.polled_at,
        rt.trip_id,
        rt.stop_id,
        rt.stop_sequence,
        rt.predicted_arrival_local,
        rt.data_lag_seconds,
        date_trunc('day', rt.predicted_arrival_local)
            + (split_part(sch.scheduled_arrival_time, ':', 1)::int) * interval 1 hour
            + (split_part(sch.scheduled_arrival_time, ':', 2)::int) * interval 1 minute
            + (split_part(sch.scheduled_arrival_time, ':', 3)::int) * interval 1 second
            as scheduled_arrival_local
    from realtime rt
    inner join scheduled sch
        on sch.trip_id = rt.trip_id
        and sch.stop_id = rt.stop_id
),

with_delay as (
    select
        *,
        date_diff('second', scheduled_arrival_local, predicted_arrival_local) as delay_seconds
    from joined
)

select
    wd.polled_at,
    wd.trip_id,
    r.route_id,
    r.route_short_name,
    t.trip_headsign,
    wd.stop_id,
    s.stop_name,
    wd.stop_sequence,
    wd.scheduled_arrival_local,
    wd.predicted_arrival_local,
    wd.delay_seconds,
    round(wd.delay_seconds / 60.0, 1) as delay_minutes,
    case
        when wd.delay_seconds is null then 'unknown'
        when wd.delay_seconds < -60 then 'early'
        when wd.delay_seconds <= 300 then 'on_time'
        else 'late'
    end as delay_status,
    wd.data_lag_seconds,
    wd.data_lag_seconds > 0 as is_late_arriving
from with_delay wd
left join {{ ref('stg_trips') }} t
    on t.trip_id = wd.trip_id
left join {{ ref('stg_routes') }} r
    on r.route_id = t.route_id
left join {{ ref('stg_stops') }} s
    on s.stop_id = wd.stop_id