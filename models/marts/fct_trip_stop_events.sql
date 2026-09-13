-- One row per scheduled stop event: a specific trip, at a specific
-- stop, at a specific scheduled time. This is the backbone fact table
-- everything else (delay calcs, weather joins, dashboards) builds on.

select
    r.route_id,
    r.route_short_name,
    t.trip_id,
    t.trip_headsign,
    st.stop_sequence,
    s.stop_id,
    s.stop_name,
    st.arrival_time,
    st.departure_time
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_trips') }} t
    on t.trip_id = st.trip_id
join {{ ref('stg_routes') }} r
    on r.route_id = t.route_id
join {{ ref('stg_stops') }} s
    on s.stop_id = st.stop_id
