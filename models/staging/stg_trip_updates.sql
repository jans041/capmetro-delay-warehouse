-- Cleaned view over the raw trip_updates poll history.
-- One row per (poll, trip, stop) prediction. delay_seconds comes
-- straight from CapMetro's feed - positive means late, negative means
-- early, null means no prediction was available for that stop yet.

select
    polled_at,
    trip_id,
    route_id,
    stop_id,
    stop_sequence,
    predicted_arrival,
    predicted_departure,
    delay_seconds
from {{ source('gtfs_rt_raw', 'raw_trip_updates') }}
