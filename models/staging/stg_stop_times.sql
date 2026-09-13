select
    trip_id,
    stop_id,
    stop_sequence,
    arrival_time,
    departure_time,
    _snapshot_id
from {{ source('gtfs_raw', 'raw_stop_times') }}
