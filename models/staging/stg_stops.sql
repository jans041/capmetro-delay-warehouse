select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    _snapshot_id
from {{ source('gtfs_raw', 'raw_stops') }}
