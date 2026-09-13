select
    trip_id,
    route_id,
    service_id,
    trip_headsign,
    _snapshot_id
from {{ source('gtfs_raw', 'raw_trips') }}
