-- Cleaned, renamed view over raw_routes.
-- Staging models do light cleanup only: rename, cast types, select
-- only the columns you actually need. No joins here.

select
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    _snapshot_id
from {{ source('gtfs_raw', 'raw_routes') }}
