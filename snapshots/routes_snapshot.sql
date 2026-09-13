{#
    SCD Type 2 on routes.

    Every time `dbt snapshot` runs, dbt compares the current state of
    stg_routes against what it captured last time. If nothing changed for
    a route, nothing happens. If something changed (e.g. CapMetro renames
    a route or changes its type), dbt:
      1. Closes out the old row by setting dbt_valid_to to now
      2. Inserts a new row with dbt_valid_from = now, dbt_valid_to = null

    This means you never lose history - you can always ask "what did
    Route 1 look like on any given date in the past?" instead of only
    ever seeing today's version.

    strategy='check' means: compare the columns listed in check_cols to
    decide if a row changed. (The alternative, 'timestamp', relies on a
    reliable updated_at column - GTFS routes.txt doesn't have one, so
    'check' is the right choice here.)
#}

{% snapshot routes_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='route_id',
        strategy='check',
        check_cols=['route_short_name', 'route_long_name', 'route_type'],
    )
}}

select
    route_id,
    route_short_name,
    route_long_name,
    route_type
from {{ ref('stg_routes') }}

{% endsnapshot %}
