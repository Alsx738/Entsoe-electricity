-- Step 1: Read crossborder flows staging model.
-- Why:
--   This model is the authoritative source for border pairs and direction values.
with source_crossborder_flows as (
    select *
    from {{ ref('stg_crossborder_flows') }}
),

-- Step 2: Keep one row per country-border-direction combination.
-- Why:
--   BI filters and joins should use a stable, deduplicated border dimension.
connections as (
    select distinct
        cast(main_country as string) as country,
        cast(border_country as string) as border_country,
        lower(cast(direction as string)) as flow_direction
    from source_crossborder_flows
    where main_country is not null
      and border_country is not null
      and direction is not null
)

-- Step 3: Create a deterministic key for dimensional joins.
select
    concat(country, '|', border_country, '|', flow_direction) as country_border_connection_key,
    country,
    border_country,
    flow_direction
from connections
