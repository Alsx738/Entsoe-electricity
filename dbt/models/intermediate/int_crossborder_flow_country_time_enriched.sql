-- Adds time dimensions, normalises direction values, and computes a signed flow measure.
-- Convention: import = positive, export = negative, unmapped = null.
-- This enables downstream net import/export aggregations without additional sign logic.
with source_crossborder_flows as (
    select *
    from {{ ref('stg_crossborder_flows') }}
),

flows_with_time_dimensions as (
    select
        id,
        timestamp,
        {{ time_dimension_columns('timestamp', include_ds=true) }},
        main_country as country,
        border_country,
        lower(direction) as flow_direction,
        year,
        flow_mw
    from source_crossborder_flows
),

flows_with_signed_measure as (
    select
        id,
        timestamp,
        ds,
        country,
        border_country,
        flow_direction,
        year,
        month,
        hour,
        day_of_week,
        quarter,
        is_weekend,
        season,
        flow_mw,
        case
            when flow_direction = 'import' then flow_mw
            when flow_direction = 'export' then -flow_mw
            else null
        end as signed_flow_mw
    from flows_with_time_dimensions
)

select
    id,
    timestamp,
    ds,
    country,
    border_country,
    flow_direction,
    year,
    month,
    hour,
    day_of_week,
    quarter,
    is_weekend,
    season,
    flow_mw,
    signed_flow_mw,
    case
        when flow_mw is null then true
        else false
    end as is_flow_missing,
    case
        when flow_direction in ('import', 'export') then false
        else true
    end as is_flow_direction_unmapped
from flows_with_signed_measure
