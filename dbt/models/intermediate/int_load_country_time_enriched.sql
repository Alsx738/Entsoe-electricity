-- Adds time dimensions and a data quality flag to staged load data.
-- is_load_non_positive flags hours where demand is null or non-positive (edge case indicator).
with source_load as (
    select *
    from {{ ref('stg_load') }}
),

load_with_time_dimensions as (
    select
        id,
        timestamp,
        {{ time_dimension_columns('timestamp', include_ds=true) }},
        country,
        year,
        load_mw
    from source_load
)

select
    id,
    timestamp,
    ds,
    country,
    year,
    month,
    hour,
    day_of_week,
    quarter,
    is_weekend,
    season,
    load_mw,
    case
        when load_mw is null then true
        when load_mw <= 0 then true
        else false
    end as is_load_non_positive
from load_with_time_dimensions
