-- Adds time dimensions and data quality flags to staged price data.
-- is_price_negative is true for valid negative prices (excess renewables on the grid).
with source_prices as (
    select *
    from {{ ref('stg_prices') }}
),

prices_with_time_dimensions as (
    select
        id,
        timestamp,
        {{ time_dimension_columns('timestamp', include_ds=true) }},
        country,
        year,
        price_eur_mwh
    from source_prices
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
    price_eur_mwh,
    case
        when price_eur_mwh is null then true
        else false
    end as is_price_missing,
    case
        when price_eur_mwh is not null and price_eur_mwh < 0 then true
        else false
    end as is_price_negative
from prices_with_time_dimensions
