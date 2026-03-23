{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='country_price_day_key',
    partition_by={'field': 'ds', 'data_type': 'date'},
    cluster_by=['country'],
    on_schema_change='sync_all_columns'
  )
}}

-- Step 1: Read enriched price rows.
-- Why:
--   Intermediate model exposes reusable time dimensions and quality indicators.
with source_prices as (
    select *
    from {{ ref('int_price_country_time_enriched') }}
    {% if is_incremental() %}
      where {{ incremental_ds_filter('ds') }}
    {% endif %}
)

-- Step 2: Aggregate at country-day grain.
-- Why:
--   Daily market analysis needs robust central tendency and volatility metrics.
select
    {{ key_country_day('ds', 'country') }} as country_price_day_key,
    {{ date_key_from_ds('ds') }} as date_key,
    country as country_key,
    country,
    ds,
    {{ year_from_ds('ds') }} as year,
    count(*) as row_count,
    avg(price_eur_mwh) as price_eur_mwh_avg,
    max(price_eur_mwh) as price_eur_mwh_peak,
    min(price_eur_mwh) as price_eur_mwh_min,
    stddev_samp(price_eur_mwh) as price_eur_mwh_stddev,
    {{ true_ratio('is_price_negative') }} as price_negative_ratio,
    {{ true_ratio('is_price_missing') }} as price_missing_ratio
from source_prices
group by
    country,
    ds
