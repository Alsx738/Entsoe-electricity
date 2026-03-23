{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='country_price_load_day_key',
    partition_by={'field': 'ds', 'data_type': 'date'},
    cluster_by=['country'],
    on_schema_change='sync_all_columns'
  )
}}

-- Step 1: Read daily country price facts.
with price_daily as (
    select
        country,
        ds,
        price_eur_mwh_avg,
        price_eur_mwh_peak,
        price_eur_mwh_min,
        price_eur_mwh_stddev,
        price_negative_ratio,
        price_missing_ratio
    from {{ ref('fct_country_price_daily') }}
    {% if is_incremental() %}
            where {{ incremental_ds_filter('ds') }}
    {% endif %}
),

-- Step 2: Read daily country load facts.
load_daily as (
    select
        country,
        ds,
        load_mw_sum,
        load_mw_avg,
        load_mw_peak,
        load_non_positive_ratio
    from {{ ref('fct_country_load_daily') }}
    {% if is_incremental() %}
            where {{ incremental_ds_filter('ds') }}
    {% endif %}
),

-- Step 3: Join price and load at country-day grain.
-- Why:
--   Full outer join allows monitoring even when one domain is delayed.
country_price_load_joined as (
    select
        coalesce(p.country, l.country) as country,
        coalesce(p.ds, l.ds) as ds,
        p.price_eur_mwh_avg,
        p.price_eur_mwh_peak,
        p.price_eur_mwh_min,
        p.price_eur_mwh_stddev,
        p.price_negative_ratio,
        p.price_missing_ratio,
        l.load_mw_sum,
        l.load_mw_avg,
        l.load_mw_peak,
        l.load_non_positive_ratio
    from price_daily p
    full outer join load_daily l
        on p.country = l.country
       and p.ds = l.ds
)

-- Step 4: Compute cross-domain KPIs.
-- Why:
--   This mart provides a compact monitoring surface for market-demand relationships.
select
    {{ key_country_day('ds', 'country') }} as country_price_load_day_key,
    {{ date_key_from_ds('ds') }} as date_key,
    country as country_key,
    country,
    ds,
    {{ year_from_ds('ds') }} as year,
    price_eur_mwh_avg,
    price_eur_mwh_peak,
    price_eur_mwh_min,
    price_eur_mwh_stddev,
    price_negative_ratio,
    price_missing_ratio,
    load_mw_sum,
    load_mw_avg,
    load_mw_peak,
    load_non_positive_ratio,
    {{ safe_divide('price_eur_mwh_avg', 'load_mw_avg') }} as price_to_load_ratio,
    case
        when price_eur_mwh_avg is null then null
        when price_eur_mwh_avg < 0 then 'negative_price_regime'
        when price_eur_mwh_avg < 50 then 'low_price_regime'
        when price_eur_mwh_avg < 120 then 'mid_price_regime'
        else 'high_price_regime'
    end as price_regime
from country_price_load_joined
