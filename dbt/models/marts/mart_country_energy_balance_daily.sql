{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='country_energy_balance_day_key',
    partition_by={'field': 'ds', 'data_type': 'date'},
    cluster_by=['country'],
    on_schema_change='sync_all_columns'
  )
}}

-- Step 1: Read country-level generation facts.
with generation_daily as (
    select
        country,
        ds,
        generation_mw_sum,
        generation_mw_avg,
        generation_mw_peak,
        capacity_match_ratio,
        capacity_plausible_ratio
    from {{ ref('fct_generation_capacity_daily_country') }}
    {% if is_incremental() %}
            where {{ incremental_ds_filter('ds') }}
    {% endif %}
),

-- Step 2: Read country-level load facts.
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

-- Step 3: Read country-level net import facts.
net_import_daily as (
    select
        country,
        ds,
        import_flow_mw_sum,
        export_flow_mw_sum,
        net_import_mw_sum,
        net_import_mw_avg,
        net_import_mw_peak
    from {{ ref('fct_country_net_import_daily') }}
    {% if is_incremental() %}
            where {{ incremental_ds_filter('ds') }}
    {% endif %}
),

-- Step 4: Join domains at country-day grain.
-- Why:
--   Full outer join preserves partially available domains during ingestion lag.
country_daily_joined as (
    select
        coalesce(g.country, l.country, n.country) as country,
        coalesce(g.ds, l.ds, n.ds) as ds,
        g.generation_mw_sum,
        g.generation_mw_avg,
        g.generation_mw_peak,
        g.capacity_match_ratio,
        g.capacity_plausible_ratio,
        l.load_mw_sum,
        l.load_mw_avg,
        l.load_mw_peak,
        l.load_non_positive_ratio,
        n.import_flow_mw_sum,
        n.export_flow_mw_sum,
        n.net_import_mw_sum,
        n.net_import_mw_avg,
        n.net_import_mw_peak
    from generation_daily g
    full outer join load_daily l
        on g.country = l.country
       and g.ds = l.ds
    full outer join net_import_daily n
        on coalesce(g.country, l.country) = n.country
       and coalesce(g.ds, l.ds) = n.ds
)

-- Step 5: Compute energy balance KPIs.
-- Why:
--   This mart is the analytical endpoint for daily adequacy monitoring.
select
    {{ key_country_day('ds', 'country') }} as country_energy_balance_day_key,
    {{ date_key_from_ds('ds') }} as date_key,
    country as country_key,
    country,
    ds,
    {{ year_from_ds('ds') }} as year,
    generation_mw_sum,
    generation_mw_avg,
    generation_mw_peak,
    load_mw_sum,
    load_mw_avg,
    load_mw_peak,
    import_flow_mw_sum,
    export_flow_mw_sum,
    net_import_mw_sum,
    net_import_mw_avg,
    net_import_mw_peak,
    capacity_match_ratio,
    capacity_plausible_ratio,
    load_non_positive_ratio,
    (generation_mw_sum - load_mw_sum) as generation_minus_load_mw,
    (generation_mw_sum + net_import_mw_sum) as supply_with_net_import_mw,
    (generation_mw_sum + net_import_mw_sum - load_mw_sum) as residual_balance_mw,
    {{ safe_divide('net_import_mw_sum', 'load_mw_sum') }} as import_dependency_ratio,
    {{ safe_divide('generation_mw_sum', 'load_mw_sum') }} as self_sufficiency_ratio
from country_daily_joined
