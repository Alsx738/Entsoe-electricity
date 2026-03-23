{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='country_load_day_key',
    partition_by={'field': 'ds', 'data_type': 'date'},
    cluster_by=['country'],
    on_schema_change='sync_all_columns'
  )
}}

-- Step 1: Read enriched load rows.
-- Why:
--   Intermediate model already includes normalized dates and quality flags.
with source_load as (
    select *
    from {{ ref('int_load_country_time_enriched') }}
    {% if is_incremental() %}
      where {{ incremental_ds_filter('ds') }}
    {% endif %}
)

-- Step 2: Aggregate at country-day grain.
-- Why:
--   This is the canonical grain for daily demand analytics.
select
    {{ key_country_day('ds', 'country') }} as country_load_day_key,
    {{ date_key_from_ds('ds') }} as date_key,
    country as country_key,
    country,
    ds,
    {{ year_from_ds('ds') }} as year,
    count(*) as row_count,
    sum(load_mw) as load_mw_sum,
    avg(load_mw) as load_mw_avg,
    max(load_mw) as load_mw_peak,
    min(load_mw) as load_mw_min,
    {{ true_ratio('is_load_non_positive') }} as load_non_positive_ratio
from source_load
group by
    country,
    ds
