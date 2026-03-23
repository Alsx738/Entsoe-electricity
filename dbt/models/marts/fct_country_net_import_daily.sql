{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='country_net_import_day_key',
    partition_by={'field': 'ds', 'data_type': 'date'},
    cluster_by=['country'],
    on_schema_change='sync_all_columns'
  )
}}

-- Step 1: Read enriched crossborder flow rows.
-- Why:
--   Signed flow measure is required to compute net import/export daily balance.
with source_crossborder_flows as (
    select *
    from {{ ref('int_crossborder_flow_country_time_enriched') }}
    {% if is_incremental() %}
      where {{ incremental_ds_filter('ds') }}
    {% endif %}
)

-- Step 2: Aggregate at country-day grain using signed flow convention.
-- Why:
--   A single daily KPI is needed for country-level energy balance marts.
select
    {{ key_country_day('ds', 'country') }} as country_net_import_day_key,
    {{ date_key_from_ds('ds') }} as date_key,
    country as country_key,
    country,
    ds,
    {{ year_from_ds('ds') }} as year,
    count(*) as row_count,
    sum(case when flow_direction = 'import' then flow_mw else 0 end) as import_flow_mw_sum,
    sum(case when flow_direction = 'export' then flow_mw else 0 end) as export_flow_mw_sum,
    sum(signed_flow_mw) as net_import_mw_sum,
    avg(signed_flow_mw) as net_import_mw_avg,
    max(signed_flow_mw) as net_import_mw_peak,
    {{ true_ratio('is_flow_direction_unmapped') }} as flow_direction_unmapped_ratio
from source_crossborder_flows
group by
    country,
    ds
