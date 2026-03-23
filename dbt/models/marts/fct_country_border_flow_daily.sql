{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='country_border_flow_day_key',
    partition_by={'field': 'ds', 'data_type': 'date'},
    cluster_by=['country'],
    on_schema_change='sync_all_columns'
  )
}}

-- Step 1: Read enriched crossborder flow rows.
-- Why:
--   The intermediate model already standardizes direction and signed flow measure.
with source_crossborder_flows as (
    select *
    from {{ ref('int_crossborder_flow_country_time_enriched') }}
    {% if is_incremental() %}
      where {{ incremental_ds_filter('ds') }}
    {% endif %}
)

-- Step 2: Aggregate at country-border-direction-day grain.
-- Why:
--   This is the most useful daily grain for border operations analysis.
select
    {{ key_country_border_direction_day('ds', 'country', 'border_country', 'flow_direction') }} as country_border_flow_day_key,
    {{ date_key_from_ds('ds') }} as date_key,
    country as country_key,
    country,
    border_country,
    flow_direction,
    ds,
    {{ year_from_ds('ds') }} as year,
    count(*) as row_count,
    sum(flow_mw) as flow_mw_sum,
    avg(flow_mw) as flow_mw_avg,
    max(flow_mw) as flow_mw_peak,
    sum(signed_flow_mw) as signed_flow_mw_sum,
    {{ true_ratio('is_flow_direction_unmapped') }} as flow_direction_unmapped_ratio,
    {{ true_ratio('is_flow_missing') }} as flow_missing_ratio
from source_crossborder_flows
group by
    country,
    border_country,
    flow_direction,
    ds
