{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='generation_country_day_key',
        partition_by={'field': 'ds', 'data_type': 'date'},
        cluster_by=['country'],
        on_schema_change='sync_all_columns'
    )
}}

with src as (
        select *
        from {{ ref('int_generation_with_capacity') }}
        {% if is_incremental() %}
        where {{ incremental_ds_filter('ds') }}
        {% endif %}
),

hourly_country as (
    select
        country,
        timestamp,
        ds,
        year,
        sum(mw) as generation_mw_total,
        sum(case when is_capacity_plausible then mw else 0 end) as generation_mw_plausible_total,
        sum(case when has_capacity_info then installed_capacity_mw else 0 end) as installed_capacity_mw_total,
        sum(case when is_capacity_plausible then installed_capacity_mw else 0 end) as installed_capacity_mw_plausible_total,
        max(case when has_capacity_info then 1 else 0 end) as has_any_capacity_info,
        max(case when is_capacity_plausible then 1 else 0 end) as has_any_capacity_plausible
    from src
    group by
        country,
        timestamp,
        ds,
        year
)

select
    {{ key_country_day('ds', 'country') }} as generation_country_day_key,
    {{ date_key_from_ds('ds') }} as date_key,
    country as country_key,

    country,
    ds,
    {{ year_from_ds('ds') }} as year,

    sum(generation_mw_total) as generation_mw_sum,
    avg(generation_mw_total) as generation_mw_avg,
    max(generation_mw_total) as generation_mw_peak,
    avg(installed_capacity_mw_total) as installed_capacity_mw_avg,
    max(installed_capacity_mw_total) as installed_capacity_mw_peak,
    {{ true_ratio('has_any_capacity_info = 1') }} as capacity_match_ratio,
    {{ true_ratio('has_any_capacity_plausible = 1') }} as capacity_plausible_ratio,
    avg({{ safe_divide('generation_mw_plausible_total', 'installed_capacity_mw_plausible_total') }}) as mw_normalized_avg,
    avg({{ capacity_utilization_pct('generation_mw_plausible_total', 'installed_capacity_mw_plausible_total') }}) as mw_normalized_pct_avg,
    max({{ capacity_utilization_pct('generation_mw_plausible_total', 'installed_capacity_mw_plausible_total') }}) as mw_normalized_pct_peak
from hourly_country
group by
    country,
    ds,
    year
