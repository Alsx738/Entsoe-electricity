{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='generation_technology_day_key',
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
)

select
    {{ key_country_day_technology('ds', 'country', 'psr_code') }} as generation_technology_day_key,
    {{ date_key_from_ds('ds') }} as date_key,
    country as country_key,
    psr_code as technology_key,

    country,
    ds,
    {{ year_from_ds('ds') }} as year,
    psr_code,
    energy_type,

    count(*) as row_count,
    sum(mw) as generation_mw_sum,
    avg(mw) as generation_mw_avg,
    max(mw) as generation_mw_peak,
    avg(installed_capacity_mw) as installed_capacity_mw_avg,
    max(installed_capacity_mw) as installed_capacity_mw_max,
    {{ true_ratio('has_capacity_info') }} as capacity_match_ratio,
    {{ true_ratio('is_capacity_plausible') }} as capacity_plausible_ratio,
    avg(mw_normalized_plausible) as mw_normalized_avg,
    max(mw_normalized_plausible) as mw_normalized_peak,
    avg(mw_normalized_pct_plausible) as mw_normalized_pct_avg,
    max(mw_normalized_pct_plausible) as mw_normalized_pct_peak
from src
group by
    country,
    ds,
    year,
    psr_code,
    energy_type
