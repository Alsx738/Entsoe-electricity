{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        partition_by={'field': 'ds', 'data_type': 'date'},
        cluster_by=['country'],
        on_schema_change='sync_all_columns'
    )
}}

-- Joins enriched generation rows with annual installed capacity on (country, psr_code, year).
-- Uses a left join so generation records are never dropped when capacity data is missing.
-- Computes capacity utilisation ratios; normalisation is suppressed (set to null) when
-- installed capacity is below the plausibility threshold to avoid misleading metrics.
with generation as (
        select *
        from {{ ref('int_generation_enriched') }}
        {% if is_incremental() %}
        where {{ incremental_ds_filter('cast(timestamp as date)') }}
        {% endif %}
),

capacity as (
    select * from {{ ref('stg_installed_capacity') }}
),

joined as (
    select
        g.id,
        g.timestamp,
        cast(g.timestamp as date) as ds,
        g.country,
        g.year,
        g.psr_code,
        g.energy_type,
        g.is_renewable,
        g.is_outlier,
        g.month,
        g.hour,
        g.day_of_week,
        g.quarter,
        g.is_weekend,
        g.season,
        g.mw,
        g.mw_raw,
        c.installed_capacity_mw,
        case
            when c.installed_capacity_mw is null then false
            else true
        end as has_capacity_info,
        {{ is_capacity_plausible('c.installed_capacity_mw') }} as is_capacity_plausible,
        {{ safe_divide('g.mw', 'c.installed_capacity_mw') }} as mw_normalized,
        {{ capacity_utilization_pct('g.mw', 'c.installed_capacity_mw') }} as mw_normalized_pct,
        case
            when {{ is_capacity_plausible('c.installed_capacity_mw') }}
                then {{ safe_divide('g.mw', 'c.installed_capacity_mw') }}
            else null
        end as mw_normalized_plausible,
        case
            when {{ is_capacity_plausible('c.installed_capacity_mw') }}
                then {{ capacity_utilization_pct('g.mw', 'c.installed_capacity_mw') }}
            else null
        end as mw_normalized_pct_plausible
    from generation g
    left join capacity c
        on g.country = c.country
       and g.psr_code = c.psr_code
       and g.year = c.year
)

select
    id,
    timestamp,
    ds,
    country,
    year,
    psr_code,
    energy_type,
    is_renewable,
    is_outlier,
    month,
    hour,
    day_of_week,
    quarter,
    is_weekend,
    season,
    mw,
    mw_raw,
    installed_capacity_mw,
    has_capacity_info,
    is_capacity_plausible,
    mw_normalized,
    mw_normalized_pct,
    mw_normalized_plausible,
    mw_normalized_pct_plausible
from joined
