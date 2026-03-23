with generation_dates as (
    select cast(timestamp as date) as ds
    from {{ ref('stg_generation') }}
    where timestamp is not null
),

installed_capacity_dates as (
    select cast(capacity_timestamp as date) as ds
    from {{ ref('stg_installed_capacity') }}
    where capacity_timestamp is not null
),

load_dates as (
    select cast(timestamp as date) as ds
    from {{ ref('stg_load') }}
    where timestamp is not null
),

prices_dates as (
    select cast(timestamp as date) as ds
    from {{ ref('stg_prices') }}
    where timestamp is not null
),

flows_dates as (
    select cast(timestamp as date) as ds
    from {{ ref('stg_crossborder_flows') }}
    where timestamp is not null
),

all_dates as (
    select ds from generation_dates
    union all
    select ds from installed_capacity_dates
    union all
    select ds from load_dates
    union all
    select ds from prices_dates
    union all
    select ds from flows_dates
),

calendar_base as (
    select distinct ds
    from all_dates
    where ds is not null
)

select
    cast(format_date('%Y%m%d', ds) as int64) as date_key,
    ds,
    extract(year from ds) as year,
    extract(month from ds) as month,
    extract(quarter from ds) as quarter,
    extract(dayofweek from ds) as day_of_week,
    {{ generation_is_weekend('ds') }} as is_weekend,
    {{ generation_season('ds') }} as season
from calendar_base
