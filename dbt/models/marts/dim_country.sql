with generation_countries as (
    select cast(country as string) as country
    from {{ ref('stg_generation') }}
    where country is not null
),

installed_capacity_countries as (
    select cast(country as string) as country
    from {{ ref('stg_installed_capacity') }}
    where country is not null
),

load_countries as (
    select cast(country as string) as country
    from {{ ref('stg_load') }}
    where country is not null
),

prices_countries as (
    select cast(country as string) as country
    from {{ ref('stg_prices') }}
    where country is not null
),

flows_main_countries as (
    select cast(main_country as string) as country
    from {{ ref('stg_crossborder_flows') }}
    where main_country is not null
),

flows_border_countries as (
    select cast(border_country as string) as country
    from {{ ref('stg_crossborder_flows') }}
    where border_country is not null
),

all_countries as (
    select country from generation_countries
    union all
    select country from installed_capacity_countries
    union all
    select country from load_countries
    union all
    select country from prices_countries
    union all
    select country from flows_main_countries
    union all
    select country from flows_border_countries
),

countries as (
    select distinct country
    from all_countries
),

-- Join with seed to get human-readable names and region grouping.
-- Left join so unknown country codes still appear rather than being silently lost.
country_names as (
    select * from {{ ref('country_names') }}
)

select
    c.country as country_key,
    c.country,
    coalesce(n.country_name, c.country) as country_name,  -- fallback to code if not in seed
    n.region
from countries c
left join country_names n on c.country = n.country_code

