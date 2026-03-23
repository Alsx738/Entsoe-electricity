with source_data as (
    select * from {{ source('gcs_source', 'prices_processed') }}
),

deduplicated as (
    select
        cast(id as string) as id,
        cast(timestamp as timestamp) as timestamp,
        cast(country as string) as country,
        cast(price_eur_mwh as float64) as price_eur_mwh,
        cast(year as int64) as year,
        row_number() over (
            partition by id
            order by cast(timestamp as timestamp) desc, cast(year as int64) desc
        ) as rn
    from source_data
    where id is not null
)

select
    id,
    timestamp,
    country,
    price_eur_mwh,
    year
from deduplicated
where rn = 1
