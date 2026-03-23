with source_data as (
    select * from {{ source('gcs_source', 'load_processed') }}
),

deduplicated as (
    select
        cast(id as string) as id,
        cast(timestamp as timestamp) as timestamp,
        cast(country as string) as country,
        cast(mw as float64) as load_mw,
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
    load_mw,
    year
from deduplicated
where rn = 1
