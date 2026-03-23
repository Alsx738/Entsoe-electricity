with source_data as (
    select * from {{ source('gcs_source', 'crossborder_flows_processed') }}
),

deduplicated as (
    select
        cast(id as string) as id,
        cast(timestamp as timestamp) as timestamp,
        cast(main_country as string) as main_country,
        cast(border_country as string) as border_country,
        cast(direction as string) as direction,
        cast(mw as float64) as flow_mw,
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
    main_country,
    border_country,
    direction,
    flow_mw,
    year
from deduplicated
where rn = 1
