-- Cleans raw installed capacity data from GCS and resolves PSR type codes to readable names.
-- Zero-capacity rows are kept intentionally (e.g. a country that has decommissioned a technology).
-- Deduplicates by keeping only the latest record per id.
with source_data as (
    select * from {{ source('gcs_source', 'installed_capacity_processed') }}
),

mapping as (
    select * from {{ ref('psr_type_mapping') }}
),

cleaned as (
    select
        cast(s.id as string) as id,
        cast(s.timestamp as timestamp) as capacity_timestamp,
        cast(s.country as string) as country,
        cast(s.psrType as string) as psr_code,
        m.psr_name as energy_type,
        cast(s.installed_capacity_mw as float64) as installed_capacity_mw,
        cast(s.year as int64) as year,
        true as has_capacity_info
    from source_data s
    left join mapping m
        on cast(s.psrType as string) = m.psr_code
    where cast(s.installed_capacity_mw as float64) is not null
),

deduplicated as (
    select
        id,
        capacity_timestamp,
        country,
        psr_code,
        energy_type,
        installed_capacity_mw,
        year,
        has_capacity_info,
        row_number() over (
            partition by id
            order by capacity_timestamp desc, year desc
        ) as rn
    from cleaned
)

select
    id,
    capacity_timestamp,
    country,
    psr_code,
    energy_type,
    installed_capacity_mw,
    year,
    has_capacity_info
from deduplicated
where rn = 1
