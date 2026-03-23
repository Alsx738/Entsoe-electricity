-- Cleans raw generation data from GCS and resolves PSR type codes to readable names.
-- Deduplicates by keeping only the latest record per id, handling reprocessing overlaps.
with generation_data as (
    select * from {{ source('gcs_source', 'generation_processed') }}
),

mapping as (
    select * from {{ ref('psr_type_mapping') }}
),

ranked as (
    select
        g.id,
        g.timestamp,
        g.psrType as psr_code,
        m.psr_name as energy_type,
        g.mw,
        g.country,
        g.year,
        row_number() over (
            partition by g.id
            order by g.timestamp desc, g.year desc
        ) as rn
    from generation_data g
    left join mapping m on g.psrType = m.psr_code
)

select
    id,
    timestamp,
    psr_code,
    energy_type,
    mw,
    country,
    year
from ranked
where rn = 1