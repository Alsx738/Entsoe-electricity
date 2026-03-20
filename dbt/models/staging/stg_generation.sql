with generation_data as (
    select * from {{ source('gcs_source', 'generation_processed') }}
),

mapping as (
    select * from {{ ref('psr_type_mapping') }}
)

select
    g.id,
    g.timestamp,
    g.psrType as psr_code,
    m.psr_name as energy_type, 
    g.mw,
    g.country,
    g.year
from generation_data g
left join mapping m on g.psrType = m.psr_code