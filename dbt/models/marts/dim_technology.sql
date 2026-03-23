with technologies as (
    select distinct
        cast(psr_code as string) as psr_code,
        cast(energy_type as string) as energy_type
    from {{ ref('int_generation_with_capacity') }}
    where psr_code is not null
)

select
    psr_code as technology_key,
    psr_code,
    energy_type,
    {{ generation_is_renewable('energy_type') }} as is_renewable
from technologies
