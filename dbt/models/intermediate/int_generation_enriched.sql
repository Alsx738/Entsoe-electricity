-- Adds time dimensions and analytical flags to staged generation data.
-- Classifies each technology as renewable, non-renewable, or ambiguous.
-- Null mw values are coalesced to 0; the original raw value is preserved in mw_raw.
with src as (
    select * from {{ ref('stg_generation') }}
)

select
    id,
    timestamp,
    psr_code,
    energy_type,
    {{ time_dimension_columns('timestamp', include_ds=false) }},
    {{ generation_is_renewable('energy_type') }} as is_renewable,
    mw as mw_raw,
    coalesce(mw, 0) as mw,
    case
        when mw is null then true
        when mw <= 0 then true
        else false
    end as is_outlier,
    country,
    year
from src
