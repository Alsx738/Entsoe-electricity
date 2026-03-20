select
  country,
  ds,
  load_mw
from {{ ref('stg_entsoe_load') }}
