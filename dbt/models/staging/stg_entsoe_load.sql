select
  cast(country as string) as country,
  cast(null as date) as ds,
  cast(null as float64) as load_mw
from {{ source('gcs_source', 'load_processed') }}
where false
