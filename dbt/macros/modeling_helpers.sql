{#
  Macro: date_key_from_ds
  Purpose:
    Build a canonical integer date key in YYYYMMDD format.
#}
{% macro date_key_from_ds(ds_col) %}
  cast(format_date('%Y%m%d', {{ ds_col }}) as int64)
{% endmacro %}


{#
  Macro: key_country_day
  Purpose:
    Build deterministic country-day key for daily country grain facts/marts.
#}
{% macro key_country_day(ds_col, country_col) %}
  concat(cast(format_date('%Y%m%d', {{ ds_col }}) as string), '|', {{ country_col }})
{% endmacro %}


{#
  Macro: key_country_day_technology
  Purpose:
    Build deterministic country-day-technology key.
#}
{% macro key_country_day_technology(ds_col, country_col, technology_col) %}
  concat(
    cast(format_date('%Y%m%d', {{ ds_col }}) as string),
    '|',
    {{ country_col }},
    '|',
    {{ technology_col }}
  )
{% endmacro %}


{#
  Macro: key_country_border_direction_day
  Purpose:
    Build deterministic key for country-border-direction-day grain.
#}
{% macro key_country_border_direction_day(ds_col, country_col, border_col, direction_col) %}
  concat(
    cast(format_date('%Y%m%d', {{ ds_col }}) as string),
    '|',
    {{ country_col }},
    '|',
    {{ border_col }},
    '|',
    {{ direction_col }}
  )
{% endmacro %}


{#
  Macro: incremental_ds_filter
  Purpose:
    Standardize incremental lookback filter on a date expression/column.
  Config:
    Uses var incremental_lookback_days (default 3).
    Supports var execution_date for explicit run boundaries.
#}
{% macro incremental_ds_filter(ds_expression) %}
  {% if var('execution_date', none) is not none %}
    {{ ds_expression }} >= date_sub(date('{{ var("execution_date") }}'), interval 1 day)
    and {{ ds_expression }} <= date('{{ var("execution_date") }}')
  {% else %}
    {{ ds_expression }} >= date_sub(
      coalesce((select max(ds) from {{ this }}), date('1900-01-01')),
      interval {{ var('incremental_lookback_days', 3) }} day
    )
  {% endif %}
{% endmacro %}


{#
  Macro: year_from_ds
  Purpose:
    Standardize year extraction from daily date column.
#}
{% macro year_from_ds(ds_col) %}
  extract(year from {{ ds_col }})
{% endmacro %}


{#
  Macro: true_ratio
  Purpose:
    Compute share of rows where a boolean expression is true.
#}
{% macro true_ratio(boolean_expression) %}
  avg(case when {{ boolean_expression }} then 1.0 else 0.0 end)
{% endmacro %}


{#
  Macro: time_dimension_columns
  Purpose:
    Emit reusable time-dimension columns from a timestamp expression.
  Args:
    timestamp_expression: column/expression to derive time dimensions from.
    include_ds: if true, includes ds as cast(timestamp as date).
#}
{% macro time_dimension_columns(timestamp_expression, include_ds=true) %}
  {% if include_ds %}
  cast({{ timestamp_expression }} as date) as ds,
  {% endif %}
  extract(month from {{ timestamp_expression }}) as month,
  extract(hour from {{ timestamp_expression }}) as hour,
  extract(dayofweek from {{ timestamp_expression }}) as day_of_week,
  extract(quarter from {{ timestamp_expression }}) as quarter,
  {{ generation_is_weekend(timestamp_expression) }} as is_weekend,
  {{ generation_season(timestamp_expression) }} as season
{% endmacro %}
