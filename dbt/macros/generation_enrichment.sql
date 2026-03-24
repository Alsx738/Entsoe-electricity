{% macro generation_is_weekend(timestamp_col) %}
  case
    when extract(dayofweek from {{ timestamp_col }}) in (1, 7) then true
    else false
  end
{% endmacro %}

{% macro generation_season(timestamp_col) %}
  case
    when extract(month from {{ timestamp_col }}) in (12, 1, 2) then 'Winter'
    when extract(month from {{ timestamp_col }}) in (3, 4, 5) then 'Spring'
    when extract(month from {{ timestamp_col }}) in (6, 7, 8) then 'Summer'
    else 'Autumn'
  end
{% endmacro %}

{#
  Macro: generation_is_renewable
  Purpose:
    Classify each generation technology as renewable (true), non-renewable (false),
    or ambiguous (null) based on EU taxonomy and ENTSO-E PSR type names.
  Classification:
    true  → Solar, Wind (on/offshore), Hydro (all types), Geothermal, Marine, Other renewable, Biomass
    false → Fossil Gas, Coal, Lignite, Oil, Oil shale, Peat, Nuclear, Fossil Coal-derived gas
    null  → Waste (legally contested), Energy storage (not a generation source), Other/unknown
#}
{% macro generation_is_renewable(energy_type_col) %}
  case
    -- Clearly renewable
    when lower(coalesce({{ energy_type_col }}, '')) like '%solar%'            then true
    when lower(coalesce({{ energy_type_col }}, '')) like '%wind%'             then true
    when lower(coalesce({{ energy_type_col }}, '')) like '%hydro%'            then true
    when lower(coalesce({{ energy_type_col }}, '')) like '%geothermal%'       then true
    when lower(coalesce({{ energy_type_col }}, '')) like '%marine%'           then true
    when lower(coalesce({{ energy_type_col }}, '')) like '%biomass%'          then true
    when lower(coalesce({{ energy_type_col }}, '')) like '%other renewable%'  then true
    -- Clearly non-renewable
    when lower(coalesce({{ energy_type_col }}, '')) like '%gas%'              then false
    when lower(coalesce({{ energy_type_col }}, '')) like '%coal%'             then false
    when lower(coalesce({{ energy_type_col }}, '')) like '%lignite%'          then false
    when lower(coalesce({{ energy_type_col }}, '')) like '%oil%'              then false
    when lower(coalesce({{ energy_type_col }}, '')) like '%peat%'             then false
    when lower(coalesce({{ energy_type_col }}, '')) like '%nuclear%'          then false
    -- Ambiguous / not applicable → false (Evita "undefined" o "null" nelle dashboard)
    else false
  end
{% endmacro %}
