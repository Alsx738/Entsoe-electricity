{#
  Macro: safe_divide
  Purpose:
    Protect any division by null/zero denominator.
  Why:
    Installed capacity can be missing (no match) or equal to zero for some
    technology/country/year combinations. This macro guarantees stable behavior.
#}
{% macro safe_divide(numerator_col, denominator_col) %}
  case
    when {{ denominator_col }} is null then null
    when {{ denominator_col }} = 0 then null
    else {{ numerator_col }} / {{ denominator_col }}
  end
{% endmacro %}


{#
  Macro: capacity_utilization_pct
  Purpose:
    Convert utilization ratio into percentage.
  Why:
    Keeps the percentage math standardized (ratio * 100) across all models.
#}
{% macro capacity_utilization_pct(numerator_col, denominator_col) %}
  100 * ({{ safe_divide(numerator_col, denominator_col) }})
{% endmacro %}


{#
  Macro: is_capacity_plausible
  Purpose:
    Apply a minimum-threshold quality rule to installed capacity values.
  Why:
    Some country/technology combinations contain tiny or zero capacities that
    are technically matched but not analytically reliable for normalization.
  Config:
    Use dbt var `capacity_plausibility_threshold_mw` to override default threshold.
#}
{% macro is_capacity_plausible(capacity_col) %}
  (
    {{ capacity_col }} is not null
    and {{ capacity_col }} >= {{ var('capacity_plausibility_threshold_mw', 10) }}
  )
{% endmacro %}
