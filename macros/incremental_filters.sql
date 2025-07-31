{% macro incremental_day_filter(column, days=40) %}
  {% if is_incremental() %}
    AND {{ column }} >= CURRENT_DATE() - {{ days }}
  {% endif %}
{% endmacro %}

{% macro incremental_quarter_filter(column, days=40) %}
  {% if is_incremental() %}
    AND {{ column }} >= DATE_TRUNC(CURRENT_DATE - {{ days }} , QUARTER)
  {% endif %}
{% endmacro %}

{% macro incremental_isoweek_filter(column, days=40) %}
  {% if is_incremental() %}
    AND {{ column }} >= DATE_TRUNC(CURRENT_DATE - {{ days }} , ISOWEEK)
  {% endif %}
{% endmacro %}
