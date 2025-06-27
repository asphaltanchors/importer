/*
ABOUTME: Macro for calculating growth percentages with consistent logic across all models.
ABOUTME: Handles division by zero and NULL values safely, returns 0 for invalid calculations.
*/

{% macro calculate_growth_percentage(current_value, previous_value) %}
  CASE 
    WHEN COALESCE({{ previous_value }}, 0) > 0 
    THEN ROUND(CAST(((COALESCE({{ current_value }}, 0) - COALESCE({{ previous_value }}, 0)) / {{ previous_value }}) * 100 AS NUMERIC), 2)
    ELSE 0 
  END
{% endmacro %}