{% macro calculate_capacity_factor(generation_col, capacity_col, hours) %}
    ROUND(
        {{ generation_col }} / NULLIF({{ capacity_col }} * {{ hours }}, 0),
        4
    )
{% endmacro %}
