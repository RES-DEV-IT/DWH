{% macro is_priority_valid(value) %}
    {{ return(value ~ " IN ('urgent', 'high', 'normal')") }}
{% endmacro %}
