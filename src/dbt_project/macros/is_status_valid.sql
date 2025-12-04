{% macro is_status_valid(value) %}
    {{ return(value ~ " IN ('live', 'pending', 'finished', 'hold')") }}
{% endmacro %}
