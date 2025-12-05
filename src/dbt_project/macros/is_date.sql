{% macro is_date(value) %}
    {{ return(value ~ " IN ('live', 'pending', 'finished', 'hold')") }}
{% endmacro %}
