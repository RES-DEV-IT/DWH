{% macro is_positive_integer(value) %}
    {{ return(value ~ " ~ '^\\d+$'") }}
{% endmacro %}
