{% macro is_project_integer(value) %}
    {{ return(value ~ " ~ '^\\d+$'") }}
{% endmacro %}
