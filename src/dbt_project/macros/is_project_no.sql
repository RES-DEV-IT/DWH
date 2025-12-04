{% macro is_project_no(value) %}
    {{ return(value ~ " ~ '^P\d+$'") }}
{% endmacro %}
