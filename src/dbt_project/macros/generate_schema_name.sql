{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Если в модели задан schema=..., верни его без префикса #}
    {% if custom_schema_name is not none %}
        {{ custom_schema_name | lower }}
    {% else %}
        {{ target.schema | lower }}
    {% endif %}
{%- endmacro %}
