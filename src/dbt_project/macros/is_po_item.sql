{% macro is_po_item(value) %}
    {{ return(value ~ " ~ '^\d+\.\d+$'") }}
{% endmacro %}
