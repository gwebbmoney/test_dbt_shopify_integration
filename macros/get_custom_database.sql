{% macro generate_database_name(custom_database_name, node) -%}
    {{ generate_database_name_for_env(custom_database_name, node) }}
{%- endmacro %}