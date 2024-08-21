{#
Override the built-in `generate_schema_name` macro so custom schemas are used verbatim for production targets,
and custom database names are incorporated into the schema name for non-production targets.

Some examples:

    For a production target with target database `analytics` and target schema `misc`:
        - Models with the custom schema `core` would go in the schema `analytics.core`.
        - Models with the custom database `foo` and custom schema `bar` would go in the schema `foo.bar`.
        - Models without a custom database or schema would go in the schema `analytics.misc`.

    For a non-production target with target database `analytics_dev` and target schema `alice`:
        - Models with the custom schema `core` would go in the schema `analytics_dev.alice_core`.
        - Models with the custom database `foo` and custom schema `bar` would go in the schema `analytics_dev.alice_foo_bar`.
        - Models without a custom database or schema would go in the schema `analytics_dev.alice`.

https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas/
https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-databases/
#}
{% macro generate_schema_name(custom_schema_name, node) %}

    {% set default_schema = target.schema %}

    {% if target.name in var('production_target_names') %}

        {% if custom_schema_name is not none %}
            {{ return(custom_schema_name | trim) }}
        {% else %}
            {{ return(default_schema) }}
        {% endif %}

    {% else %}

        {% set generated_schema_name = default_schema %}

        {% if node is not none and node.config is not none and node.config.database is not none %}
            {% set generated_schema_name = generated_schema_name ~ '_' ~ node.config.database %}
        {% endif %}

        {% if custom_schema_name is not none %}
            {% set generated_schema_name = generated_schema_name ~ '_' ~ (custom_schema_name | trim) %}
        {% endif %}

        {{ return(generated_schema_name) }}

    {% endif %}

{% endmacro %}
