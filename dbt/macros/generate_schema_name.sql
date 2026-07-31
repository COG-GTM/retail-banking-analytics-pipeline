{#-
    Teradata has a single-level namespace: a "schema" is a database.
    dbt's default behaviour prefixes custom schemas with the target schema
    (ETL_STAGING_DB_DATA_PRODUCTS_DB), which would not match the existing
    DDL contract, so custom schemas are used verbatim instead.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
