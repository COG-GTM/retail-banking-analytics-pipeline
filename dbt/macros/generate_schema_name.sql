{#-
    The migrated layout maps each Teradata database onto a Snowflake schema of
    the same purpose (ETL_STAGING, DATA_PRODUCTS). dbt's default behaviour
    prefixes custom schemas with the target schema (ETL_STAGING_DATA_PRODUCTS),
    which would not match the DDL contract, so custom schemas are used verbatim.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
