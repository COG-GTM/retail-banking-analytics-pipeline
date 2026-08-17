{#
    Use the custom schema name verbatim when one is supplied via +schema config.
    This makes models land in the exact Unity Catalog schemas declared in
    dbt_project.yml (silver, gold, ...) instead of the default
    "<target_schema>_<custom>" concatenation.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
