{#-
    Audit logging for the dbt pipeline, replacing the hand-written
    `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG` statements that each BTEQ script
    carried. dbt's own run artifacts (target/run_results.json, manifest.json)
    are the primary audit record; this on-run-end hook mirrors one row per node
    into ETL_RUN_LOG so existing operational reports keep working.

    Set the `enable_etl_run_log` var to false to run without the audit table.
-#}

{% macro log_run_results(results) %}

    {%- set noop = 'select 1 as NO_OP from (select 1 as ONE) d' -%}

    {%- if not execute or not var('enable_etl_run_log') -%}
        {{ return(noop) }}
    {%- endif -%}

    {%- set rows = [] -%}
    {%- for res in results -%}
        {%- if res.node.resource_type in ('model', 'seed', 'test') -%}
            {%- set row_count = res.adapter_response.get('rows_affected', 0)
                                if res.adapter_response else 0 -%}
            {%- do rows.append(
                "select " ~
                "'" ~ res.node.name | replace("'", "''") ~ "' as JOB_NAME, " ~
                "'" ~ res.node.resource_type | upper ~ "' as STEP_NAME, " ~
                "'" ~ res.status | upper | replace("'", "''") ~ "' as STATUS, " ~
                "cast(" ~ (row_count or 0) ~ " as bigint) as ROW_COUNT, " ~
                "cast('" ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S') ~ "' as timestamp(6)) as START_TS, " ~
                "current_timestamp(6) as END_TS"
            ) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if rows | length == 0 -%}
        {{ return(noop) }}
    {%- endif -%}

    insert into {{ var('etl_run_log_relation') }}
        (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS)
    {{ rows | join('\n    union all\n    ') }}

{% endmacro %}
