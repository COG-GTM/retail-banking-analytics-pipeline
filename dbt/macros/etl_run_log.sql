{#-
    Snowflake replacement for the BTEQ control-flow constructs used by the
    staging jobs:

      * .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE  -> dbt run failure semantics
                                                    (any failing statement
                                                     aborts the invocation)
      * .IF ACTIVITYCOUNT = 0 THEN .EXIT 99      -> assert_not_empty_and_log()
      * INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG   -> log_etl_run()

    Provisional: TICKET-03 owns the shared run-log mechanism. This macro
    implements the same contract (job name, step name, status, row count,
    start/end timestamps) and can be replaced by the shared version without
    touching the models, which only reference assert_not_empty_and_log().
-#}

{% macro etl_run_log_relation() -%}
    {{ target.database }}.{{ env_var('SF_SCHEMA_STAGING', 'ETL_STAGING') }}.ETL_RUN_LOG
{%- endmacro %}


{% macro create_etl_run_log_if_not_exists() -%}
    {% set ddl %}
        create table if not exists {{ etl_run_log_relation() }} (
            job_name    varchar(100),
            step_name   varchar(100),
            status      varchar(20),
            row_count   number(18, 0),
            start_ts    timestamp_ntz,
            end_ts      timestamp_ntz
        )
    {% endset %}
    {% do run_query(ddl) %}
{%- endmacro %}


{% macro log_etl_run(job_name, step_name, status, row_count, start_ts) -%}
    {% do create_etl_run_log_if_not_exists() %}
    {% set insert_sql %}
        insert into {{ etl_run_log_relation() }}
            (job_name, step_name, status, row_count, start_ts, end_ts)
        select
            '{{ job_name }}',
            '{{ step_name }}',
            '{{ status }}',
            {{ row_count }},
            '{{ start_ts }}'::timestamp_ntz,
            current_timestamp()::timestamp_ntz
    {% endset %}
    {% do run_query(insert_sql) %}
{%- endmacro %}


{#-
    Post-hook helper: counts the rows just written, records the outcome in the
    run log and fails the run when the model produced no rows (the BTEQ
    ".EXIT 99" behaviour). Returns a no-op statement because dbt executes the
    rendered hook body as SQL.
-#}
{% macro assert_not_empty_and_log(relation, job_name, step_name) -%}
    {%- if execute -%}
        {%- set start_ts = run_started_at.strftime('%Y-%m-%d %H:%M:%S.%f') -%}
        {%- set count_result = run_query('select count(*) as row_count from ' ~ relation) -%}
        {%- set row_count = count_result.columns[0].values()[0] | int -%}
        {%- if row_count == 0 -%}
            {%- do log_etl_run(job_name, step_name, 'FAILED', 0, start_ts) -%}
            {%- do exceptions.raise_compiler_error(
                    'Zero-row result for ' ~ relation ~ ' (' ~ job_name ~ '/' ~ step_name ~ '); failing the run.'
                ) -%}
        {%- endif -%}
        {%- do log_etl_run(job_name, step_name, 'SUCCESS', row_count, start_ts) -%}
    {%- endif -%}
    select 1
{%- endmacro %}
