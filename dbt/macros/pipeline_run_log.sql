{#
    Shared Snowflake run-log mechanism replacing the BTEQ
    "INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG" audit step.
#}

{% macro run_log_relation() %}
    {{ return(var('run_log_relation')) }}
{% endmacro %}

{% macro create_run_log_if_not_exists() %}
    {% set ddl %}
        create table if not exists {{ run_log_relation() }} (
            JOB_NAME    varchar(100),
            STEP_NAME   varchar(100),
            STATUS      varchar(20),
            ROW_COUNT   number(18, 0),
            START_TS    timestamp_ntz(6),
            END_TS      timestamp_ntz(6),
            DURATION_S  number(18, 3),
            INVOCATION_ID varchar(64)
        )
    {% endset %}
    {% do run_query(ddl) %}
{% endmacro %}

{#
    Fail-fast replacement for ".IF ACTIVITYCOUNT = 0 THEN .EXIT 99".
    Raises at run time so the dbt run fails and downstream models are skipped
    instead of consuming an empty table.
#}
{% macro assert_model_not_empty() %}
    {% if execute %}
        {% set row_count = run_query('select count(*) as n from ' ~ this)
                             .columns[0].values()[0] | int %}
        {% if row_count == 0 %}
            {% do log_pipeline_run_row(this.identifier, 'ROW_COUNT_CHECK', 'FAILED', 0) %}
            {% do exceptions.raise_compiler_error(
                this.identifier ~ ' produced 0 rows; failing the run instead of publishing an empty table.'
            ) %}
        {% endif %}
    {% endif %}
    {{ return('select 1') }}
{% endmacro %}

{% macro log_pipeline_run_row(job_name, step_name, status, row_count) %}
    {% do create_run_log_if_not_exists() %}
    {% set insert_sql %}
        insert into {{ run_log_relation() }}
            (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, DURATION_S, INVOCATION_ID)
        select
            '{{ job_name }}',
            '{{ step_name }}',
            '{{ status }}',
            {{ row_count }},
            '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}'::timestamp_ntz(6),
            current_timestamp()::timestamp_ntz(6),
            datediff(
                millisecond,
                '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}'::timestamp_ntz(6),
                current_timestamp()::timestamp_ntz(6)
            ) / 1000.0,
            '{{ invocation_id }}'
    {% endset %}
    {% do run_query(insert_sql) %}
{% endmacro %}

{% macro log_pipeline_run(job_name, step_name) %}
    {% if execute %}
        {% set row_count = run_query('select count(*) as n from ' ~ this)
                             .columns[0].values()[0] | int %}
        {% do log_pipeline_run_row(job_name, step_name, 'SUCCESS', row_count) %}
    {% endif %}
    {{ return('select 1') }}
{% endmacro %}
