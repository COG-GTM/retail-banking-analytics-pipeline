{#
    Shared Snowflake run-log mechanism, replacing the BTEQ
    "INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG" audit step.

    Used as a post-hook on migrated staging models. Writes one row per model
    run with job name, step name, status, row count and run duration.
#}
{% macro log_etl_run(job_name, step_name, status='SUCCESS') %}

    insert into {{ var('etl_run_log_relation', target.database ~ '.' ~ target.schema ~ '.ETL_RUN_LOG') }}
    (
        JOB_NAME,
        STEP_NAME,
        STATUS,
        ROW_COUNT,
        START_TS,
        END_TS,
        DURATION_SEC
    )
    select
        '{{ job_name }}',
        '{{ step_name }}',
        '{{ status }}',
        count(*),
        '{{ run_started_at }}'::timestamp_ntz,
        current_timestamp()::timestamp_ntz,
        datediff('second', '{{ run_started_at }}'::timestamp_ntz, current_timestamp()::timestamp_ntz)
    from {{ this }}

{% endmacro %}
