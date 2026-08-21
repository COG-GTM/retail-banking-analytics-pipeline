{#
    Replacement for the BTEQ `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG ...` step.
    Used as a post-hook so that every model records an audit row with the number
    of rows it produced. A zero-row build is recorded as ZERO_ROWS; the
    accompanying not-empty test fails the run.
#}
{% macro log_etl_run(job_name, step_name='FULL_LOAD') %}
    insert into {{ target.database }}.{{ var('etl_staging_schema') }}.ETL_RUN_LOG
        (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS)
    select
        '{{ job_name }}',
        '{{ step_name }}',
        case when count(*) = 0 then 'ZERO_ROWS' else 'SUCCESS' end,
        count(*),
        current_timestamp(),
        current_timestamp()
    from {{ this }}
{% endmacro %}
