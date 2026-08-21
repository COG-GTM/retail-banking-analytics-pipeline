/*
    Reconciliation of the Snowflake dbt model against the legacy Teradata
    ETL_STAGING_DB.STG_TXN_SUMMARY output for the same lookback window.

    Run the legacy side through the Teradata connection, land it in Snowflake as
    LEGACY_STG_TXN_SUMMARY, then compare row counts and per-column sums:

        dbt compile --select recon_stg_txn_summary
*/
with snowflake_side as (
    select
        count(*)                    as ROW_COUNT,
        count(distinct CUSTOMER_ID) as DISTINCT_CUSTOMERS,
        sum(TXN_COUNT_TOTAL)        as TXN_COUNT_TOTAL,
        sum(TXN_COUNT_DEBIT)        as TXN_COUNT_DEBIT,
        sum(TXN_COUNT_CREDIT)       as TXN_COUNT_CREDIT,
        sum(TXN_COUNT_FEE)          as TXN_COUNT_FEE,
        sum(AMT_TOTAL_DEBIT)        as AMT_TOTAL_DEBIT,
        sum(AMT_TOTAL_CREDIT)       as AMT_TOTAL_CREDIT,
        sum(AMT_TOTAL_FEES)         as AMT_TOTAL_FEES,
        sum(DISTINCT_MERCHANTS)     as DISTINCT_MERCHANTS
    from {{ ref('stg_txn_summary') }}
),

teradata_side as (
    select
        count(*)                    as ROW_COUNT,
        count(distinct CUSTOMER_ID) as DISTINCT_CUSTOMERS,
        sum(TXN_COUNT_TOTAL)        as TXN_COUNT_TOTAL,
        sum(TXN_COUNT_DEBIT)        as TXN_COUNT_DEBIT,
        sum(TXN_COUNT_CREDIT)       as TXN_COUNT_CREDIT,
        sum(TXN_COUNT_FEE)          as TXN_COUNT_FEE,
        sum(AMT_TOTAL_DEBIT)        as AMT_TOTAL_DEBIT,
        sum(AMT_TOTAL_CREDIT)       as AMT_TOTAL_CREDIT,
        sum(AMT_TOTAL_FEES)         as AMT_TOTAL_FEES,
        sum(DISTINCT_MERCHANTS)     as DISTINCT_MERCHANTS
    from {{ target.database }}.{{ var('etl_staging_schema') }}.LEGACY_STG_TXN_SUMMARY
)

select
    'ROW_COUNT'          as METRIC, s.ROW_COUNT          as SNOWFLAKE_VALUE, t.ROW_COUNT          as TERADATA_VALUE from snowflake_side s cross join teradata_side t
union all select 'DISTINCT_CUSTOMERS', s.DISTINCT_CUSTOMERS, t.DISTINCT_CUSTOMERS from snowflake_side s cross join teradata_side t
union all select 'TXN_COUNT_TOTAL',    s.TXN_COUNT_TOTAL,    t.TXN_COUNT_TOTAL    from snowflake_side s cross join teradata_side t
union all select 'TXN_COUNT_DEBIT',    s.TXN_COUNT_DEBIT,    t.TXN_COUNT_DEBIT    from snowflake_side s cross join teradata_side t
union all select 'TXN_COUNT_CREDIT',   s.TXN_COUNT_CREDIT,   t.TXN_COUNT_CREDIT   from snowflake_side s cross join teradata_side t
union all select 'TXN_COUNT_FEE',      s.TXN_COUNT_FEE,      t.TXN_COUNT_FEE      from snowflake_side s cross join teradata_side t
union all select 'AMT_TOTAL_DEBIT',    s.AMT_TOTAL_DEBIT,    t.AMT_TOTAL_DEBIT    from snowflake_side s cross join teradata_side t
union all select 'AMT_TOTAL_CREDIT',   s.AMT_TOTAL_CREDIT,   t.AMT_TOTAL_CREDIT   from snowflake_side s cross join teradata_side t
union all select 'AMT_TOTAL_FEES',     s.AMT_TOTAL_FEES,     t.AMT_TOTAL_FEES     from snowflake_side s cross join teradata_side t
union all select 'DISTINCT_MERCHANTS', s.DISTINCT_MERCHANTS, t.DISTINCT_MERCHANTS from snowflake_side s cross join teradata_side t
