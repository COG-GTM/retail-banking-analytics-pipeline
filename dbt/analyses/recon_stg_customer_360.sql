/*
    Reconciliation query for STG_CUSTOMER_360 (Teradata -> Snowflake).

    Run the Teradata side against ETL_STAGING_DB.STG_CUSTOMER_360 with the same
    expression list (replacing HASH_MD5/SUM with Teradata's HASHROW/SUM) and
    compare the single result row against the Snowflake output below. Row count
    and every column-level checksum must match.

    Compile with:  dbt compile --select recon_stg_customer_360
*/
select
    count(*)                                                    as ROW_COUNT,
    count(distinct CUSTOMER_ID)                                 as DISTINCT_CUSTOMERS,
    sum(coalesce(AGE, 0))                                       as CHK_AGE,
    sum(coalesce(TENURE_MONTHS, 0))                             as CHK_TENURE_MONTHS,
    sum(coalesce(NUM_ACCOUNTS, 0))                              as CHK_NUM_ACCOUNTS,
    sum(coalesce(NUM_ACTIVE_ACCOUNTS, 0))                       as CHK_NUM_ACTIVE_ACCOUNTS,
    sum(coalesce(TOTAL_BALANCE, 0))                             as CHK_TOTAL_BALANCE,
    sum(coalesce(TOTAL_CREDIT_LIMIT, 0))                        as CHK_TOTAL_CREDIT_LIMIT,
    sum(coalesce(CREDIT_UTILIZATION_PCT, 0))                    as CHK_CREDIT_UTILIZATION_PCT,
    sum(case when HAS_CHECKING = 'Y' then 1 else 0 end)         as CHK_HAS_CHECKING,
    sum(case when HAS_SAVINGS  = 'Y' then 1 else 0 end)         as CHK_HAS_SAVINGS,
    sum(case when HAS_CREDIT   = 'Y' then 1 else 0 end)         as CHK_HAS_CREDIT,
    sum(case when HAS_LOAN     = 'Y' then 1 else 0 end)         as CHK_HAS_LOAN,
    count(PRIMARY_ADDRESS)                                      as CHK_ADDRESS_POPULATED,
    count_if(TOTAL_CREDIT_LIMIT = 0 and CREDIT_UTILIZATION_PCT <> 0) as CHK_DIVIDE_BY_ZERO_VIOLATIONS
from {{ ref('stg_customer_360') }}
