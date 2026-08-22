-- =============================================================================
-- Reconciliation: Teradata ETL_STAGING_DB.STG_CUSTOMER_360 vs Snowflake model
-- =============================================================================
-- Run after loading the Teradata extract into
-- {{ var('staging_database') }}.{{ var('staging_schema') }}.STG_CUSTOMER_360_TD.
-- Compares row counts and column-level checksums; any non-zero DIFF is a break.
-- LOAD_TS is excluded (run-time dependent).
-- =============================================================================

with snowflake_side as (

    select
        count(*)                                    as ROW_COUNT,
        sum(hash(CUSTOMER_ID))                      as CK_CUSTOMER_ID,
        sum(hash(AGE))                              as CK_AGE,
        sum(hash(TENURE_MONTHS))                    as CK_TENURE_MONTHS,
        sum(hash(PRIMARY_ADDRESS))                  as CK_PRIMARY_ADDRESS,
        sum(hash(NUM_ACCOUNTS))                     as CK_NUM_ACCOUNTS,
        sum(hash(NUM_ACTIVE_ACCOUNTS))              as CK_NUM_ACTIVE_ACCOUNTS,
        sum(hash(HAS_CHECKING, HAS_SAVINGS, HAS_CREDIT, HAS_LOAN)) as CK_PRODUCT_FLAGS,
        sum(hash(TOTAL_BALANCE))                    as CK_TOTAL_BALANCE,
        sum(hash(TOTAL_CREDIT_LIMIT))               as CK_TOTAL_CREDIT_LIMIT,
        sum(hash(CREDIT_UTILIZATION_PCT))           as CK_CREDIT_UTILIZATION_PCT
    from {{ ref('stg_customer_360') }}

),

teradata_side as (

    select
        count(*)                                    as ROW_COUNT,
        sum(hash(CUSTOMER_ID))                      as CK_CUSTOMER_ID,
        sum(hash(AGE))                              as CK_AGE,
        sum(hash(TENURE_MONTHS))                    as CK_TENURE_MONTHS,
        sum(hash(PRIMARY_ADDRESS))                  as CK_PRIMARY_ADDRESS,
        sum(hash(NUM_ACCOUNTS))                     as CK_NUM_ACCOUNTS,
        sum(hash(NUM_ACTIVE_ACCOUNTS))              as CK_NUM_ACTIVE_ACCOUNTS,
        sum(hash(HAS_CHECKING, HAS_SAVINGS, HAS_CREDIT, HAS_LOAN)) as CK_PRODUCT_FLAGS,
        sum(hash(TOTAL_BALANCE))                    as CK_TOTAL_BALANCE,
        sum(hash(TOTAL_CREDIT_LIMIT))               as CK_TOTAL_CREDIT_LIMIT,
        sum(hash(CREDIT_UTILIZATION_PCT))           as CK_CREDIT_UTILIZATION_PCT
    from {{ var('staging_database') }}.{{ var('staging_schema') }}.STG_CUSTOMER_360_TD

)

select
    s.ROW_COUNT                     as SNOWFLAKE_ROW_COUNT,
    t.ROW_COUNT                     as TERADATA_ROW_COUNT,
    s.ROW_COUNT - t.ROW_COUNT       as DIFF_ROW_COUNT,
    iff(s.CK_CUSTOMER_ID = t.CK_CUSTOMER_ID, 'MATCH', 'BREAK')                     as CUSTOMER_ID_CHECK,
    iff(s.CK_AGE = t.CK_AGE, 'MATCH', 'BREAK')                                     as AGE_CHECK,
    iff(s.CK_TENURE_MONTHS = t.CK_TENURE_MONTHS, 'MATCH', 'BREAK')                 as TENURE_MONTHS_CHECK,
    iff(s.CK_PRIMARY_ADDRESS = t.CK_PRIMARY_ADDRESS, 'MATCH', 'BREAK')             as PRIMARY_ADDRESS_CHECK,
    iff(s.CK_NUM_ACCOUNTS = t.CK_NUM_ACCOUNTS, 'MATCH', 'BREAK')                   as NUM_ACCOUNTS_CHECK,
    iff(s.CK_NUM_ACTIVE_ACCOUNTS = t.CK_NUM_ACTIVE_ACCOUNTS, 'MATCH', 'BREAK')     as NUM_ACTIVE_ACCOUNTS_CHECK,
    iff(s.CK_PRODUCT_FLAGS = t.CK_PRODUCT_FLAGS, 'MATCH', 'BREAK')                 as PRODUCT_FLAGS_CHECK,
    iff(s.CK_TOTAL_BALANCE = t.CK_TOTAL_BALANCE, 'MATCH', 'BREAK')                 as TOTAL_BALANCE_CHECK,
    iff(s.CK_TOTAL_CREDIT_LIMIT = t.CK_TOTAL_CREDIT_LIMIT, 'MATCH', 'BREAK')       as TOTAL_CREDIT_LIMIT_CHECK,
    iff(s.CK_CREDIT_UTILIZATION_PCT = t.CK_CREDIT_UTILIZATION_PCT, 'MATCH', 'BREAK') as CREDIT_UTILIZATION_CHECK
from snowflake_side s
cross join teradata_side t
