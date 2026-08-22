/*
    Teradata -> Snowflake reconciliation for STG_RISK_FACTORS.

    Compile with `dbt compile --select recon_stg_risk_factors` and run the same
    aggregates on Teradata (ETL_STAGING_DB.STG_RISK_FACTORS) after a matching
    run date. Row counts and integer features must match exactly; the
    floating-point features (volatility, velocity, ratios) are compared against
    the agreed tolerance in docs/modernization/TICKET-05_stg_risk_factors.md.
*/

select
    count(*)                                    as ROW_COUNT,
    count(distinct CUSTOMER_ID)                 as DISTINCT_CUSTOMERS,
    sum(ACCOUNT_OVERDRAFT_CNT)                  as SUM_OVERDRAFT_CNT,
    sum(NSF_FEE_TOTAL)                          as SUM_NSF_FEE_TOTAL,
    sum(LARGE_WITHDRAWAL_CNT)                   as SUM_LARGE_WD_CNT,
    sum(LARGE_WITHDRAWAL_AMT)                   as SUM_LARGE_WD_AMT,
    avg(AVG_DAILY_BALANCE_30D)                  as AVG_BAL_30D,
    avg(AVG_DAILY_BALANCE_90D)                  as AVG_BAL_90D,
    avg(BALANCE_VOLATILITY)                     as AVG_BALANCE_VOLATILITY,
    avg(CREDIT_UTIL_RATIO)                      as AVG_CREDIT_UTIL,
    avg(PAYMENT_ONTIME_PCT)                     as AVG_PAYMENT_ONTIME_PCT,
    sum(PAYMENT_LATE_CNT)                       as SUM_PAYMENT_LATE_CNT,
    avg(EXTERNAL_CREDIT_SCORE)                  as AVG_BUREAU_SCORE,
    sum(DEBIT_VELOCITY_7D)                      as SUM_DEBIT_VELOCITY_7D,
    sum(DEBIT_VELOCITY_30D)                     as SUM_DEBIT_VELOCITY_30D,
    sum(NEW_MERCHANT_CNT_30D)                   as SUM_NEW_MERCHANT_CNT_30D,
    sum(INTERNATIONAL_TXN_CNT)                  as SUM_INTERNATIONAL_TXN_CNT,
    sum(HIGH_RISK_MERCHANT_CNT)                 as SUM_HIGH_RISK_MERCHANT_CNT,
    /* Customers with no transaction activity must survive the migration. */
    sum(case when DEBIT_VELOCITY_30D = 0 and ACCOUNT_OVERDRAFT_CNT = 0
             then 1 else 0 end)                 as INACTIVE_CUSTOMER_COUNT
from {{ ref('stg_risk_factors') }}
