{{ config(materialized = 'ephemeral') }}

/*
    Snowflake port of the BTEQ work table
    ETL_STAGING_DB.WRK_DAILY_BALANCE (bteq/03_stg_risk_factors.bteq).

    Materialized as an ephemeral model, so it is inlined as a CTE into
    stg_risk_factors: no physical work table is created and the BTEQ
    "DROP TABLE ... WRK_DAILY_BALANCE" cleanup step disappears with it.

    End-of-day balance = the running balance on the last posted transaction of
    each account/day, over the trailing balance_lookback_months window.
*/

select
    acct.CUSTOMER_ID,
    t.ACCOUNT_ID,
    t.TRANSACTION_DATE,
    t.RUNNING_BALANCE as EOD_BALANCE
from {{ source('txn_processing', 'TRANSACTIONS') }} t
inner join {{ source('core_banking', 'ACCOUNTS') }} acct
    on t.ACCOUNT_ID = acct.ACCOUNT_ID
where t.TRANSACTION_DATE >= dateadd('month', -{{ var('balance_lookback_months') }}, current_date())
  and t.STATUS_CODE = 'P'
qualify row_number() over (
    partition by t.ACCOUNT_ID, t.TRANSACTION_DATE
    order by t.TRANSACTION_TS desc
) = 1
