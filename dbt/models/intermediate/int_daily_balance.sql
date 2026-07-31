{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID, ACCOUNT_ID)'
    )
}}

-- End-of-day balance snapshots for the last 3 months, one row per account/day.
-- Replaces the ETL_STAGING_DB.WRK_DAILY_BALANCE work table created and dropped
-- by bteq/03_stg_risk_factors.bteq; dbt rebuilds it on every run instead.

select
    acct.CUSTOMER_ID,
    t.ACCOUNT_ID,
    t.TRANSACTION_DATE,
    t.RUNNING_BALANCE as EOD_BALANCE
from {{ source('txn_processing', 'TRANSACTIONS') }} t
inner join {{ source('core_banking', 'ACCOUNTS') }} acct
    on t.ACCOUNT_ID = acct.ACCOUNT_ID
where t.TRANSACTION_DATE >= add_months(CURRENT_DATE, -3)
  and t.STATUS_CODE = 'P'
-- Keep only the last transaction per account per day
qualify row_number() over (
    partition by t.ACCOUNT_ID, t.TRANSACTION_DATE
    order by t.TRANSACTION_TS desc
) = 1
