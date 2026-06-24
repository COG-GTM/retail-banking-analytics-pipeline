{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'phase1_bteq']
    )
}}

-- Daily end-of-day balance snapshots used for balance-volatility calculation.
-- Former BTEQ work table ETL_STAGING_DB.WRK_DAILY_BALANCE
-- (bteq/03_stg_risk_factors.bteq, INTERMEDIATE TABLE 1), materialized as an
-- ephemeral dbt model (inlined as a CTE wherever it is ref()'d).

select
    acct.customer_id,
    t.account_id,
    t.transaction_date,
    -- Use the running balance as-of each transaction date.
    t.running_balance as eod_balance
from {{ source('txn_processing', 'transactions') }} t
inner join {{ source('core_banking', 'accounts') }} acct
    on t.account_id = acct.account_id
where t.transaction_date >= add_months(current_date, -3)
  and t.status_code = 'P'
-- Keep only the last transaction per account per day.
qualify row_number() over (
    partition by t.account_id, t.transaction_date
    order by t.transaction_ts desc
) = 1
