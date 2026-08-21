{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Replaces ETL_STAGING_DB.WRK_DAILY_BALANCE from bteq/03_stg_risk_factors.bteq.

    Ephemeral: the CTE is inlined into every consuming model, so no work table
    is created and no explicit DROP cleanup is required.

    End-of-day balance per account per day: the running balance carried by the
    last posted transaction of the day (Teradata QUALIFY ROW_NUMBER is
    supported natively by Snowflake).
*/

select
    acct.customer_id,
    t.account_id,
    t.transaction_date,
    t.running_balance as eod_balance
from {{ source('txn_processing', 'transactions') }} t
inner join {{ source('core_banking', 'accounts') }} acct
    on t.account_id = acct.account_id
where t.transaction_date >= dateadd(month, -{{ var('balance_lookback_months') }}, current_date)
  and t.status_code = 'P'
qualify row_number() over (
    partition by t.account_id, t.transaction_date
    order by t.transaction_ts desc
) = 1
