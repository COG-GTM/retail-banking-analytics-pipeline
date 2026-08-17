{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'phase1_bteq']
    )
}}

-- Payment behaviour on credit and loan accounts.
-- Former BTEQ work table ETL_STAGING_DB.WRK_PAYMENT_HISTORY
-- (bteq/03_stg_risk_factors.bteq, INTERMEDIATE TABLE 2), materialized as an
-- ephemeral dbt model.
--
-- The on-time / late proxy compares each payment date against the account's
-- monthly anniversary of OPEN_DATE. This anniversary arithmetic uses
-- ADD_MONTHS / MONTHS_BETWEEN, supported natively by Databricks SQL (see
-- README "Dialect notes"). The Teradata cast shorthand "expr (INTEGER)" is
-- rewritten to cast(expr as integer).

select
    acct.customer_id,
    acct.account_id,
    count(*) as total_payments,
    -- On-time payments: posted on/before the (floor(months elapsed) + 1) anniversary.
    sum(case
        when t.transaction_date <= add_months(
                 acct.open_date,
                 cast(months_between(t.transaction_date, acct.open_date) as integer) + 1)
        then 1 else 0
    end) as ontime_payments,
    -- Late payments: posted after that anniversary.
    sum(case
        when t.transaction_date > add_months(
                 acct.open_date,
                 cast(months_between(t.transaction_date, acct.open_date) as integer) + 1)
        then 1 else 0
    end) as late_payments,
    -- Months since the most recent late payment.
    cast(months_between(
        current_date,
        coalesce(max(case
            when t.transaction_date > add_months(
                     acct.open_date,
                     cast(months_between(t.transaction_date, acct.open_date) as integer) + 1)
            then t.transaction_date
        end), acct.open_date)
    ) as integer) as months_since_last_late
from {{ source('txn_processing', 'transactions') }} t
inner join {{ source('core_banking', 'accounts') }} acct
    on t.account_id = acct.account_id
inner join {{ source('txn_processing', 'transaction_types') }} tt
    on t.transaction_type_cd = tt.transaction_type_cd
where acct.account_type in ('CREDIT', 'LOAN')
  and tt.category = 'CREDIT'          -- payment transactions
  and t.status_code = 'P'
  and t.transaction_date >= add_months(current_date, -24)
group by
    acct.customer_id,
    acct.account_id
