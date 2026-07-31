{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID, ACCOUNT_ID)'
    )
}}

-- Payment behaviour on CREDIT and LOAN accounts over the last 24 months.
-- Replaces the ETL_STAGING_DB.WRK_PAYMENT_HISTORY work table created and
-- dropped by bteq/03_stg_risk_factors.bteq.
-- A payment is treated as on time when it posts within one month of the
-- account anniversary date (the due-date proxy used by the BTEQ script).

select
    acct.CUSTOMER_ID,
    acct.ACCOUNT_ID,
    count(*) as TOTAL_PAYMENTS,
    sum(case
        when t.TRANSACTION_DATE <= add_months(acct.OPEN_DATE,
             (months_between(t.TRANSACTION_DATE, acct.OPEN_DATE) (integer)) + 1)
        then 1 else 0
    end) as ONTIME_PAYMENTS,
    sum(case
        when t.TRANSACTION_DATE > add_months(acct.OPEN_DATE,
             (months_between(t.TRANSACTION_DATE, acct.OPEN_DATE) (integer)) + 1)
        then 1 else 0
    end) as LATE_PAYMENTS,
    cast(months_between(
        CURRENT_DATE,
        coalesce(max(case
            when t.TRANSACTION_DATE > add_months(acct.OPEN_DATE,
                 (months_between(t.TRANSACTION_DATE, acct.OPEN_DATE) (integer)) + 1)
            then t.TRANSACTION_DATE
        end), acct.OPEN_DATE)
    ) as integer) as MONTHS_SINCE_LAST_LATE
from {{ source('txn_processing', 'TRANSACTIONS') }} t
inner join {{ source('core_banking', 'ACCOUNTS') }} acct
    on t.ACCOUNT_ID = acct.ACCOUNT_ID
inner join {{ source('txn_processing', 'TRANSACTION_TYPES') }} tt
    on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
where acct.ACCOUNT_TYPE in ('CREDIT', 'LOAN')
  and tt.CATEGORY = 'CREDIT'          -- Payment transactions
  and t.STATUS_CODE = 'P'
  and t.TRANSACTION_DATE >= add_months(CURRENT_DATE, -24)
group by
    acct.CUSTOMER_ID,
    acct.ACCOUNT_ID,
    acct.OPEN_DATE
