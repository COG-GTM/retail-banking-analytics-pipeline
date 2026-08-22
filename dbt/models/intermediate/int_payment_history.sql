{{ config(materialized = 'ephemeral') }}

/*
    Snowflake port of the BTEQ work table
    ETL_STAGING_DB.WRK_PAYMENT_HISTORY (bteq/03_stg_risk_factors.bteq).

    Ephemeral, so it is inlined into stg_risk_factors instead of creating and
    later dropping a physical work table.

    A payment is "on time" when it posts within the anniversary month implied
    by the account open date - the same due-date proxy the BTEQ script uses:

        ADD_MONTHS(OPEN_DATE, CAST(MONTHS_BETWEEN(TRANSACTION_DATE, OPEN_DATE) AS INTEGER) + 1)

    Teradata's CAST(... AS INTEGER) on a decimal rounds; Snowflake's implicit
    cast truncates, so ROUND() is explicit here to keep the legacy boundary.
*/

with payments as (

    select
        acct.CUSTOMER_ID,
        acct.ACCOUNT_ID,
        t.TRANSACTION_DATE,
        dateadd(
            'month',
            cast(round(months_between(t.TRANSACTION_DATE, acct.OPEN_DATE)) as integer) + 1,
            acct.OPEN_DATE
        ) as DUE_DATE_PROXY,
        acct.OPEN_DATE
    from {{ source('txn_processing', 'TRANSACTIONS') }} t
    inner join {{ source('core_banking', 'ACCOUNTS') }} acct
        on t.ACCOUNT_ID = acct.ACCOUNT_ID
    inner join {{ source('txn_processing', 'TRANSACTION_TYPES') }} tt
        on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
    where acct.ACCOUNT_TYPE in ('CREDIT', 'LOAN')
      and tt.CATEGORY = 'CREDIT'          /* Payment transactions */
      and t.STATUS_CODE = 'P'
      and t.TRANSACTION_DATE >= dateadd('month', -{{ var('payment_lookback_months') }}, current_date())

)

select
    CUSTOMER_ID,
    ACCOUNT_ID,
    count(*)                                                                    as TOTAL_PAYMENTS,
    sum(case when TRANSACTION_DATE <= DUE_DATE_PROXY then 1 else 0 end)         as ONTIME_PAYMENTS,
    sum(case when TRANSACTION_DATE > DUE_DATE_PROXY then 1 else 0 end)          as LATE_PAYMENTS,
    /* Months since the most recent late payment; accounts that never paid late
       fall back to the account open date, exactly as in the BTEQ script. */
    cast(round(months_between(
        current_date(),
        coalesce(
            max(case when TRANSACTION_DATE > DUE_DATE_PROXY then TRANSACTION_DATE end),
            max(OPEN_DATE)
        )
    )) as integer)                                                              as MONTHS_SINCE_LAST_LATE
from payments
group by CUSTOMER_ID, ACCOUNT_ID
