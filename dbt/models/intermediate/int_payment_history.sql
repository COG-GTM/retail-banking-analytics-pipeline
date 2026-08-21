{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Replaces ETL_STAGING_DB.WRK_PAYMENT_HISTORY from bteq/03_stg_risk_factors.bteq.

    Payment behaviour on CREDIT and LOAN accounts. A payment is "on time" when it
    posts on or before the anniversary-derived due-date proxy:
        ADD_MONTHS(OPEN_DATE, CAST(MONTHS_BETWEEN(TXN_DATE, OPEN_DATE) AS INTEGER) + 1)

    Teradata's CAST(<decimal> AS INTEGER) truncates toward zero, so the Snowflake
    port uses TRUNC() around MONTHS_BETWEEN() rather than a bare cast (Snowflake
    CAST rounds half away from zero).
*/

select
    acct.customer_id,
    acct.account_id,
    count(*) as total_payments,
    sum(
        case
            when t.transaction_date <= dateadd(
                    month,
                    trunc(months_between(t.transaction_date, acct.open_date))::int + 1,
                    acct.open_date
                 )
            then 1 else 0
        end
    ) as ontime_payments,
    sum(
        case
            when t.transaction_date > dateadd(
                    month,
                    trunc(months_between(t.transaction_date, acct.open_date))::int + 1,
                    acct.open_date
                 )
            then 1 else 0
        end
    ) as late_payments,
    cast(
        trunc(
            months_between(
                current_date,
                coalesce(
                    max(
                        case
                            when t.transaction_date > dateadd(
                                    month,
                                    trunc(months_between(t.transaction_date, acct.open_date))::int + 1,
                                    acct.open_date
                                 )
                            then t.transaction_date
                        end
                    ),
                    acct.open_date
                )
            )
        ) as number(9, 0)
    ) as months_since_last_late
from {{ source('txn_processing', 'transactions') }} t
inner join {{ source('core_banking', 'accounts') }} acct
    on t.account_id = acct.account_id
inner join {{ source('txn_processing', 'transaction_types') }} tt
    on t.transaction_type_cd = tt.transaction_type_cd
where acct.account_type in ('CREDIT', 'LOAN')
  and tt.category = 'CREDIT'          -- payment transactions
  and t.status_code = 'P'
  and t.transaction_date >= dateadd(month, -{{ var('payment_lookback_months') }}, current_date)
group by
    acct.customer_id,
    acct.account_id
