{{
    config(
        materialized='table',
        post_hook="{{ assert_not_empty_and_log(this, '03_stg_risk_factors', 'FULL_LOAD') }}"
    )
}}

/*
    =========================================================================
    stg_risk_factors - Snowflake/dbt port of bteq/03_stg_risk_factors.bteq
    =========================================================================
    Grain:   one row per customer with CUSTOMER_STATUS in ('A','I')
    Source:  CORE_BANKING.CUSTOMERS / ACCOUNTS / CUSTOMER_BUREAU_SCORES
             TXN_PROCESSING.TRANSACTIONS / TRANSACTION_TYPES
    Target:  ETL_STAGING.STG_RISK_FACTORS

    Teradata -> Snowflake notes:
      * ADD_MONTHS(d, -n)        -> DATEADD(month, -n, d)
      * CURRENT_DATE - n         -> DATEADD(day, -n, CURRENT_DATE)
      * CURRENT_TIMESTAMP(6)     -> CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
      * DECIMAL(p,s)             -> NUMBER(p,s)
      * QUALIFY / STDDEV_POP     -> supported natively, semantics unchanged
      * The correlated NOT IN sub-query used for new-merchant detection is not
        supported inside an aggregate in Snowflake and is rewritten as an
        anti-join against prior_merchants (identical semantics: the sub-query
        excluded NULL merchant names, and COUNT(DISTINCT ...) ignores NULLs).
      * Work tables WRK_DAILY_BALANCE / WRK_PAYMENT_HISTORY become ephemeral
        models, so nothing has to be dropped after the run.
*/

with daily_balance as (
    select * from {{ ref('int_daily_balance') }}
),

payment_history as (
    select * from {{ ref('int_payment_history') }}
),

/* -- Overdraft / NSF fees in last 12 months -- */
overdraft as (
    select
        acct.customer_id,
        sum(case when t.running_balance < 0 then 1 else 0 end) as overdraft_count,
        sum(
            case
                when tt.category = 'FEE' and tt.description like '%NSF%'
                then abs(t.amount) else 0
            end
        ) as nsf_total
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where t.transaction_date >= dateadd(month, -{{ var('fee_lookback_months') }}, current_date)
      and t.status_code = 'P'
    group by acct.customer_id
),

/* -- Large withdrawals (>= $5,000 single debit) -- */
large_withdrawals as (
    select
        acct.customer_id,
        count(*) as large_wd_cnt,
        sum(abs(t.amount)) as large_wd_amt
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where tt.category = 'DEBIT'
      and abs(t.amount) >= 5000
      and t.transaction_date >= dateadd(month, -{{ var('fee_lookback_months') }}, current_date)
      and t.status_code = 'P'
    group by acct.customer_id
),

/* -- Average daily balance (30-day and 90-day) and volatility -- */
balance_metrics as (
    select
        customer_id,
        avg(case when transaction_date >= dateadd(day, -30, current_date) then eod_balance end) as avg_bal_30d,
        avg(case when transaction_date >= dateadd(day, -90, current_date) then eod_balance end) as avg_bal_90d,
        stddev_pop(eod_balance) as bal_stddev
    from daily_balance
    group by customer_id
),

/* -- Credit utilization ratio -- */
credit as (
    select
        customer_id,
        sum(coalesce(current_balance, 0)) as total_credit_bal,
        sum(coalesce(credit_limit, 0)) as total_credit_limit
    from {{ source('core_banking', 'accounts') }}
    where account_type = 'CREDIT'
      and account_status = 'O'
    group by customer_id
),

/* -- Payment history summary (customer level) -- */
payment_summary as (
    select
        customer_id,
        sum(total_payments) as total_payments,
        sum(ontime_payments) as ontime_payments,
        sum(late_payments) as late_payments,
        min(months_since_last_late) as months_since_last_late
    from payment_history
    group by customer_id
),

/* -- External bureau score: latest report per customer -- */
bureau as (
    select
        customer_id,
        external_credit_score as credit_score
    from {{ source('core_banking', 'customer_bureau_scores') }}
    qualify row_number() over (
        partition by customer_id order by report_date desc
    ) = 1
),

/* -- Debit velocity (7-day and 30-day rolling totals) -- */
velocity as (
    select
        acct.customer_id,
        sum(
            case when t.transaction_date >= dateadd(day, -7, current_date)
                 then abs(t.amount) else 0 end
        ) as debit_7d,
        sum(
            case when t.transaction_date >= dateadd(day, -30, current_date)
                 then abs(t.amount) else 0 end
        ) as debit_30d
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where tt.category = 'DEBIT'
      and t.transaction_date >= dateadd(day, -30, current_date)
      and t.status_code = 'P'
    group by acct.customer_id
),

/* -- Merchants seen on an account before the 30-day new-merchant window -- */
prior_merchants as (
    select distinct
        account_id,
        merchant_name
    from {{ source('txn_processing', 'transactions') }}
    where transaction_date < dateadd(day, -30, current_date)
      and merchant_name is not null
),

/* -- Merchant risk indicators -- */
merchant_risk as (
    select
        acct.customer_id,
        count(distinct
            case
                when t.transaction_date >= dateadd(day, -30, current_date)
                 and pm.merchant_name is null
                then t.merchant_name
            end
        ) as new_merch_30d,
        sum(case when t.channel_code = 'INTL' then 1 else 0 end) as intl_txn_cnt,
        sum(
            case when t.merchant_category in (
                'GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP'
            ) then 1 else 0 end
        ) as high_risk_cnt
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    left join prior_merchants pm
        on pm.account_id = t.account_id
       and pm.merchant_name = t.merchant_name
    where t.transaction_date >= dateadd(month, -{{ var('merchant_lookback_months') }}, current_date)
      and t.status_code = 'P'
    group by acct.customer_id
)

select
    c.customer_id,

    /* ---- Overdraft & NSF ---- */
    cast(coalesce(overdraft.overdraft_count, 0) as number(9, 0))    as account_overdraft_cnt,
    cast(coalesce(overdraft.nsf_total, 0.00) as number(15, 2))      as nsf_fee_total,

    /* ---- Large Withdrawal Detection ---- */
    cast(coalesce(large_withdrawals.large_wd_cnt, 0) as number(9, 0))   as large_withdrawal_cnt,
    cast(coalesce(large_withdrawals.large_wd_amt, 0.00) as number(18, 2)) as large_withdrawal_amt,

    /* ---- Balance Metrics ---- */
    cast(coalesce(balance_metrics.avg_bal_30d, 0.00) as number(15, 2))  as avg_daily_balance_30d,
    cast(coalesce(balance_metrics.avg_bal_90d, 0.00) as number(15, 2))  as avg_daily_balance_90d,
    cast(coalesce(balance_metrics.bal_stddev, 0.0000) as number(10, 4)) as balance_volatility,

    /* ---- Credit Utilization ---- */
    cast(
        case
            when credit.total_credit_limit > 0
            then credit.total_credit_bal / credit.total_credit_limit
            else 0.0000
        end as number(5, 4)
    ) as credit_util_ratio,

    /* ---- Payment History ---- */
    cast(
        case
            when payment_summary.total_payments > 0
            then payment_summary.ontime_payments * 100.0 / payment_summary.total_payments
            else 100.00
        end as number(5, 2)
    ) as payment_ontime_pct,
    cast(coalesce(payment_summary.late_payments, 0) as number(9, 0))            as payment_late_cnt,
    cast(coalesce(payment_summary.months_since_last_late, 999) as number(9, 0)) as months_since_last_late,

    /* ---- External Bureau Score ---- */
    cast(coalesce(bureau.credit_score, 0) as number(9, 0)) as external_credit_score,

    /* ---- Transaction Velocity ---- */
    cast(coalesce(velocity.debit_7d, 0.00) as number(15, 2))  as debit_velocity_7d,
    cast(coalesce(velocity.debit_30d, 0.00) as number(15, 2)) as debit_velocity_30d,

    /* ---- Merchant Risk Indicators ---- */
    cast(coalesce(merchant_risk.new_merch_30d, 0) as number(9, 0))  as new_merchant_cnt_30d,
    cast(coalesce(merchant_risk.intl_txn_cnt, 0) as number(9, 0))   as international_txn_cnt,
    cast(coalesce(merchant_risk.high_risk_cnt, 0) as number(9, 0))  as high_risk_merchant_cnt,

    current_timestamp()::timestamp_ntz as load_ts

from {{ source('core_banking', 'customers') }} c
left join overdraft
    on c.customer_id = overdraft.customer_id
left join large_withdrawals
    on c.customer_id = large_withdrawals.customer_id
left join balance_metrics
    on c.customer_id = balance_metrics.customer_id
left join credit
    on c.customer_id = credit.customer_id
left join payment_summary
    on c.customer_id = payment_summary.customer_id
left join bureau
    on c.customer_id = bureau.customer_id
left join velocity
    on c.customer_id = velocity.customer_id
left join merchant_risk
    on c.customer_id = merchant_risk.customer_id
where c.customer_status in ('A', 'I')
