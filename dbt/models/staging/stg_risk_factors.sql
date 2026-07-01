-- =============================================================================
-- stg_risk_factors
-- Risk-related feature vectors per customer for the downstream risk scoring
-- model: balance behaviour, payment history, transaction velocity, and
-- external credit bureau data.
-- Ported from bteq/03_stg_risk_factors.bteq (Teradata BTEQ -> Spark SQL / Delta).
-- The two BTEQ work tables (WRK_DAILY_BALANCE, WRK_PAYMENT_HISTORY) are
-- expressed here as CTEs. Consumed by: sas/03_sas_risk_scoring.sas.
-- =============================================================================

with daily_balance as (
    -- WRK_DAILY_BALANCE: last running balance per account per day (last 3 months).
    select
        acct.customer_id,
        t.account_id,
        t.transaction_date,
        t.running_balance as eod_balance
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    where t.transaction_date >= add_months(current_date(), -3)
      and t.status_code = 'P'
    qualify row_number() over (
        partition by t.account_id, t.transaction_date
        order by t.transaction_ts desc
    ) = 1
),

payment_history as (
    -- WRK_PAYMENT_HISTORY: payment behaviour on credit and loan accounts.
    select
        acct.customer_id,
        acct.account_id,
        count(*) as total_payments,
        sum(case
            when t.transaction_date <= add_months(acct.open_date,
                 cast(months_between(t.transaction_date, acct.open_date) as int) + 1)
            then 1 else 0
        end) as ontime_payments,
        sum(case
            when t.transaction_date > add_months(acct.open_date,
                 cast(months_between(t.transaction_date, acct.open_date) as int) + 1)
            then 1 else 0
        end) as late_payments,
        cast(months_between(
            current_date(),
            coalesce(max(case
                when t.transaction_date > add_months(acct.open_date,
                     cast(months_between(t.transaction_date, acct.open_date) as int) + 1)
                then t.transaction_date
            end), acct.open_date)
        ) as int) as months_since_last_late
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where acct.account_type in ('CREDIT', 'LOAN')
      and tt.category = 'CREDIT'          -- Payment transactions.
      and t.status_code = 'P'
      and t.transaction_date >= add_months(current_date(), -24)
    group by acct.customer_id, acct.account_id
),

overdraft as (
    -- Overdraft / NSF fees in last 12 months.
    select
        acct.customer_id,
        sum(case when t.running_balance < 0 then 1 else 0 end) as overdraft_count,
        sum(case when tt.category = 'FEE' and tt.description like '%NSF%'
                 then abs(t.amount) else 0 end) as nsf_total
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where t.transaction_date >= add_months(current_date(), -12)
      and t.status_code = 'P'
    group by acct.customer_id
),

large_withdrawals as (
    -- Large withdrawals (>= $5,000 single debit) in last 12 months.
    select
        acct.customer_id,
        count(*)           as large_wd_cnt,
        sum(abs(t.amount)) as large_wd_amt
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where tt.category = 'DEBIT'
      and abs(t.amount) >= 5000
      and t.transaction_date >= add_months(current_date(), -12)
      and t.status_code = 'P'
    group by acct.customer_id
),

balance_metrics as (
    -- Average daily balance (30d / 90d) and volatility (stddev_pop).
    select
        customer_id,
        avg(case when transaction_date >= date_sub(current_date(), 30) then eod_balance end) as avg_bal_30d,
        avg(case when transaction_date >= date_sub(current_date(), 90) then eod_balance end) as avg_bal_90d,
        stddev_pop(eod_balance)                                                              as bal_stddev
    from daily_balance
    group by customer_id
),

credit as (
    -- Credit utilization ratio inputs.
    select
        customer_id,
        sum(coalesce(current_balance, 0)) as total_credit_bal,
        sum(coalesce(credit_limit, 0))    as total_credit_limit
    from {{ source('core_banking', 'accounts') }}
    where account_type = 'CREDIT'
      and account_status = 'O'
    group by customer_id
),

pmh as (
    -- Roll up payment history from account grain to customer grain.
    select
        customer_id,
        sum(total_payments)         as total_payments,
        sum(ontime_payments)        as ontime_payments,
        sum(late_payments)          as late_payments,
        min(months_since_last_late) as months_since_last_late
    from payment_history
    group by customer_id
),

bureau as (
    -- Most recent external bureau score per customer.
    select
        customer_id,
        external_credit_score as credit_score
    from {{ source('core_banking', 'customer_bureau_scores') }}
    qualify row_number() over (
        partition by customer_id order by report_date desc
    ) = 1
),

velocity as (
    -- Debit velocity (7-day and 30-day rolling totals).
    select
        acct.customer_id,
        sum(case when t.transaction_date >= date_sub(current_date(), 7)
                 then abs(t.amount) else 0 end) as debit_7d,
        sum(case when t.transaction_date >= date_sub(current_date(), 30)
                 then abs(t.amount) else 0 end) as debit_30d
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where tt.category = 'DEBIT'
      and t.transaction_date >= date_sub(current_date(), 30)
      and t.status_code = 'P'
    group by acct.customer_id
),

merch_txns as (
    -- Posted transactions in the last 6 months, keyed to the customer.
    select
        acct.customer_id,
        t.account_id,
        t.transaction_date,
        t.merchant_name,
        t.merchant_category,
        t.channel_code
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    where t.transaction_date >= add_months(current_date(), -6)
      and t.status_code = 'P'
),

prior_merchants as (
    -- Merchants seen on an account before the last 30 days (full history).
    select distinct
        account_id,
        merchant_name
    from {{ source('txn_processing', 'transactions') }}
    where transaction_date < date_sub(current_date(), 30)
      and merchant_name is not null
),

new_merchants as (
    -- New merchants in last 30 days not previously seen on the account.
    select
        mt.customer_id,
        count(distinct mt.merchant_name) as new_merch_30d
    from merch_txns mt
    left anti join prior_merchants pm
        on mt.account_id = pm.account_id
       and mt.merchant_name = pm.merchant_name
    where mt.transaction_date >= date_sub(current_date(), 30)
      and mt.merchant_name is not null
    group by mt.customer_id
),

merch as (
    -- Merchant risk indicators over the 6-month window.
    select
        customer_id,
        sum(case when channel_code = 'INTL' then 1 else 0 end) as intl_txn_cnt,
        sum(case when merchant_category in (
            'GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP'
        ) then 1 else 0 end) as high_risk_cnt
    from merch_txns
    group by customer_id
)

select
    c.customer_id,

    -- ---- Overdraft & NSF ----
    coalesce(overdraft.overdraft_count, 0)          as account_overdraft_cnt,
    coalesce(overdraft.nsf_total, 0.00)             as nsf_fee_total,

    -- ---- Large Withdrawal Detection ----
    coalesce(lg_wd.large_wd_cnt, 0)                 as large_withdrawal_cnt,
    coalesce(lg_wd.large_wd_amt, 0.00)              as large_withdrawal_amt,

    -- ---- Balance Metrics ----
    coalesce(bal.avg_bal_30d, 0.00)                 as avg_daily_balance_30d,
    coalesce(bal.avg_bal_90d, 0.00)                 as avg_daily_balance_90d,
    coalesce(bal.bal_stddev, 0.0000)                as balance_volatility,

    -- ---- Credit Utilization ----
    case
        when credit.total_credit_limit > 0
        then cast(credit.total_credit_bal / credit.total_credit_limit as decimal(5, 4))
        else 0.0000
    end                                             as credit_util_ratio,

    -- ---- Payment History ----
    case
        when pmh.total_payments > 0
        then cast(pmh.ontime_payments * 100.0 / pmh.total_payments as decimal(5, 2))
        else 100.00
    end                                             as payment_ontime_pct,
    coalesce(pmh.late_payments, 0)                  as payment_late_cnt,
    coalesce(pmh.months_since_last_late, 999)       as months_since_last_late,

    -- ---- External Bureau Score ----
    coalesce(bureau.credit_score, 0)                as external_credit_score,

    -- ---- Transaction Velocity ----
    coalesce(vel.debit_7d, 0.00)                    as debit_velocity_7d,
    coalesce(vel.debit_30d, 0.00)                   as debit_velocity_30d,

    -- ---- Merchant Risk Indicators ----
    coalesce(nm.new_merch_30d, 0)                   as new_merchant_cnt_30d,
    coalesce(merch.intl_txn_cnt, 0)                 as international_txn_cnt,
    coalesce(merch.high_risk_cnt, 0)                as high_risk_merchant_cnt,

    current_timestamp()                             as load_ts
from {{ source('core_banking', 'customers') }} c
left join overdraft
    on c.customer_id = overdraft.customer_id
left join large_withdrawals lg_wd
    on c.customer_id = lg_wd.customer_id
left join balance_metrics bal
    on c.customer_id = bal.customer_id
left join credit
    on c.customer_id = credit.customer_id
left join pmh
    on c.customer_id = pmh.customer_id
left join bureau
    on c.customer_id = bureau.customer_id
left join velocity vel
    on c.customer_id = vel.customer_id
left join new_merchants nm
    on c.customer_id = nm.customer_id
left join merch
    on c.customer_id = merch.customer_id
where c.customer_status in ('A', 'I')
