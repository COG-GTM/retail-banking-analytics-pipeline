{{
    config(
        materialized='view',
        tags=['staging', 'phase1_bteq']
    )
}}

-- Risk-related feature vectors per customer for the SAS/ML risk-scoring model.
-- Translated from the final CREATE TABLE AS SELECT in bteq/03_stg_risk_factors.bteq.
-- The two BTEQ work tables are now ephemeral models referenced via ref():
--   WRK_DAILY_BALANCE   -> {{ ref('int_wrk_daily_balance') }}
--   WRK_PAYMENT_HISTORY -> {{ ref('int_wrk_payment_history') }}
-- ADD_MONTHS retained as native Teradata date function. QUALIFY kept.

with overdraft as (

    -- Overdraft / NSF fees in the last 12 months.
    select
        acct.customer_id,
        sum(case when t.running_balance < 0 then 1 else 0 end) as overdraft_count,
        sum(case when tt.category = 'FEE' and tt.description like '%NSF%'
                 then abs(t.amount) else 0 end)                as nsf_total
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where t.transaction_date >= add_months(current_date, -12)
      and t.status_code = 'P'
    group by acct.customer_id

),

lg_wd as (

    -- Large withdrawals (>= $5,000 single debit).
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
      and t.transaction_date >= add_months(current_date, -12)
      and t.status_code = 'P'
    group by acct.customer_id

),

bal as (

    -- Average daily balance (30-day and 90-day) and volatility.
    select
        customer_id,
        avg(case when transaction_date >= current_date - 30 then eod_balance end) as avg_bal_30d,
        avg(case when transaction_date >= current_date - 90 then eod_balance end) as avg_bal_90d,
        stddev_pop(eod_balance)                                                   as bal_stddev
    from {{ ref('int_wrk_daily_balance') }}
    group by customer_id

),

credit as (

    -- Credit utilization ratio.
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

    -- Payment history summary (rolled up from the ephemeral work table).
    select
        customer_id,
        sum(total_payments)         as total_payments,
        sum(ontime_payments)        as ontime_payments,
        sum(late_payments)          as late_payments,
        min(months_since_last_late) as months_since_last_late
    from {{ ref('int_wrk_payment_history') }}
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

vel as (

    -- Debit velocity (7-day and 30-day rolling totals).
    select
        acct.customer_id,
        sum(case when t.transaction_date >= current_date - 7
                 then abs(t.amount) else 0 end) as debit_7d,
        sum(case when t.transaction_date >= current_date - 30
                 then abs(t.amount) else 0 end) as debit_30d
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.transaction_type_cd = tt.transaction_type_cd
    where tt.category = 'DEBIT'
      and t.transaction_date >= current_date - 30
      and t.status_code = 'P'
    group by acct.customer_id

),

merch as (

    -- Merchant risk indicators.
    select
        acct.customer_id,
        -- New merchants in last 30 days never used by this account before.
        count(distinct case
            when t.transaction_date >= current_date - 30
             and t.merchant_name not in (
                 select distinct t2.merchant_name
                 from {{ source('txn_processing', 'transactions') }} t2
                 where t2.account_id = t.account_id
                   and t2.transaction_date < current_date - 30
                   and t2.merchant_name is not null
             )
            then t.merchant_name
        end) as new_merch_30d,
        -- International transactions (channel heuristic).
        sum(case when t.channel_code = 'INTL' then 1 else 0 end) as intl_txn_cnt,
        -- High-risk merchant categories.
        sum(case when t.merchant_category in (
            'GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP'
        ) then 1 else 0 end) as high_risk_cnt
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.account_id = acct.account_id
    where t.transaction_date >= add_months(current_date, -6)
      and t.status_code = 'P'
    group by acct.customer_id

)

select
    c.customer_id,

    -- Overdraft & NSF
    coalesce(overdraft.overdraft_count, 0)      as account_overdraft_cnt,
    coalesce(overdraft.nsf_total, 0.00)         as nsf_fee_total,

    -- Large withdrawals
    coalesce(lg_wd.large_wd_cnt, 0)             as large_withdrawal_cnt,
    coalesce(lg_wd.large_wd_amt, 0.00)          as large_withdrawal_amt,

    -- Balance metrics
    coalesce(bal.avg_bal_30d, 0.00)             as avg_daily_balance_30d,
    coalesce(bal.avg_bal_90d, 0.00)             as avg_daily_balance_90d,
    coalesce(bal.bal_stddev, 0.0000)            as balance_volatility,

    -- Credit utilization
    case
        when credit.total_credit_limit > 0
        then cast(credit.total_credit_bal / credit.total_credit_limit as decimal(5,4))
        else 0.0000
    end                                         as credit_util_ratio,

    -- Payment history
    case
        when pmh.total_payments > 0
        then cast(pmh.ontime_payments * 100.0 / pmh.total_payments as decimal(5,2))
        else 100.00
    end                                         as payment_ontime_pct,
    coalesce(pmh.late_payments, 0)              as payment_late_cnt,
    coalesce(pmh.months_since_last_late, 999)   as months_since_last_late,

    -- External bureau score
    coalesce(bureau.credit_score, 0)            as external_credit_score,

    -- Transaction velocity
    coalesce(vel.debit_7d, 0.00)                as debit_velocity_7d,
    coalesce(vel.debit_30d, 0.00)               as debit_velocity_30d,

    -- Merchant risk indicators
    coalesce(merch.new_merch_30d, 0)            as new_merchant_cnt_30d,
    coalesce(merch.intl_txn_cnt, 0)             as international_txn_cnt,
    coalesce(merch.high_risk_cnt, 0)            as high_risk_merchant_cnt,

    current_timestamp(6)                        as load_ts
from {{ source('core_banking', 'customers') }} c
left join overdraft on c.customer_id = overdraft.customer_id
left join lg_wd     on c.customer_id = lg_wd.customer_id
left join bal       on c.customer_id = bal.customer_id
left join credit    on c.customer_id = credit.customer_id
left join pmh       on c.customer_id = pmh.customer_id
left join bureau    on c.customer_id = bureau.customer_id
left join vel       on c.customer_id = vel.customer_id
left join merch     on c.customer_id = merch.customer_id
where c.customer_status in ('A', 'I')
