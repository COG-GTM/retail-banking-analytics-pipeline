{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID)'
    )
}}

-- Risk feature vector, one row per non-closed customer.
-- Port of bteq/03_stg_risk_factors.bteq; output contract is
-- ETL_STAGING_DB.STG_RISK_FACTORS in ddl/01_staging_tables.sql.
-- The BTEQ .GOTO / DROP TABLE cleanup around the two work tables is replaced by
-- the dbt DAG: int_daily_balance and int_payment_history are built first.

with overdraft as (

    -- Overdrafts and NSF fees over the last 12 months
    select
        acct.CUSTOMER_ID,
        sum(case when t.RUNNING_BALANCE < 0 then 1 else 0 end) as OVERDRAFT_COUNT,
        sum(case when tt.CATEGORY = 'FEE' and tt.DESCRIPTION like '%NSF%'
                 then abs(t.AMOUNT) else 0 end)                as NSF_TOTAL
    from {{ source('txn_processing', 'TRANSACTIONS') }} t
    inner join {{ source('core_banking', 'ACCOUNTS') }} acct
        on t.ACCOUNT_ID = acct.ACCOUNT_ID
    inner join {{ source('txn_processing', 'TRANSACTION_TYPES') }} tt
        on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
    where t.TRANSACTION_DATE >= add_months(CURRENT_DATE, -12)
      and t.STATUS_CODE = 'P'
    group by acct.CUSTOMER_ID

),

large_withdrawals as (

    -- Single debits of $5,000 or more over the last 12 months
    select
        acct.CUSTOMER_ID,
        count(*)           as LARGE_WD_CNT,
        sum(abs(t.AMOUNT)) as LARGE_WD_AMT
    from {{ source('txn_processing', 'TRANSACTIONS') }} t
    inner join {{ source('core_banking', 'ACCOUNTS') }} acct
        on t.ACCOUNT_ID = acct.ACCOUNT_ID
    inner join {{ source('txn_processing', 'TRANSACTION_TYPES') }} tt
        on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
    where tt.CATEGORY = 'DEBIT'
      and abs(t.AMOUNT) >= 5000
      and t.TRANSACTION_DATE >= add_months(CURRENT_DATE, -12)
      and t.STATUS_CODE = 'P'
    group by acct.CUSTOMER_ID

),

balance_metrics as (

    -- Average daily balance (30d / 90d) and balance volatility
    select
        CUSTOMER_ID,
        avg(case when TRANSACTION_DATE >= CURRENT_DATE - 30 then EOD_BALANCE end) as AVG_BAL_30D,
        avg(case when TRANSACTION_DATE >= CURRENT_DATE - 90 then EOD_BALANCE end) as AVG_BAL_90D,
        stddev_pop(EOD_BALANCE)                                                   as BAL_STDDEV
    from {{ ref('int_daily_balance') }}
    group by CUSTOMER_ID

),

credit_exposure as (

    select
        CUSTOMER_ID,
        sum(coalesce(CURRENT_BALANCE, 0)) as TOTAL_CREDIT_BAL,
        sum(coalesce(CREDIT_LIMIT, 0))    as TOTAL_CREDIT_LIMIT
    from {{ source('core_banking', 'ACCOUNTS') }}
    where ACCOUNT_TYPE = 'CREDIT'
      and ACCOUNT_STATUS = 'O'
    group by CUSTOMER_ID

),

payment_history as (

    select
        CUSTOMER_ID,
        sum(TOTAL_PAYMENTS)         as TOTAL_PAYMENTS,
        sum(ONTIME_PAYMENTS)        as ONTIME_PAYMENTS,
        sum(LATE_PAYMENTS)          as LATE_PAYMENTS,
        min(MONTHS_SINCE_LAST_LATE) as MONTHS_SINCE_LAST_LATE
    from {{ ref('int_payment_history') }}
    group by CUSTOMER_ID

),

bureau as (

    -- Most recent external bureau score per customer
    select
        CUSTOMER_ID,
        EXTERNAL_CREDIT_SCORE as CREDIT_SCORE
    from {{ source('core_banking', 'CUSTOMER_BUREAU_SCORES') }}
    qualify row_number() over (
        partition by CUSTOMER_ID order by REPORT_DATE desc
    ) = 1

),

velocity as (

    -- Rolling 7-day and 30-day debit totals
    select
        acct.CUSTOMER_ID,
        sum(case when t.TRANSACTION_DATE >= CURRENT_DATE - 7
                 then abs(t.AMOUNT) else 0 end) as DEBIT_7D,
        sum(case when t.TRANSACTION_DATE >= CURRENT_DATE - 30
                 then abs(t.AMOUNT) else 0 end) as DEBIT_30D
    from {{ source('txn_processing', 'TRANSACTIONS') }} t
    inner join {{ source('core_banking', 'ACCOUNTS') }} acct
        on t.ACCOUNT_ID = acct.ACCOUNT_ID
    inner join {{ source('txn_processing', 'TRANSACTION_TYPES') }} tt
        on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
    where tt.CATEGORY = 'DEBIT'
      and t.TRANSACTION_DATE >= CURRENT_DATE - 30
      and t.STATUS_CODE = 'P'
    group by acct.CUSTOMER_ID

),

merchant_risk as (

    select
        acct.CUSTOMER_ID,
        -- Merchants first seen on the account in the last 30 days
        count(distinct case
            when t.TRANSACTION_DATE >= CURRENT_DATE - 30
             and t.MERCHANT_NAME not in (
                 select distinct t2.MERCHANT_NAME
                 from {{ source('txn_processing', 'TRANSACTIONS') }} t2
                 where t2.ACCOUNT_ID = t.ACCOUNT_ID
                   and t2.TRANSACTION_DATE < CURRENT_DATE - 30
                   and t2.MERCHANT_NAME is not null
             )
            then t.MERCHANT_NAME
        end) as NEW_MERCH_30D,
        -- International transactions (channel heuristic)
        sum(case when t.CHANNEL_CODE = 'INTL' then 1 else 0 end) as INTL_TXN_CNT,
        -- High-risk merchant categories
        sum(case when t.MERCHANT_CATEGORY in (
            'GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP'
        ) then 1 else 0 end) as HIGH_RISK_CNT
    from {{ source('txn_processing', 'TRANSACTIONS') }} t
    inner join {{ source('core_banking', 'ACCOUNTS') }} acct
        on t.ACCOUNT_ID = acct.ACCOUNT_ID
    where t.TRANSACTION_DATE >= add_months(CURRENT_DATE, -6)
      and t.STATUS_CODE = 'P'
    group by acct.CUSTOMER_ID

)

select
    c.CUSTOMER_ID,

    -- Overdraft & NSF
    coalesce(overdraft.OVERDRAFT_COUNT, 0)      as ACCOUNT_OVERDRAFT_CNT,
    coalesce(overdraft.NSF_TOTAL, 0.00)         as NSF_FEE_TOTAL,

    -- Large withdrawals
    coalesce(lg_wd.LARGE_WD_CNT, 0)             as LARGE_WITHDRAWAL_CNT,
    coalesce(lg_wd.LARGE_WD_AMT, 0.00)          as LARGE_WITHDRAWAL_AMT,

    -- Balance behaviour
    coalesce(bal.AVG_BAL_30D, 0.00)             as AVG_DAILY_BALANCE_30D,
    coalesce(bal.AVG_BAL_90D, 0.00)             as AVG_DAILY_BALANCE_90D,
    coalesce(bal.BAL_STDDEV, 0.0000)            as BALANCE_VOLATILITY,

    -- Credit utilization
    case
        when credit.TOTAL_CREDIT_LIMIT > 0
        then cast(credit.TOTAL_CREDIT_BAL / credit.TOTAL_CREDIT_LIMIT as decimal(5,4))
        else 0.0000
    end                                         as CREDIT_UTIL_RATIO,

    -- Payment history
    case
        when pmh.TOTAL_PAYMENTS > 0
        then cast(pmh.ONTIME_PAYMENTS * 100.0 / pmh.TOTAL_PAYMENTS as decimal(5,2))
        else 100.00
    end                                         as PAYMENT_ONTIME_PCT,
    coalesce(pmh.LATE_PAYMENTS, 0)              as PAYMENT_LATE_CNT,
    coalesce(pmh.MONTHS_SINCE_LAST_LATE, 999)   as MONTHS_SINCE_LAST_LATE,

    -- External bureau score
    coalesce(bureau.CREDIT_SCORE, 0)            as EXTERNAL_CREDIT_SCORE,

    -- Transaction velocity
    coalesce(vel.DEBIT_7D, 0.00)                as DEBIT_VELOCITY_7D,
    coalesce(vel.DEBIT_30D, 0.00)               as DEBIT_VELOCITY_30D,

    -- Merchant risk indicators
    coalesce(merch.NEW_MERCH_30D, 0)            as NEW_MERCHANT_CNT_30D,
    coalesce(merch.INTL_TXN_CNT, 0)             as INTERNATIONAL_TXN_CNT,
    coalesce(merch.HIGH_RISK_CNT, 0)            as HIGH_RISK_MERCHANT_CNT,

    current_timestamp(6)                        as LOAD_TS

from {{ source('core_banking', 'CUSTOMERS') }} c
left join overdraft
    on c.CUSTOMER_ID = overdraft.CUSTOMER_ID
left join large_withdrawals lg_wd
    on c.CUSTOMER_ID = lg_wd.CUSTOMER_ID
left join balance_metrics bal
    on c.CUSTOMER_ID = bal.CUSTOMER_ID
left join credit_exposure credit
    on c.CUSTOMER_ID = credit.CUSTOMER_ID
left join payment_history pmh
    on c.CUSTOMER_ID = pmh.CUSTOMER_ID
left join bureau
    on c.CUSTOMER_ID = bureau.CUSTOMER_ID
left join velocity vel
    on c.CUSTOMER_ID = vel.CUSTOMER_ID
left join merchant_risk merch
    on c.CUSTOMER_ID = merch.CUSTOMER_ID
where c.CUSTOMER_STATUS in ('A', 'I')
