{{
    config(
        materialized = 'table',
        alias = 'STG_RISK_FACTORS',
        post_hook = "{{ log_etl_run(job_name='03_stg_risk_factors', step_name='FULL_LOAD') }}"
    )
}}

/*
    Snowflake port of bteq/03_stg_risk_factors.bteq.

    Risk feature vectors per customer for the downstream risk scoring model:
    overdraft/NSF behaviour, large withdrawals, balance level and volatility,
    credit utilization, payment history, bureau score, debit velocity and
    merchant risk indicators.

    BTEQ construct mapping:
      - WRK_DAILY_BALANCE / WRK_PAYMENT_HISTORY work tables plus their
        DROP TABLE cleanup  -> ephemeral models int_daily_balance /
        int_payment_history, inlined as CTEs (no orphaned work tables).
      - .SET ERRORLEVEL / .IF ERRORCODE <> 0 THEN .EXIT  -> dbt fails the model
        and the invocation on any SQL error.
      - .IF ACTIVITYCOUNT = 0 THEN .EXIT 99  -> singular test
        tests/assert_stg_risk_factors_not_empty.sql
      - INSERT INTO ETL_RUN_LOG  -> log_etl_run() post-hook.
      - COLLECT STATISTICS  -> dropped (automatic in Snowflake).
      - CREATE MULTISET TABLE ... PRIMARY INDEX (CUSTOMER_ID) -> table
        materialization; Snowflake has no primary index.
      - ADD_MONTHS(CURRENT_DATE, -n) / CURRENT_DATE - n -> dateadd().
      - STDDEV_POP and QUALIFY ROW_NUMBER() are native in Snowflake.
*/

with overdraft as (

    /* Overdraft occurrences and NSF fees in the trailing fee window. */
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
    where t.TRANSACTION_DATE >= dateadd('month', -{{ var('fee_lookback_months') }}, current_date())
      and t.STATUS_CODE = 'P'
    group by acct.CUSTOMER_ID

),

large_withdrawals as (

    /* Single debits at or above the large-withdrawal threshold. */
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
      and abs(t.AMOUNT) >= {{ var('large_withdrawal_threshold') }}
      and t.TRANSACTION_DATE >= dateadd('month', -{{ var('fee_lookback_months') }}, current_date())
      and t.STATUS_CODE = 'P'
    group by acct.CUSTOMER_ID

),

balances as (

    /* Average daily balance (30/90 day) and population volatility over the
       full 3-month end-of-day balance history. AVG ignores NULLs in both
       engines, so customers with no activity inside a window keep a NULL
       average and fall back to the COALESCE default below. */
    select
        CUSTOMER_ID,
        avg(case when TRANSACTION_DATE >= dateadd('day', -30, current_date()) then EOD_BALANCE end) as AVG_BAL_30D,
        avg(case when TRANSACTION_DATE >= dateadd('day', -90, current_date()) then EOD_BALANCE end) as AVG_BAL_90D,
        stddev_pop(EOD_BALANCE)                                                                     as BAL_STDDEV
    from {{ ref('int_daily_balance') }}
    group by CUSTOMER_ID

),

credit as (

    /* Credit utilization inputs across open credit accounts. */
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

    /* Most recent external bureau score per customer. */
    select
        CUSTOMER_ID,
        EXTERNAL_CREDIT_SCORE as CREDIT_SCORE
    from {{ source('core_banking', 'CUSTOMER_BUREAU_SCORES') }}
    qualify row_number() over (
        partition by CUSTOMER_ID
        order by REPORT_DATE desc
    ) = 1

),

velocity as (

    /* Debit velocity: 7-day and 30-day rolling debit totals. The outer 30-day
       filter means customers with no debits in the window are absent and get
       the COALESCE default, matching Teradata. */
    select
        acct.CUSTOMER_ID,
        sum(case when t.TRANSACTION_DATE >= dateadd('day', -7, current_date())
                 then abs(t.AMOUNT) else 0 end)  as DEBIT_7D,
        sum(case when t.TRANSACTION_DATE >= dateadd('day', -30, current_date())
                 then abs(t.AMOUNT) else 0 end)  as DEBIT_30D
    from {{ source('txn_processing', 'TRANSACTIONS') }} t
    inner join {{ source('core_banking', 'ACCOUNTS') }} acct
        on t.ACCOUNT_ID = acct.ACCOUNT_ID
    inner join {{ source('txn_processing', 'TRANSACTION_TYPES') }} tt
        on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
    where tt.CATEGORY = 'DEBIT'
      and t.TRANSACTION_DATE >= dateadd('day', -30, current_date())
      and t.STATUS_CODE = 'P'
    group by acct.CUSTOMER_ID

),

merchant_activity as (

    select
        acct.CUSTOMER_ID,
        t.ACCOUNT_ID,
        t.TRANSACTION_DATE,
        t.MERCHANT_NAME,
        t.MERCHANT_CATEGORY,
        t.CHANNEL_CODE
    from {{ source('txn_processing', 'TRANSACTIONS') }} t
    inner join {{ source('core_banking', 'ACCOUNTS') }} acct
        on t.ACCOUNT_ID = acct.ACCOUNT_ID
    where t.TRANSACTION_DATE >= dateadd('month', -{{ var('merchant_lookback_months') }}, current_date())
      and t.STATUS_CODE = 'P'

),

prior_merchants as (

    /* Merchants an account used before the 30-day "new merchant" window.
       The BTEQ correlated NOT IN subquery scans all history and is not
       restricted to posted transactions; that is preserved here. Rewritten as
       a de-duplicated anti-join because Snowflake does not allow a correlated
       subquery inside an aggregate expression. */
    select distinct
        ACCOUNT_ID,
        MERCHANT_NAME
    from {{ source('txn_processing', 'TRANSACTIONS') }}
    where TRANSACTION_DATE < dateadd('day', -30, current_date())
      and MERCHANT_NAME is not null

),

merchant_risk as (

    select
        m.CUSTOMER_ID,
        count(distinct case
            when m.TRANSACTION_DATE >= dateadd('day', -30, current_date())
             and p.MERCHANT_NAME is null
            then m.MERCHANT_NAME
        end)                                                            as NEW_MERCH_30D,
        sum(case when m.CHANNEL_CODE = 'INTL' then 1 else 0 end)        as INTL_TXN_CNT,
        sum(case when m.MERCHANT_CATEGORY in (
            'GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP'
        ) then 1 else 0 end)                                            as HIGH_RISK_CNT
    from merchant_activity m
    left join prior_merchants p
        on m.ACCOUNT_ID = p.ACCOUNT_ID
       and m.MERCHANT_NAME = p.MERCHANT_NAME
    group by m.CUSTOMER_ID

)

select
    c.CUSTOMER_ID,

    /* ---- Overdraft & NSF ---- */
    cast(coalesce(overdraft.OVERDRAFT_COUNT, 0) as number(10, 0))       as ACCOUNT_OVERDRAFT_CNT,
    cast(coalesce(overdraft.NSF_TOTAL, 0.00) as number(15, 2))          as NSF_FEE_TOTAL,

    /* ---- Large withdrawal detection ---- */
    cast(coalesce(lg_wd.LARGE_WD_CNT, 0) as number(10, 0))              as LARGE_WITHDRAWAL_CNT,
    cast(coalesce(lg_wd.LARGE_WD_AMT, 0.00) as number(18, 2))           as LARGE_WITHDRAWAL_AMT,

    /* ---- Balance metrics ---- */
    cast(coalesce(bal.AVG_BAL_30D, 0.00) as number(15, 2))              as AVG_DAILY_BALANCE_30D,
    cast(coalesce(bal.AVG_BAL_90D, 0.00) as number(15, 2))              as AVG_DAILY_BALANCE_90D,
    cast(coalesce(bal.BAL_STDDEV, 0.0000) as number(10, 4))             as BALANCE_VOLATILITY,

    /* ---- Credit utilization ---- */
    case
        when credit.TOTAL_CREDIT_LIMIT > 0
        then cast(credit.TOTAL_CREDIT_BAL / credit.TOTAL_CREDIT_LIMIT as number(5, 4))
        else 0.0000
    end                                                                 as CREDIT_UTIL_RATIO,

    /* ---- Payment history ---- */
    case
        when pmh.TOTAL_PAYMENTS > 0
        then cast(pmh.ONTIME_PAYMENTS * 100.0 / pmh.TOTAL_PAYMENTS as number(5, 2))
        else 100.00
    end                                                                 as PAYMENT_ONTIME_PCT,
    cast(coalesce(pmh.LATE_PAYMENTS, 0) as number(10, 0))               as PAYMENT_LATE_CNT,
    cast(coalesce(pmh.MONTHS_SINCE_LAST_LATE, 999) as number(10, 0))    as MONTHS_SINCE_LAST_LATE,

    /* ---- External bureau score ---- */
    cast(coalesce(bureau.CREDIT_SCORE, 0) as number(10, 0))             as EXTERNAL_CREDIT_SCORE,

    /* ---- Transaction velocity ---- */
    cast(coalesce(vel.DEBIT_7D, 0.00) as number(15, 2))                 as DEBIT_VELOCITY_7D,
    cast(coalesce(vel.DEBIT_30D, 0.00) as number(15, 2))                as DEBIT_VELOCITY_30D,

    /* ---- Merchant risk indicators ---- */
    cast(coalesce(merch.NEW_MERCH_30D, 0) as number(10, 0))             as NEW_MERCHANT_CNT_30D,
    cast(coalesce(merch.INTL_TXN_CNT, 0) as number(10, 0))              as INTERNATIONAL_TXN_CNT,
    cast(coalesce(merch.HIGH_RISK_CNT, 0) as number(10, 0))             as HIGH_RISK_MERCHANT_CNT,

    cast(current_timestamp() as timestamp_ntz(6))                       as LOAD_TS

from {{ source('core_banking', 'CUSTOMERS') }} c
left join overdraft
    on c.CUSTOMER_ID = overdraft.CUSTOMER_ID
left join large_withdrawals lg_wd
    on c.CUSTOMER_ID = lg_wd.CUSTOMER_ID
left join balances bal
    on c.CUSTOMER_ID = bal.CUSTOMER_ID
left join credit
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
