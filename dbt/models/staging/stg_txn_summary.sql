{{
    config(
        materialized='table',
        alias='STG_TXN_SUMMARY',
        cluster_by=['CUSTOMER_ID', 'ACCOUNT_ID'],
        post_hook="{{ log_etl_run('02_stg_txn_summary') }}"
    )
}}

/*
    Snowflake port of bteq/02_stg_txn_summary.bteq.

    Aggregates posted transactions per customer/account over a configurable
    lookback window. The Teradata volatile table VT_RUN_PARAMS is replaced by
    the `lookback_months` dbt var, so the window can be changed at run time
    with `dbt run --vars '{lookback_months: 24}'` and no code edit.
*/

with run_params as (

    select
        dateadd(month, -{{ var('lookback_months') }}, current_date) as PERIOD_START,
        current_date                                               as PERIOD_END

),

posted_txn as (

    select
        t.ACCOUNT_ID,
        t.TRANSACTION_DATE,
        t.AMOUNT,
        t.MERCHANT_NAME,
        t.MERCHANT_CATEGORY,
        t.CHANNEL_CODE,
        tt.CATEGORY    as TXN_CATEGORY,
        tt.IS_REVENUE  as IS_REVENUE
    from {{ source('txn_processing', 'transactions') }} t
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
    cross join run_params rp
    where t.TRANSACTION_DATE between rp.PERIOD_START and rp.PERIOD_END
      and t.STATUS_CODE = '{{ var('txn_posted_status_code') }}'

),

top_merchant_category as (

    /* Highest-spend merchant category per account within the lookback window.
       Replaces the Teradata QUALIFY-over-windowed-SUM subquery; ties are broken
       alphabetically so the result is deterministic. */
    select
        ACCOUNT_ID,
        MERCHANT_CATEGORY
    from posted_txn
    where MERCHANT_CATEGORY is not null
    group by ACCOUNT_ID, MERCHANT_CATEGORY
    qualify row_number() over (
        partition by ACCOUNT_ID
        order by sum(abs(AMOUNT)) desc, MERCHANT_CATEGORY
    ) = 1

),

aggregated as (

    select
        acct.CUSTOMER_ID                                                    as CUSTOMER_ID,
        acct.ACCOUNT_ID                                                     as ACCOUNT_ID,
        acct.ACCOUNT_TYPE                                                   as ACCOUNT_TYPE,
        rp.PERIOD_START                                                     as SUMMARY_PERIOD_START,
        rp.PERIOD_END                                                       as SUMMARY_PERIOD_END,

        /* ---- Volume Counts ---- */
        count(*)                                                            as TXN_COUNT_TOTAL,
        sum(iff(pt.TXN_CATEGORY = 'DEBIT', 1, 0))                           as TXN_COUNT_DEBIT,
        sum(iff(pt.TXN_CATEGORY = 'CREDIT', 1, 0))                          as TXN_COUNT_CREDIT,
        sum(iff(pt.TXN_CATEGORY = 'FEE', 1, 0))                             as TXN_COUNT_FEE,
        sum(iff(pt.IS_REVENUE = 'Y', 1, 0))                                 as TXN_COUNT_REVENUE,

        /* ---- Dollar Amounts ---- */
        cast(sum(iff(pt.TXN_CATEGORY = 'DEBIT', abs(pt.AMOUNT), 0))
             as number(18, 2))                                              as AMT_TOTAL_DEBIT,
        cast(sum(iff(pt.TXN_CATEGORY = 'CREDIT', pt.AMOUNT, 0))
             as number(18, 2))                                              as AMT_TOTAL_CREDIT,
        cast(sum(iff(pt.TXN_CATEGORY = 'FEE', abs(pt.AMOUNT), 0))
             as number(18, 2))                                              as AMT_TOTAL_FEES,
        cast(sum(iff(pt.IS_REVENUE = 'Y', abs(pt.AMOUNT), 0))
             as number(18, 2))                                              as AMT_TOTAL_REVENUE,

        /* ---- Averages ---- */
        cast(avg(iff(pt.TXN_CATEGORY = 'DEBIT', abs(pt.AMOUNT), null))
             as number(15, 2))                                              as AMT_AVG_DEBIT,
        cast(avg(iff(pt.TXN_CATEGORY = 'CREDIT', pt.AMOUNT, null))
             as number(15, 2))                                              as AMT_AVG_CREDIT,

        /* ---- Maximums ---- */
        cast(max(iff(pt.TXN_CATEGORY = 'DEBIT', abs(pt.AMOUNT), 0))
             as number(15, 2))                                              as AMT_MAX_SINGLE_DEBIT,
        cast(max(iff(pt.TXN_CATEGORY = 'CREDIT', pt.AMOUNT, 0))
             as number(15, 2))                                              as AMT_MAX_SINGLE_CREDIT,

        /* ---- Merchant Diversity ---- */
        count(distinct pt.MERCHANT_NAME)                                    as DISTINCT_MERCHANTS,
        max(tmc.MERCHANT_CATEGORY)                                          as TOP_MERCHANT_CATEGORY,

        /* ---- Channel Mix (NULLIFZERO -> NULLIF(x, 0)) ---- */
        cast(sum(iff(pt.CHANNEL_CODE = 'ATM', 1, 0)) * 100.0
             / nullif(count(*), 0) as number(5, 2))                         as PCT_ATM,
        cast(sum(iff(pt.CHANNEL_CODE = 'POS', 1, 0)) * 100.0
             / nullif(count(*), 0) as number(5, 2))                         as PCT_POS,
        cast(sum(iff(pt.CHANNEL_CODE = 'WEB', 1, 0)) * 100.0
             / nullif(count(*), 0) as number(5, 2))                         as PCT_WEB,
        cast(sum(iff(pt.CHANNEL_CODE = 'MOB', 1, 0)) * 100.0
             / nullif(count(*), 0) as number(5, 2))                         as PCT_MOBILE,
        cast(sum(iff(pt.CHANNEL_CODE not in ('ATM', 'POS', 'WEB', 'MOB'), 1, 0)) * 100.0
             / nullif(count(*), 0) as number(5, 2))                         as PCT_OTHER_CHANNEL,

        /* ---- Recency ---- */
        datediff(day, max(pt.TRANSACTION_DATE), rp.PERIOD_END)              as DAYS_SINCE_LAST_TXN,
        current_timestamp()                                                 as LOAD_TS

    from posted_txn pt
    inner join {{ source('core_banking', 'accounts') }} acct
        on pt.ACCOUNT_ID = acct.ACCOUNT_ID
    left join top_merchant_category tmc
        on pt.ACCOUNT_ID = tmc.ACCOUNT_ID
    cross join run_params rp
    group by
        acct.CUSTOMER_ID,
        acct.ACCOUNT_ID,
        acct.ACCOUNT_TYPE,
        rp.PERIOD_START,
        rp.PERIOD_END

)

select * from aggregated
