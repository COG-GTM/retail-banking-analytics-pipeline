{{
    config(
        materialized='table',
        alias='STG_TXN_SUMMARY',
        post_hook=[
            """
            insert into {{ var('etl_staging_database') }}.STAGING.ETL_RUN_LOG
                (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS)
            select '02_stg_txn_summary', 'FULL_LOAD', 'SUCCESS', count(*),
                   '{{ run_started_at }}'::timestamp_ntz, current_timestamp()
            from {{ this }}
            """
        ]
    )
}}

/*
    Snowflake/dbt port of bteq/02_stg_txn_summary.bteq (MBA-2205 / TICKET-04).

    Teradata -> Snowflake mapping:
      VT_RUN_PARAMS volatile table  -> run_params CTE driven by var('lookback_months')
      ADD_MONTHS(CURRENT_DATE, -n)  -> dateadd(month, -n, current_date())
      NULLIFZERO(x)                 -> nullif(x, 0)
      CURRENT_DATE - date           -> datediff(day, date, current_date())
      CURRENT_TIMESTAMP(6)          -> current_timestamp()
      QUALIFY ROW_NUMBER()          -> qualify row_number() (same semantics)
      COLLECT STATISTICS            -> dropped (Snowflake maintains its own stats)
*/

with run_params as (

    select
        dateadd(month, -{{ var('lookback_months') }}, current_date()) as period_start,
        current_date()                                               as period_end

),

posted_txn as (

    select
        t.TRANSACTION_ID,
        t.ACCOUNT_ID,
        t.TRANSACTION_TYPE_CD,
        t.TRANSACTION_DATE,
        t.AMOUNT,
        t.MERCHANT_NAME,
        t.MERCHANT_CATEGORY,
        t.CHANNEL_CODE
    from {{ source('txn_processing', 'transactions') }} t
    cross join run_params rp
    where t.TRANSACTION_DATE between rp.PERIOD_START and rp.PERIOD_END
      and t.STATUS_CODE = 'P'   /* posted transactions only */

),

/* Top merchant category per account by total absolute spend.
   The Teradata version grouped on the correlated sub-select column, which could
   fan the grain out; here the pick is resolved to exactly one row per account. */
top_merchant_category as (

    select
        ACCOUNT_ID,
        MERCHANT_CATEGORY
    from (
        select
            ACCOUNT_ID,
            MERCHANT_CATEGORY,
            sum(abs(AMOUNT)) as CATEGORY_SPEND
        from posted_txn
        where MERCHANT_CATEGORY is not null
        group by ACCOUNT_ID, MERCHANT_CATEGORY
    )
    qualify row_number() over (
        partition by ACCOUNT_ID
        order by CATEGORY_SPEND desc, MERCHANT_CATEGORY
    ) = 1

),

aggregated as (

    select
        acct.CUSTOMER_ID,
        acct.ACCOUNT_ID,
        acct.ACCOUNT_TYPE,
        rp.PERIOD_START                                                as SUMMARY_PERIOD_START,
        rp.PERIOD_END                                                  as SUMMARY_PERIOD_END,

        /* ---- Volume Counts ---- */
        count(*)                                                       as TXN_COUNT_TOTAL,
        sum(iff(tt.CATEGORY = 'DEBIT', 1, 0))                          as TXN_COUNT_DEBIT,
        sum(iff(tt.CATEGORY = 'CREDIT', 1, 0))                         as TXN_COUNT_CREDIT,
        sum(iff(tt.CATEGORY = 'FEE', 1, 0))                            as TXN_COUNT_FEE,
        sum(iff(tt.IS_REVENUE = 'Y', 1, 0))                            as TXN_COUNT_REVENUE,

        /* ---- Dollar Amounts ---- */
        sum(iff(tt.CATEGORY = 'DEBIT', abs(t.AMOUNT), 0))              as AMT_TOTAL_DEBIT,
        sum(iff(tt.CATEGORY = 'CREDIT', t.AMOUNT, 0))                  as AMT_TOTAL_CREDIT,
        sum(iff(tt.CATEGORY = 'FEE', abs(t.AMOUNT), 0))                as AMT_TOTAL_FEES,
        sum(iff(tt.IS_REVENUE = 'Y', abs(t.AMOUNT), 0))                as AMT_TOTAL_REVENUE,

        /* ---- Averages ---- */
        avg(iff(tt.CATEGORY = 'DEBIT', abs(t.AMOUNT), null))           as AMT_AVG_DEBIT,
        avg(iff(tt.CATEGORY = 'CREDIT', t.AMOUNT, null))               as AMT_AVG_CREDIT,

        /* ---- Maximums ---- */
        max(iff(tt.CATEGORY = 'DEBIT', abs(t.AMOUNT), 0))              as AMT_MAX_SINGLE_DEBIT,
        max(iff(tt.CATEGORY = 'CREDIT', t.AMOUNT, 0))                  as AMT_MAX_SINGLE_CREDIT,

        /* ---- Merchant Diversity ---- */
        count(distinct t.MERCHANT_NAME)                                as DISTINCT_MERCHANTS,
        max(tmc.MERCHANT_CATEGORY)                                     as TOP_MERCHANT_CATEGORY,

        /* ---- Channel Mix (percentages sum to 100 per account) ---- */
        cast(sum(iff(t.CHANNEL_CODE = 'ATM', 1, 0)) * 100.0
             / nullif(count(*), 0) as decimal(5, 2))                   as PCT_ATM,
        cast(sum(iff(t.CHANNEL_CODE = 'POS', 1, 0)) * 100.0
             / nullif(count(*), 0) as decimal(5, 2))                   as PCT_POS,
        cast(sum(iff(t.CHANNEL_CODE = 'WEB', 1, 0)) * 100.0
             / nullif(count(*), 0) as decimal(5, 2))                   as PCT_WEB,
        cast(sum(iff(t.CHANNEL_CODE = 'MOB', 1, 0)) * 100.0
             / nullif(count(*), 0) as decimal(5, 2))                   as PCT_MOBILE,
        cast(sum(iff(t.CHANNEL_CODE = 'ACH', 1, 0)) * 100.0
             / nullif(count(*), 0) as decimal(5, 2))                   as PCT_ACH,
        cast(sum(iff(t.CHANNEL_CODE not in ('ATM', 'POS', 'WEB', 'MOB', 'ACH'), 1, 0)) * 100.0
             / nullif(count(*), 0) as decimal(5, 2))                   as PCT_OTHER,

        /* ---- Recency ---- */
        datediff(day, max(t.TRANSACTION_DATE), current_date())         as DAYS_SINCE_LAST_TXN,
        current_timestamp()                                            as LOAD_TS

    from posted_txn t
    inner join {{ source('core_banking', 'accounts') }} acct
        on t.ACCOUNT_ID = acct.ACCOUNT_ID
    inner join {{ source('txn_processing', 'transaction_types') }} tt
        on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
    cross join run_params rp
    left join top_merchant_category tmc
        on t.ACCOUNT_ID = tmc.ACCOUNT_ID
    group by
        acct.CUSTOMER_ID,
        acct.ACCOUNT_ID,
        acct.ACCOUNT_TYPE,
        rp.PERIOD_START,
        rp.PERIOD_END

)

select * from aggregated
