{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID, ACCOUNT_ID)'
    )
}}

-- Per-customer/account transaction aggregates over a configurable lookback.
-- Port of bteq/02_stg_txn_summary.bteq; output contract is
-- ETL_STAGING_DB.STG_TXN_SUMMARY in ddl/01_staging_tables.sql.

with run_params as (

    -- Replaces the VT_RUN_PARAMS volatile table; lookback comes from a dbt var
    select
        add_months(CURRENT_DATE, -{{ var('lookback_months') }}) as PERIOD_START,
        CURRENT_DATE                                            as PERIOD_END
    from (select 1 as ONE) d

),

top_category as (

    -- Top merchant category per account by total absolute spend in the window
    select
        t2.ACCOUNT_ID,
        t2.MERCHANT_CATEGORY
    from {{ source('txn_processing', 'TRANSACTIONS') }} t2
    inner join run_params rp2
        on t2.TRANSACTION_DATE between rp2.PERIOD_START and rp2.PERIOD_END
    where t2.STATUS_CODE = 'P'
      and t2.MERCHANT_CATEGORY is not null
    qualify row_number() over (
        partition by t2.ACCOUNT_ID
        order by sum(abs(t2.AMOUNT)) over (
            partition by t2.ACCOUNT_ID, t2.MERCHANT_CATEGORY
        ) desc
    ) = 1

)

select
    acct.CUSTOMER_ID,
    acct.ACCOUNT_ID,
    acct.ACCOUNT_TYPE,
    rp.PERIOD_START                                                 as SUMMARY_PERIOD_START,
    rp.PERIOD_END                                                   as SUMMARY_PERIOD_END,

    -- Volume counts
    count(*)                                                        as TXN_COUNT_TOTAL,
    sum(case when tt.CATEGORY = 'DEBIT'  then 1 else 0 end)         as TXN_COUNT_DEBIT,
    sum(case when tt.CATEGORY = 'CREDIT' then 1 else 0 end)         as TXN_COUNT_CREDIT,
    sum(case when tt.CATEGORY = 'FEE'    then 1 else 0 end)         as TXN_COUNT_FEE,

    -- Dollar amounts
    sum(case when tt.CATEGORY = 'DEBIT'
             then abs(t.AMOUNT) else 0 end)                         as AMT_TOTAL_DEBIT,
    sum(case when tt.CATEGORY = 'CREDIT'
             then t.AMOUNT else 0 end)                              as AMT_TOTAL_CREDIT,
    sum(case when tt.CATEGORY = 'FEE'
             then abs(t.AMOUNT) else 0 end)                         as AMT_TOTAL_FEES,

    -- Averages
    avg(case when tt.CATEGORY = 'DEBIT'
             then abs(t.AMOUNT) else null end)                      as AMT_AVG_DEBIT,
    avg(case when tt.CATEGORY = 'CREDIT'
             then t.AMOUNT else null end)                           as AMT_AVG_CREDIT,

    -- Maximums
    max(case when tt.CATEGORY = 'DEBIT'
             then abs(t.AMOUNT) else 0 end)                         as AMT_MAX_SINGLE_DEBIT,
    max(case when tt.CATEGORY = 'CREDIT'
             then t.AMOUNT else 0 end)                              as AMT_MAX_SINGLE_CREDIT,

    -- Merchant diversity and top category
    count(distinct t.MERCHANT_NAME)                                 as DISTINCT_MERCHANTS,
    max(top_cat.MERCHANT_CATEGORY)                                  as TOP_MERCHANT_CATEGORY,

    -- Channel mix
    cast(sum(case when t.CHANNEL_CODE = 'ATM' then 1 else 0 end) * 100.0
         / nullifzero(count(*)) as decimal(5,2))                    as PCT_ATM,
    cast(sum(case when t.CHANNEL_CODE = 'POS' then 1 else 0 end) * 100.0
         / nullifzero(count(*)) as decimal(5,2))                    as PCT_POS,
    cast(sum(case when t.CHANNEL_CODE = 'WEB' then 1 else 0 end) * 100.0
         / nullifzero(count(*)) as decimal(5,2))                    as PCT_WEB,
    cast(sum(case when t.CHANNEL_CODE in ('MOB') then 1 else 0 end) * 100.0
         / nullifzero(count(*)) as decimal(5,2))                    as PCT_MOBILE,

    -- Recency
    cast(CURRENT_DATE - max(t.TRANSACTION_DATE) as integer)         as DAYS_SINCE_LAST_TXN,
    current_timestamp(6)                                            as LOAD_TS

from {{ source('txn_processing', 'TRANSACTIONS') }} t
inner join {{ source('core_banking', 'ACCOUNTS') }} acct
    on t.ACCOUNT_ID = acct.ACCOUNT_ID
inner join {{ source('txn_processing', 'TRANSACTION_TYPES') }} tt
    on t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
cross join run_params rp
left join top_category top_cat
    on t.ACCOUNT_ID = top_cat.ACCOUNT_ID
where t.TRANSACTION_DATE between rp.PERIOD_START and rp.PERIOD_END
  and t.STATUS_CODE = 'P'   -- Posted transactions only
group by
    acct.CUSTOMER_ID,
    acct.ACCOUNT_ID,
    acct.ACCOUNT_TYPE,
    rp.PERIOD_START,
    rp.PERIOD_END,
    top_cat.MERCHANT_CATEGORY
