{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID)
        PARTITION BY COLUMN(REPORTING_PERIOD)'
    )
}}

-- Per-customer transaction behaviour with trend, percentile and anomaly flags.
-- Port of sas/02_sas_txn_analytics.sas; output contract is
-- DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS in ddl/02_data_product_tables.sql.
-- PROC RANK groups=100 becomes NTILE(100) - 1 and the PROC MEANS IQR outlier
-- rule becomes PERCENTILE_CONT bounds computed over the whole population.

with customer_txn as (

    select
        CUSTOMER_ID,
        count(distinct ACCOUNT_ID)                                          as TOTAL_ACCOUNTS,
        sum(case when DAYS_SINCE_LAST_TXN <= 30 then 1 else 0 end)          as ACTIVE_ACCOUNTS,
        sum(TXN_COUNT_TOTAL)                                                as TOTAL_TRANSACTIONS,
        sum(AMT_TOTAL_DEBIT)                                                as TOTAL_DEBIT_AMT,
        sum(AMT_TOTAL_CREDIT)                                               as TOTAL_CREDIT_AMT,
        sum(AMT_TOTAL_CREDIT) - sum(AMT_TOTAL_DEBIT)                        as NET_CASH_FLOW,
        case when sum(TXN_COUNT_TOTAL) > 0
             then sum(AMT_TOTAL_DEBIT + AMT_TOTAL_CREDIT) / sum(TXN_COUNT_TOTAL)
             else 0 end                                                     as AVG_TRANSACTION_SIZE,
        sum(AMT_TOTAL_FEES)                                                 as TOTAL_FEES,
        max(TOP_MERCHANT_CATEGORY)                                          as TOP_SPEND_CATEGORY,
        -- Transaction-weighted share of WEB + MOB activity
        case when sum(TXN_COUNT_TOTAL) > 0
             then sum(TXN_COUNT_TOTAL * (coalesce(PCT_WEB, 0) + coalesce(PCT_MOBILE, 0)) / 100)
                  / sum(TXN_COUNT_TOTAL) * 100
             else 0 end                                                     as DIGITAL_TXN_PCT
    from {{ ref('stg_txn_summary') }}
    group by CUSTOMER_ID

),

spend_distribution as (

    -- Population statistics behind the PROC MEANS IQR anomaly rule
    select
        percentile_cont(0.5)  within group (order by TOTAL_DEBIT_AMT)   as MEDIAN_DEBIT,
        percentile_cont(0.75) within group (order by TOTAL_DEBIT_AMT)
            - percentile_cont(0.25) within group (order by TOTAL_DEBIT_AMT) as IQR_DEBIT
    from customer_txn

),

scored as (

    select
        t.CUSTOMER_ID,
        t.TOTAL_ACCOUNTS,
        t.ACTIVE_ACCOUNTS,
        t.TOTAL_TRANSACTIONS,
        t.TOTAL_DEBIT_AMT,
        t.TOTAL_CREDIT_AMT,
        t.NET_CASH_FLOW,
        t.AVG_TRANSACTION_SIZE,
        t.TOTAL_FEES,
        t.TOP_SPEND_CATEGORY,
        t.DIGITAL_TXN_PCT,

        -- Spend trend relative to average transaction size
        case
            when t.NET_CASH_FLOW >  t.AVG_TRANSACTION_SIZE * 5 then 'UP'
            when t.NET_CASH_FLOW < -t.AVG_TRANSACTION_SIZE * 5 then 'DOWN'
            else 'STABLE'
        end                                                                 as MONTHLY_SPEND_TREND,

        -- PROC RANK groups=100 produced 0-99 percentile buckets
        ntile(100) over (order by t.TOTAL_DEBIT_AMT) - 1                    as SPEND_PERCENTILE,

        -- Revenue components
        t.TOTAL_FEES                                                        as FEE_INCOME,
        t.TOTAL_DEBIT_AMT * 0.02                                            as INTEREST_INCOME,
        t.TOTAL_FEES + t.TOTAL_DEBIT_AMT * 0.02                             as REVENUE_CONTRIBUTION,

        -- Spend beyond median + 3 * IQR is flagged as anomalous
        case
            when d.IQR_DEBIT > 0
             and t.TOTAL_DEBIT_AMT > d.MEDIAN_DEBIT + (3 * d.IQR_DEBIT)
            then 'Y' else 'N'
        end                                                                 as ANOMALY_FLAG

    from customer_txn t
    cross join spend_distribution d

)

select
    CUSTOMER_ID,
    cast(cast(CURRENT_DATE as format 'YYYY-MM') as char(7))     as REPORTING_PERIOD,
    cast(TOTAL_ACCOUNTS as smallint)                            as TOTAL_ACCOUNTS,
    cast(ACTIVE_ACCOUNTS as smallint)                           as ACTIVE_ACCOUNTS,
    cast(TOTAL_TRANSACTIONS as integer)                         as TOTAL_TRANSACTIONS,
    cast(TOTAL_DEBIT_AMT as decimal(18,2))                      as TOTAL_DEBIT_AMT,
    cast(TOTAL_CREDIT_AMT as decimal(18,2))                     as TOTAL_CREDIT_AMT,
    cast(NET_CASH_FLOW as decimal(18,2))                        as NET_CASH_FLOW,
    cast(AVG_TRANSACTION_SIZE as decimal(15,2))                 as AVG_TRANSACTION_SIZE,
    MONTHLY_SPEND_TREND,
    cast(SPEND_PERCENTILE as decimal(5,2))                      as SPEND_PERCENTILE,
    TOP_SPEND_CATEGORY,
    cast(DIGITAL_TXN_PCT as decimal(5,2))                       as DIGITAL_TXN_PCT,
    cast(FEE_INCOME as decimal(15,2))                           as FEE_INCOME,
    cast(INTEREST_INCOME as decimal(15,2))                      as INTEREST_INCOME,
    cast(REVENUE_CONTRIBUTION as decimal(15,2))                 as REVENUE_CONTRIBUTION,
    ANOMALY_FLAG,
    cast('{{ var("txn_model_version") }}' as varchar(20))       as MODEL_VERSION,
    CURRENT_DATE                                                as EFFECTIVE_DATE,
    current_timestamp(6)                                        as LOAD_TS
from scored
