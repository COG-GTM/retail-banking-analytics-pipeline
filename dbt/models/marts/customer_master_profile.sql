{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID)'
    )
}}

-- Golden record: the canonical customer view joining the three data products
-- onto the customer base. Port of sas/04_sas_data_products.sas, whose four-way
-- data-step MERGE (`if _base`) becomes LEFT JOINs from stg_customer_360; the
-- ref() dependencies make dbt build this model last.
-- Output contract: DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE in
-- ddl/02_data_product_tables.sql.

with base as (

    select
        CUSTOMER_ID,
        trim(FIRST_NAME) || ' ' || trim(LAST_NAME)  as FULL_NAME,
        AGE,
        STATE_CODE,
        CUSTOMER_SINCE,
        TENURE_MONTHS,
        CUSTOMER_STATUS,
        NUM_ACCOUNTS                                as TOTAL_ACCOUNTS,
        NUM_ACTIVE_ACCOUNTS                         as ACTIVE_ACCOUNTS,
        TOTAL_BALANCE,
        TOTAL_CREDIT_LIMIT,
        CREDIT_UTILIZATION_PCT
    from {{ ref('stg_customer_360') }}
    where CUSTOMER_STATUS = 'A'

),

segments as (

    select
        CUSTOMER_ID,
        SEGMENT_NAME,
        LIFETIME_VALUE_SCORE,
        ENGAGEMENT_SCORE,
        CROSS_SELL_FLAG,
        UPSELL_FLAG,
        RETENTION_RISK_FLAG
    from {{ ref('customer_segments') }}

),

txn as (

    -- Current-period analytics only, as in the SAS extract
    select
        CUSTOMER_ID,
        TOTAL_TRANSACTIONS  as MONTHLY_TRANSACTIONS,
        TOTAL_DEBIT_AMT     as MONTHLY_SPEND,
        NET_CASH_FLOW,
        TOP_SPEND_CATEGORY,
        DIGITAL_TXN_PCT
    from {{ ref('transaction_analytics') }}
    where EFFECTIVE_DATE = CURRENT_DATE

),

risk as (

    select
        CUSTOMER_ID,
        COMPOSITE_RISK_SCORE,
        RISK_TIER,
        PROBABILITY_OF_DEFAULT,
        WATCH_LIST_FLAG
    from {{ ref('customer_risk_scores') }}

)

select
    b.CUSTOMER_ID,
    cast(b.FULL_NAME as varchar(120))                       as FULL_NAME,
    b.AGE,
    b.STATE_CODE,
    b.CUSTOMER_SINCE,
    b.TENURE_MONTHS,
    b.CUSTOMER_STATUS,

    -- Segment data; defaults apply when the customer was never segmented
    coalesce(s.SEGMENT_NAME, 'UNCLASSIFIED')                as SEGMENT_NAME,
    coalesce(s.LIFETIME_VALUE_SCORE, 0.00)                  as LIFETIME_VALUE_SCORE,
    coalesce(s.ENGAGEMENT_SCORE, 0.00)                      as ENGAGEMENT_SCORE,

    -- Account summary
    b.TOTAL_ACCOUNTS,
    b.ACTIVE_ACCOUNTS,
    b.TOTAL_BALANCE,
    b.TOTAL_CREDIT_LIMIT,
    b.CREDIT_UTILIZATION_PCT,

    -- Transaction summary; zeros when there is no current-period activity
    coalesce(t.MONTHLY_TRANSACTIONS, 0)                     as MONTHLY_TRANSACTIONS,
    coalesce(t.MONTHLY_SPEND, 0.00)                         as MONTHLY_SPEND,
    coalesce(t.NET_CASH_FLOW, 0.00)                         as NET_CASH_FLOW,
    coalesce(t.TOP_SPEND_CATEGORY, '')                      as TOP_SPEND_CATEGORY,
    coalesce(t.DIGITAL_TXN_PCT, 0.00)                       as DIGITAL_TXN_PCT,

    -- Risk profile; score and probability stay NULL when the customer is unscored
    r.COMPOSITE_RISK_SCORE,
    coalesce(r.RISK_TIER, 'UNKNOWN')                        as RISK_TIER,
    r.PROBABILITY_OF_DEFAULT,
    coalesce(r.WATCH_LIST_FLAG, 'N')                        as WATCH_LIST_FLAG,

    -- Actionable flags
    coalesce(s.CROSS_SELL_FLAG, 'N')                        as CROSS_SELL_FLAG,
    coalesce(s.UPSELL_FLAG, 'N')                            as UPSELL_FLAG,
    coalesce(s.RETENTION_RISK_FLAG, 'N')                    as RETENTION_RISK_FLAG,

    cast('{{ var("master_model_version") }}' as varchar(20)) as MODEL_VERSION,
    CURRENT_DATE                                            as EFFECTIVE_DATE,
    current_timestamp(6)                                    as LOAD_TS
from base b
left join segments s
    on b.CUSTOMER_ID = s.CUSTOMER_ID
left join txn t
    on b.CUSTOMER_ID = t.CUSTOMER_ID
left join risk r
    on b.CUSTOMER_ID = r.CUSTOMER_ID
