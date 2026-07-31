{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID)'
    )
}}

-- Deterministic segmentation features for active customers, plus the
-- standardized copies of the six clustering inputs.
-- Ports STEP 2 (feature engineering) and STEP 3 (PROC STDIZE method=std) of
-- sas/01_sas_customer_segments.sas. This model is the input contract for the
-- clustering step, which runs outside dbt - see customer_segments.

with base as (

    select
        CUSTOMER_ID,
        AGE,
        TENURE_MONTHS,
        CUSTOMER_STATUS,
        SEGMENT_CODE,
        STATE_CODE,
        NUM_ACCOUNTS,
        NUM_ACTIVE_ACCOUNTS,
        HAS_CHECKING,
        HAS_SAVINGS,
        HAS_CREDIT,
        HAS_LOAN,
        TOTAL_BALANCE,
        TOTAL_CREDIT_LIMIT,
        CREDIT_UTILIZATION_PCT
    from {{ ref('stg_customer_360') }}
    where CUSTOMER_STATUS = 'A'

),

features as (

    select
        CUSTOMER_ID,
        AGE,
        TENURE_MONTHS,
        NUM_ACCOUNTS,
        NUM_ACTIVE_ACCOUNTS,
        TOTAL_BALANCE,
        TOTAL_CREDIT_LIMIT,
        CREDIT_UTILIZATION_PCT,

        -- Proportion of the four product types held
        (case when HAS_CHECKING = 'Y' then 1 else 0 end
       + case when HAS_SAVINGS  = 'Y' then 1 else 0 end
       + case when HAS_CREDIT   = 'Y' then 1 else 0 end
       + case when HAS_LOAN     = 'Y' then 1 else 0 end) / 4.0    as PRODUCT_BREADTH,

        case
            when TENURE_MONTHS < 12 then 'NEW (<1yr)'
            when TENURE_MONTHS < 36 then 'DEVELOPING (1-3yr)'
            when TENURE_MONTHS < 84 then 'ESTABLISHED (3-7yr)'
            else 'LOYAL (7yr+)'
        end                                                       as TENURE_GROUP,

        case
            when AGE < 25 then 'GEN_Z'
            when AGE < 41 then 'MILLENNIAL'
            when AGE < 57 then 'GEN_X'
            when AGE < 76 then 'BOOMER'
            else 'SILENT'
        end                                                       as AGE_GROUP,

        case
            when TOTAL_BALANCE < 1000   then 'LOW'
            when TOTAL_BALANCE < 10000  then 'MODERATE'
            when TOTAL_BALANCE < 100000 then 'AFFLUENT'
            else 'HIGH_NET_WORTH'
        end                                                       as BALANCE_TIER,

        -- Placeholder in the SAS program; enriched later by txn analytics
        0.00                                                      as DIGITAL_ADOPTION_SCORE,

        -- SAS log() is the natural logarithm, so LN() (Teradata LOG is base 10)
        ln(case when TOTAL_BALANCE > 1 then TOTAL_BALANCE else 1 end)  as LOG_BALANCE,
        NUM_ACTIVE_ACCOUNTS
            / cast(case when NUM_ACCOUNTS > 1 then NUM_ACCOUNTS else 1 end as decimal(9,4))
                                                                  as ACCT_RATIO
    from base

),

moments as (

    -- PROC STDIZE method=std centres on the mean and scales by the sample
    -- standard deviation
    select
        avg(LOG_BALANCE)                    as MEAN_LOG_BALANCE,
        nullifzero(stddev_samp(LOG_BALANCE))            as SD_LOG_BALANCE,
        avg(TENURE_MONTHS)                  as MEAN_TENURE_MONTHS,
        nullifzero(stddev_samp(TENURE_MONTHS))          as SD_TENURE_MONTHS,
        avg(CREDIT_UTILIZATION_PCT)         as MEAN_CREDIT_UTIL,
        nullifzero(stddev_samp(CREDIT_UTILIZATION_PCT)) as SD_CREDIT_UTIL,
        avg(PRODUCT_BREADTH)                as MEAN_PRODUCT_BREADTH,
        nullifzero(stddev_samp(PRODUCT_BREADTH))        as SD_PRODUCT_BREADTH,
        avg(ACCT_RATIO)                     as MEAN_ACCT_RATIO,
        nullifzero(stddev_samp(ACCT_RATIO))             as SD_ACCT_RATIO,
        avg(AGE)                            as MEAN_AGE,
        nullifzero(stddev_samp(AGE))                    as SD_AGE
    from features

)

select
    f.CUSTOMER_ID,
    f.AGE,
    f.TENURE_MONTHS,
    f.NUM_ACCOUNTS,
    f.NUM_ACTIVE_ACCOUNTS,
    f.TOTAL_BALANCE,
    f.TOTAL_CREDIT_LIMIT,
    f.CREDIT_UTILIZATION_PCT,
    f.PRODUCT_BREADTH,
    f.TENURE_GROUP,
    f.AGE_GROUP,
    f.BALANCE_TIER,
    f.DIGITAL_ADOPTION_SCORE,
    f.LOG_BALANCE,
    f.ACCT_RATIO,

    -- Standardized clustering inputs
    (f.LOG_BALANCE - m.MEAN_LOG_BALANCE)                    / m.SD_LOG_BALANCE      as STD_LOG_BALANCE,
    (f.TENURE_MONTHS - m.MEAN_TENURE_MONTHS)                / m.SD_TENURE_MONTHS    as STD_TENURE_MONTHS,
    (f.CREDIT_UTILIZATION_PCT - m.MEAN_CREDIT_UTIL)         / m.SD_CREDIT_UTIL      as STD_CREDIT_UTILIZATION_PCT,
    (f.PRODUCT_BREADTH - m.MEAN_PRODUCT_BREADTH)            / m.SD_PRODUCT_BREADTH  as STD_PRODUCT_BREADTH,
    (f.ACCT_RATIO - m.MEAN_ACCT_RATIO)                      / m.SD_ACCT_RATIO       as STD_ACCT_RATIO,
    (f.AGE - m.MEAN_AGE)                                    / m.SD_AGE              as STD_AGE
from features f
cross join moments m
