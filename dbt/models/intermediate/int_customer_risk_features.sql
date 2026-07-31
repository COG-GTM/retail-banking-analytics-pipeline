{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID)'
    )
}}

-- Risk model features for active customers: staging risk factors joined to
-- customer attributes, with bureau-score imputation and the derived ratios.
-- Ports STEP 1 and STEP 2 of sas/03_sas_risk_scoring.sas. This model is the
-- input contract for the probability-of-default scoring step, which cannot run
-- in dbt SQL - see customer_risk_scores.

with risk_raw as (

    select
        r.CUSTOMER_ID,
        r.ACCOUNT_OVERDRAFT_CNT,
        r.NSF_FEE_TOTAL,
        r.LARGE_WITHDRAWAL_CNT,
        r.LARGE_WITHDRAWAL_AMT,
        r.AVG_DAILY_BALANCE_30D,
        r.AVG_DAILY_BALANCE_90D,
        r.BALANCE_VOLATILITY,
        r.CREDIT_UTIL_RATIO,
        r.PAYMENT_ONTIME_PCT,
        r.PAYMENT_LATE_CNT,
        r.MONTHS_SINCE_LAST_LATE,
        r.EXTERNAL_CREDIT_SCORE,
        r.DEBIT_VELOCITY_7D,
        r.DEBIT_VELOCITY_30D,
        r.NEW_MERCHANT_CNT_30D,
        r.INTERNATIONAL_TXN_CNT,
        r.HIGH_RISK_MERCHANT_CNT,
        c.TENURE_MONTHS,
        c.NUM_ACTIVE_ACCOUNTS,
        c.TOTAL_BALANCE,
        c.CUSTOMER_STATUS
    from {{ ref('stg_risk_factors') }} r
    inner join {{ ref('stg_customer_360') }} c
        on r.CUSTOMER_ID = c.CUSTOMER_ID
    where c.CUSTOMER_STATUS = 'A'

),

imputed as (

    select
        risk_raw.*,
        -- Missing or invalid bureau scores fall back to the population median
        case
            when EXTERNAL_CREDIT_SCORE is null or EXTERNAL_CREDIT_SCORE <= 0 then 680
            else EXTERNAL_CREDIT_SCORE
        end as BUREAU_SCORE_IMPUTED
    from risk_raw

)

select
    CUSTOMER_ID,
    ACCOUNT_OVERDRAFT_CNT,
    NSF_FEE_TOTAL,
    LARGE_WITHDRAWAL_CNT,
    LARGE_WITHDRAWAL_AMT,
    AVG_DAILY_BALANCE_30D,
    AVG_DAILY_BALANCE_90D,
    BALANCE_VOLATILITY,
    CREDIT_UTIL_RATIO,
    PAYMENT_ONTIME_PCT,
    PAYMENT_LATE_CNT,
    MONTHS_SINCE_LAST_LATE,
    BUREAU_SCORE_IMPUTED                                as EXTERNAL_CREDIT_SCORE,
    DEBIT_VELOCITY_7D,
    DEBIT_VELOCITY_30D,
    NEW_MERCHANT_CNT_30D,
    INTERNATIONAL_TXN_CNT,
    HIGH_RISK_MERCHANT_CNT,
    TENURE_MONTHS,
    NUM_ACTIVE_ACCOUNTS,
    TOTAL_BALANCE,

    -- Bureau score rescaled from 300-850 onto 0-100
    (BUREAU_SCORE_IMPUTED - 300) / (850 - 300.0) * 100  as BUREAU_SCORE_NORM,

    -- 30-day average balance relative to the 90-day average
    case
        when AVG_DAILY_BALANCE_90D > 0
        then AVG_DAILY_BALANCE_30D / AVG_DAILY_BALANCE_90D
        else 1
    end                                                 as BALANCE_TREND_RATIO,

    -- 7-day debit velocity annualised to a 30-day pace
    case
        when DEBIT_VELOCITY_30D > 0
        then (DEBIT_VELOCITY_7D * (30 / 7.0)) / DEBIT_VELOCITY_30D
        else 1
    end                                                 as VELOCITY_RATIO,

    -- Training target proxy used by the SAS logistic regression
    case when PAYMENT_LATE_CNT > 2 then 1 else 0 end    as DEFAULT_FLAG
from imputed
