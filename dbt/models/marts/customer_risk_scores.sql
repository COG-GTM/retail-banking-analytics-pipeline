{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID)'
    )
}}

-- Composite customer risk scoring data product.
-- Ports the deterministic parts of sas/03_sas_risk_scoring.sas: the risk
-- component breakdown, the weighted composite score, tier bucketing, primary
-- and secondary risk drivers and the watch-list / review flags.
--
-- EXTERNAL SCORING BOUNDARY - PROC LOGISTIC has no dbt SQL equivalent, so
-- PROBABILITY_OF_DEFAULT is read from the customer_default_probabilities
-- hand-off table (in-database via `dbt run-operation run_td_glm_default_scores`
-- or loaded from an external scoring job with `dbt seed`). Unscored customers
-- get 0, mirroring the SAS coalesce(PROB_DEFAULT, 0).
--
-- Output contract: DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES in
-- ddl/02_data_product_tables.sql.

with features as (

    select * from {{ ref('int_customer_risk_features') }}

),

default_probability as (

    select
        CUSTOMER_ID,
        PROBABILITY_OF_DEFAULT,
        MODEL_VERSION
    from {{ ref('customer_default_probabilities') }}
    qualify row_number() over (
        partition by CUSTOMER_ID order by SCORED_AT desc
    ) = 1

),

components as (

    select
        f.CUSTOMER_ID,
        f.VELOCITY_RATIO,

        -- Each component is clamped to the 0-100 scale
        case when 100 - f.BUREAU_SCORE_NORM < 0 then 0
             when 100 - f.BUREAU_SCORE_NORM > 100 then 100
             else 100 - f.BUREAU_SCORE_NORM end             as CREDIT_RISK_COMPONENT,
        case when 100 - f.PAYMENT_ONTIME_PCT < 0 then 0
             when 100 - f.PAYMENT_ONTIME_PCT > 100 then 100
             else 100 - f.PAYMENT_ONTIME_PCT end            as BEHAVIOUR_RISK_COMPONENT,
        case when (f.VELOCITY_RATIO - 1) * 50 < 0 then 0
             when (f.VELOCITY_RATIO - 1) * 50 > 100 then 100
             else (f.VELOCITY_RATIO - 1) * 50 end           as VELOCITY_RISK_COMPONENT,
        case when f.BUREAU_SCORE_NORM < 0 then 0
             when f.BUREAU_SCORE_NORM > 100 then 100
             else f.BUREAU_SCORE_NORM end                   as BUREAU_SCORE_COMPONENT,
        case when f.PAYMENT_ONTIME_PCT < 0 then 0
             when f.PAYMENT_ONTIME_PCT > 100 then 100
             else f.PAYMENT_ONTIME_PCT end                  as PAYMENT_HISTORY_COMPONENT,

        coalesce(pd.PROBABILITY_OF_DEFAULT, 0)              as PROBABILITY_OF_DEFAULT,
        pd.MODEL_VERSION                                    as SCORING_MODEL_VERSION
    from features f
    left join default_probability pd
        on f.CUSTOMER_ID = pd.CUSTOMER_ID

),

scored as (

    select
        components.*,
        round(
            CREDIT_RISK_COMPONENT              * 0.30 +
            BEHAVIOUR_RISK_COMPONENT           * 0.25 +
            VELOCITY_RISK_COMPONENT            * 0.15 +
            (100 - BUREAU_SCORE_COMPONENT)     * 0.20 +
            (100 - PAYMENT_HISTORY_COMPONENT)  * 0.10
        , 2)                                                as COMPOSITE_RISK_SCORE
    from components

),

driver_candidates as (

    -- The SAS data step scanned four candidate drivers in a fixed order and
    -- kept the top two; ties resolve to the earlier candidate.
    select CUSTOMER_ID, 1 as DRIVER_ORD, 'CREDIT_UTILIZATION'   as DRIVER_LABEL,
           CREDIT_RISK_COMPONENT          as DRIVER_VALUE from scored
    union all
    select CUSTOMER_ID, 2, 'PAYMENT_BEHAVIOUR',
           BEHAVIOUR_RISK_COMPONENT                        from scored
    union all
    select CUSTOMER_ID, 3, 'TRANSACTION_VELOCITY',
           VELOCITY_RISK_COMPONENT                         from scored
    union all
    select CUSTOMER_ID, 4, 'BUREAU_SCORE',
           100 - BUREAU_SCORE_COMPONENT                    from scored

),

drivers as (

    select
        CUSTOMER_ID,
        max(case when DRIVER_RANK = 1 then DRIVER_LABEL end) as PRIMARY_RISK_DRIVER,
        max(case when DRIVER_RANK = 2 then DRIVER_LABEL end) as SECONDARY_RISK_DRIVER
    from (
        select
            CUSTOMER_ID,
            DRIVER_LABEL,
            row_number() over (
                partition by CUSTOMER_ID
                order by DRIVER_VALUE desc, DRIVER_ORD asc
            ) as DRIVER_RANK
        from driver_candidates
    ) ranked
    group by CUSTOMER_ID

)

select
    s.CUSTOMER_ID,
    cast(s.COMPOSITE_RISK_SCORE as decimal(6,2))        as COMPOSITE_RISK_SCORE,
    case
        when s.COMPOSITE_RISK_SCORE < 20 then 'LOW'
        when s.COMPOSITE_RISK_SCORE < 40 then 'MODERATE'
        when s.COMPOSITE_RISK_SCORE < 60 then 'ELEVATED'
        when s.COMPOSITE_RISK_SCORE < 80 then 'HIGH'
        else 'CRITICAL'
    end                                                 as RISK_TIER,
    cast(round(s.PROBABILITY_OF_DEFAULT, 6) as decimal(7,6)) as PROBABILITY_OF_DEFAULT,
    cast(s.CREDIT_RISK_COMPONENT as decimal(5,2))       as CREDIT_RISK_COMPONENT,
    cast(s.BEHAVIOUR_RISK_COMPONENT as decimal(5,2))    as BEHAVIOUR_RISK_COMPONENT,
    cast(s.VELOCITY_RISK_COMPONENT as decimal(5,2))     as VELOCITY_RISK_COMPONENT,
    cast(s.BUREAU_SCORE_COMPONENT as decimal(5,2))      as BUREAU_SCORE_COMPONENT,
    cast(s.PAYMENT_HISTORY_COMPONENT as decimal(5,2))   as PAYMENT_HISTORY_COMPONENT,
    d.PRIMARY_RISK_DRIVER,
    d.SECONDARY_RISK_DRIVER,
    -- Placeholder in the SAS program; would compare against the prior run
    cast(0 as decimal(6,2))                             as SCORE_DELTA_30D,
    -- CRITICAL tier (composite >= 80) with a high modelled default probability
    case
        when s.COMPOSITE_RISK_SCORE >= 80 and s.PROBABILITY_OF_DEFAULT > 0.5
        then 'Y' else 'N'
    end                                                 as WATCH_LIST_FLAG,
    case
        when s.COMPOSITE_RISK_SCORE >= 60 and s.VELOCITY_RATIO > 2.0
        then 'Y' else 'N'
    end                                                 as REVIEW_REQUIRED_FLAG,
    cast(coalesce(s.SCORING_MODEL_VERSION, '{{ var("risk_model_version") }}')
         as varchar(20))                                as MODEL_VERSION,
    CURRENT_DATE                                        as EFFECTIVE_DATE,
    current_timestamp(6)                                as LOAD_TS
from scored s
inner join drivers d
    on s.CUSTOMER_ID = d.CUSTOMER_ID
