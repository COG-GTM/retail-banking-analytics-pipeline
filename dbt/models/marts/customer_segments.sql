{{
    config(
        materialized='table',
        index='PRIMARY INDEX (CUSTOMER_ID)'
    )
}}

-- Behavioural customer segmentation data product.
-- Ports the deterministic parts of sas/01_sas_customer_segments.sas: feature
-- engineering (int_customer_segment_features), the LTV and engagement score
-- heuristics, and the cross-sell / upsell / retention-risk flags.
--
-- The k-means step (PROC FASTCLUS) has no dbt SQL equivalent, so cluster
-- assignment is an external boundary: SEGMENT_ID / SEGMENT_NAME /
-- SUBSEGMENT_ID come from the customer_segment_assignments hand-off table.
-- Customers with no assignment fall back to UNCLASSIFIED, so the DAG still
-- runs before the first scoring pass. See the model description in
-- models/marts/_models.yml and macros/td_kmeans_segments.sql.

with features as (

    select * from {{ ref('int_customer_segment_features') }}

),

assignments as (

    select
        CUSTOMER_ID,
        SEGMENT_ID,
        SEGMENT_NAME,
        SUBSEGMENT_ID,
        MODEL_VERSION
    from {{ ref('customer_segment_assignments') }}
    qualify row_number() over (
        partition by CUSTOMER_ID order by SCORED_AT desc
    ) = 1

)

select
    f.CUSTOMER_ID,
    coalesce(a.SEGMENT_NAME, 'UNCLASSIFIED')                    as SEGMENT_NAME,
    cast(a.SEGMENT_ID as smallint)                              as SEGMENT_ID,
    cast(coalesce(a.SUBSEGMENT_ID, 0) as smallint)              as SUBSEGMENT_ID,

    -- Lifetime value heuristic: balance * tenure * breadth
    cast(round(f.LOG_BALANCE * f.TENURE_MONTHS * f.PRODUCT_BREADTH * 10, 2)
         as decimal(10,2))                                      as LIFETIME_VALUE_SCORE,
    cast(round(f.ACCT_RATIO * 100, 2) as decimal(5,2))          as ENGAGEMENT_SCORE,
    cast(f.DIGITAL_ADOPTION_SCORE as decimal(5,2))              as DIGITAL_ADOPTION_SCORE,
    cast(round(f.PRODUCT_BREADTH * 100, 2) as decimal(5,2))     as PRODUCT_BREADTH_INDEX,

    f.TENURE_GROUP,
    f.AGE_GROUP,
    f.BALANCE_TIER,

    -- Placeholder in the SAS program; enriched later by txn analytics
    cast('' as varchar(10))                                     as CHANNEL_PREFERENCE,

    -- Cross-sell if low product breadth but good engagement
    case when f.PRODUCT_BREADTH < 0.50 and f.ACCT_RATIO >= 0.75
         then 'Y' else 'N' end                                  as CROSS_SELL_FLAG,
    -- Upsell if moderate balance with room to grow
    case when f.BALANCE_TIER = 'MODERATE' and f.TENURE_GROUP <> 'NEW (<1yr)'
         then 'Y' else 'N' end                                  as UPSELL_FLAG,
    -- Retention risk if low engagement despite long tenure
    case when f.ACCT_RATIO < 0.50 and f.TENURE_MONTHS >= 60
         then 'Y' else 'N' end                                  as RETENTION_RISK_FLAG,

    cast(coalesce(a.MODEL_VERSION, '{{ var("segment_model_version") }}')
         as varchar(20))                                        as MODEL_VERSION,
    CURRENT_DATE                                                as EFFECTIVE_DATE,
    current_timestamp(6)                                        as LOAD_TS
from features f
left join assignments a
    on f.CUSTOMER_ID = a.CUSTOMER_ID
