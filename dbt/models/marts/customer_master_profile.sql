{{
    config(
        materialized='table',
        tags=['marts', 'data_product', 'golden_record']
    )
}}

-- CUSTOMER_MASTER_PROFILE "golden record" data product.
-- Translated from sas/04_sas_data_products.sas. The PROC SQL extracts plus the
-- DATA-step 4-way MERGE (with IN= flags and default values for missing matches)
-- become a LEFT JOIN from the base customer onto the three products, with
-- COALESCE supplying the same defaults the SAS merge applied.
--   customer_segments / customer_risk_scores are EXTERNAL ML outputs, modelled
--   here as seeds (see dbt/README.md).

with base as (

    select
        customer_id,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        age,
        state_code,
        customer_since,
        tenure_months,
        customer_status,
        num_accounts                                as total_accounts,
        num_active_accounts                         as active_accounts,
        total_balance,
        total_credit_limit,
        credit_utilization_pct
    from {{ ref('stg_customer_360') }}
    where customer_status = 'A'

),

segments as (

    select
        customer_id,
        segment_name,
        lifetime_value_score,
        engagement_score,
        cross_sell_flag,
        upsell_flag,
        retention_risk_flag
    from {{ ref('customer_segments') }}

),

txn as (

    -- Current-period transaction analytics only (matches SAS where today()).
    select
        customer_id,
        total_transactions as monthly_transactions,
        total_debit_amt    as monthly_spend,
        net_cash_flow,
        top_spend_category,
        digital_txn_pct
    from {{ ref('transaction_analytics') }}
    where effective_date = current_date

),

risk as (

    select
        customer_id,
        composite_risk_score,
        risk_tier,
        probability_of_default,
        watch_list_flag
    from {{ ref('customer_risk_scores') }}

)

select
    b.customer_id,
    b.full_name,
    b.age,
    b.state_code,
    b.customer_since,
    b.tenure_months,
    b.customer_status,

    -- Segment data (defaults when no segment match).
    coalesce(s.segment_name, 'UNCLASSIFIED')        as segment_name,
    coalesce(s.lifetime_value_score, 0)             as lifetime_value_score,
    coalesce(s.engagement_score, 0)                 as engagement_score,

    -- Account summary.
    b.total_accounts,
    b.active_accounts,
    b.total_balance,
    b.total_credit_limit,
    b.credit_utilization_pct,

    -- Transaction summary (defaults when no txn match).
    coalesce(t.monthly_transactions, 0)             as monthly_transactions,
    coalesce(t.monthly_spend, 0)                    as monthly_spend,
    coalesce(t.net_cash_flow, 0)                    as net_cash_flow,
    coalesce(t.top_spend_category, '')              as top_spend_category,
    coalesce(t.digital_txn_pct, 0)                  as digital_txn_pct,

    -- Risk profile (score / PD remain NULL when no match, as in the SAS merge).
    r.composite_risk_score,
    coalesce(r.risk_tier, 'UNKNOWN')                as risk_tier,
    r.probability_of_default,
    coalesce(r.watch_list_flag, 'N')                as watch_list_flag,

    -- Actionable flags (default 'N').
    coalesce(s.cross_sell_flag, 'N')                as cross_sell_flag,
    coalesce(s.upsell_flag, 'N')                    as upsell_flag,
    coalesce(s.retention_risk_flag, 'N')            as retention_risk_flag,

    -- Metadata.
    cast('{{ var("model_version_master", "MASTER_V1.5") }}' as varchar(20)) as model_version,
    current_date                                    as effective_date,
    current_timestamp(6)                            as load_ts
from base b
left join segments s on b.customer_id = s.customer_id
left join txn t      on b.customer_id = t.customer_id
left join risk r     on b.customer_id = r.customer_id
