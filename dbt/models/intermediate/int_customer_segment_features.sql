{{
    config(
        materialized='view',
        tags=['intermediate', 'phase2_features', 'ml_input']
    )
}}

-- SQL-only feature engineering for customer segmentation.
-- Translated from sas/01_sas_customer_segments.sas STEP 1 (extract active
-- customers) and STEP 2 (feature engineering). The standardization (PROC STDIZE)
-- and k-means clustering (PROC FASTCLUS) remain an EXTERNAL ML step that consumes
-- this model and writes the customer_segments output (see dbt/README.md).

with cust_360 as (

    select
        customer_id,
        age,
        tenure_months,
        customer_status,
        segment_code,
        state_code,
        num_accounts,
        num_active_accounts,
        has_checking,
        has_savings,
        has_credit,
        has_loan,
        total_balance,
        total_credit_limit,
        credit_utilization_pct
    from {{ ref('stg_customer_360') }}
    where customer_status = 'A'

)

select
    customer_id,
    age,
    tenure_months,
    customer_status,
    segment_code,
    state_code,
    num_accounts,
    num_active_accounts,
    has_checking,
    has_savings,
    has_credit,
    has_loan,
    total_balance,
    total_credit_limit,
    credit_utilization_pct,

    -- Product breadth index: proportion of the 4 product types held
    -- (SAS mean() over the boolean flags).
    (
        (case when has_checking = 'Y' then 1 else 0 end)
      + (case when has_savings  = 'Y' then 1 else 0 end)
      + (case when has_credit   = 'Y' then 1 else 0 end)
      + (case when has_loan     = 'Y' then 1 else 0 end)
    ) / 4.0                                                       as product_breadth,

    -- Tenure grouping.
    case
        when tenure_months < 12 then 'NEW (<1yr)'
        when tenure_months < 36 then 'DEVELOPING (1-3yr)'
        when tenure_months < 84 then 'ESTABLISHED (3-7yr)'
        else 'LOYAL (7yr+)'
    end                                                          as tenure_group,

    -- Age grouping.
    case
        when age < 25 then 'GEN_Z'
        when age < 41 then 'MILLENNIAL'
        when age < 57 then 'GEN_X'
        when age < 76 then 'BOOMER'
        else 'SILENT'
    end                                                          as age_group,

    -- Balance tier.
    case
        when total_balance < 1000   then 'LOW'
        when total_balance < 10000  then 'MODERATE'
        when total_balance < 100000 then 'AFFLUENT'
        else 'HIGH_NET_WORTH'
    end                                                          as balance_tier,

    -- Digital adoption proxy (placeholder; enriched downstream by txn analytics).
    0                                                            as digital_adoption_score,

    -- Numeric features for clustering.
    ln(case when total_balance > 1 then total_balance else 1 end) as log_balance,
    cast(num_active_accounts as decimal(9,4))
        / case when num_accounts > 1 then num_accounts else 1 end as acct_ratio
from cust_360
