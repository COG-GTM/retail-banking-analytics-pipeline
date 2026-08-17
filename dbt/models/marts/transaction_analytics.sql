{{
    config(
        materialized='table',
        tags=['marts', 'data_product']
    )
}}

-- TRANSACTION_ANALYTICS data product.
-- Translated from sas/02_sas_txn_analytics.sas. The PROC SQL aggregation and the
-- DATA-step trend/revenue logic become CTEs; the statistical steps that SAS ran
-- as procedures are expressed in SQL:
--   * PROC RANK groups=100  -> ntile(100) window (0-99 percentile)
--   * PROC MEANS IQR anomaly -> percentile_cont median / IQR + threshold flag

with cust_txn as (

    -- Aggregate account-level summaries to customer level.
    select
        customer_id,
        count(distinct account_id)                            as total_accounts,
        sum(case when days_since_last_txn <= 30 then 1 else 0 end) as active_accounts,
        sum(txn_count_total)                                  as total_transactions,
        sum(amt_total_debit)                                  as total_debit_amt,
        sum(amt_total_credit)                                 as total_credit_amt,
        sum(amt_total_credit) - sum(amt_total_debit)          as net_cash_flow,
        case when sum(txn_count_total) > 0
             then sum(amt_total_debit + amt_total_credit) / sum(txn_count_total)
             else 0 end                                       as avg_transaction_size,
        sum(amt_total_fees)                                   as total_fees,
        max(top_merchant_category)                            as top_spend_category,
        case when sum(txn_count_total) > 0
             then (sum(txn_count_total * (pct_web + pct_mobile) / 100))
                  / sum(txn_count_total) * 100
             else 0 end                                       as digital_txn_pct
    from {{ ref('stg_txn_summary') }}
    group by customer_id

),

cust_txn_trend as (

    -- Spend trend direction + placeholder revenue components.
    select
        cust_txn.*,
        case
            when net_cash_flow > avg_transaction_size * 5  then 'UP'
            when net_cash_flow < -avg_transaction_size * 5 then 'DOWN'
            else 'STABLE'
        end                                  as monthly_spend_trend,
        total_fees                           as fee_income,
        total_debit_amt * 0.02               as interest_income,   -- simplified interest proxy
        total_fees + total_debit_amt * 0.02  as revenue_contribution
    from cust_txn

),

ranked as (

    -- Spend percentile (SAS PROC RANK groups=100 yields 0-99).
    select
        cust_txn_trend.*,
        cast(ntile(100) over (order by total_debit_amt) - 1 as decimal(5,2)) as spend_percentile
    from cust_txn_trend

),

stats as (

    -- Population median and IQR of debit spend for anomaly detection.
    select
        percentile_cont(0.5) within group (order by total_debit_amt) as median_debit,
        percentile_cont(0.75) within group (order by total_debit_amt)
          - percentile_cont(0.25) within group (order by total_debit_amt) as iqr_debit
    from cust_txn_trend

)

select
    r.customer_id,
    -- Reporting period (YYYY-MM) of the run date.
    date_format(current_date, 'yyyy-MM')                     as reporting_period,
    cast(r.total_accounts as smallint)                       as total_accounts,
    cast(r.active_accounts as smallint)                      as active_accounts,
    cast(r.total_transactions as integer)                    as total_transactions,
    cast(r.total_debit_amt as decimal(18,2))                 as total_debit_amt,
    cast(r.total_credit_amt as decimal(18,2))                as total_credit_amt,
    cast(r.net_cash_flow as decimal(18,2))                   as net_cash_flow,
    cast(r.avg_transaction_size as decimal(15,2))            as avg_transaction_size,
    r.monthly_spend_trend,
    r.spend_percentile,
    r.top_spend_category,
    cast(r.digital_txn_pct as decimal(5,2))                  as digital_txn_pct,
    cast(r.fee_income as decimal(15,2))                      as fee_income,
    cast(r.interest_income as decimal(15,2))                 as interest_income,
    cast(r.revenue_contribution as decimal(15,2))            as revenue_contribution,
    -- Flag customers whose spend exceeds median + 3 * IQR.
    case
        when r.total_debit_amt > s.median_debit + (3 * s.iqr_debit) and s.iqr_debit > 0
        then 'Y' else 'N'
    end                                                      as anomaly_flag,
    cast('{{ var("model_version_txn", "TXN_V2.1") }}' as string) as model_version,
    current_date                                             as effective_date,
    current_timestamp()                                      as load_ts
from ranked r
cross join stats s
