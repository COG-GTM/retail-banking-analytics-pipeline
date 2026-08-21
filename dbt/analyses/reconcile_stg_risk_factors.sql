/*
    Reconciliation harness for TICKET-05 (MBA-2206).

    Compares the Snowflake model against a landed copy of the Teradata
    ETL_STAGING_DB.STG_RISK_FACTORS output (expected in
    <SF_SCHEMA_STAGING>.STG_RISK_FACTORS_TD_BASELINE) and reports, per customer,
    every feature whose difference exceeds the agreed tolerance:

      * exact match required for counts and the bureau score
      * 0.01 absolute tolerance on currency amounts (NUMBER(_,2))
      * 0.0001 absolute tolerance on ratios and volatility

    Run with:
        dbt compile --select reconcile_stg_risk_factors
    then execute the compiled SQL in Snowflake. Zero rows means the port
    reconciles row for row, including customers with no transactions in any
    lookback window (they appear in both sides with defaulted features).
*/

with sf as (
    select * from {{ ref('stg_risk_factors') }}
),

td as (
    select * from {{ target.database }}.{{ env_var('SF_SCHEMA_STAGING', 'ETL_STAGING') }}.stg_risk_factors_td_baseline
),

joined as (
    select
        coalesce(sf.customer_id, td.customer_id) as customer_id,
        sf.customer_id is null as missing_in_snowflake,
        td.customer_id is null as missing_in_teradata,
        sf.account_overdraft_cnt  - td.account_overdraft_cnt   as d_account_overdraft_cnt,
        sf.nsf_fee_total          - td.nsf_fee_total           as d_nsf_fee_total,
        sf.large_withdrawal_cnt   - td.large_withdrawal_cnt    as d_large_withdrawal_cnt,
        sf.large_withdrawal_amt   - td.large_withdrawal_amt    as d_large_withdrawal_amt,
        sf.avg_daily_balance_30d  - td.avg_daily_balance_30d   as d_avg_daily_balance_30d,
        sf.avg_daily_balance_90d  - td.avg_daily_balance_90d   as d_avg_daily_balance_90d,
        sf.balance_volatility     - td.balance_volatility      as d_balance_volatility,
        sf.credit_util_ratio      - td.credit_util_ratio       as d_credit_util_ratio,
        sf.payment_ontime_pct     - td.payment_ontime_pct      as d_payment_ontime_pct,
        sf.payment_late_cnt       - td.payment_late_cnt        as d_payment_late_cnt,
        sf.months_since_last_late - td.months_since_last_late  as d_months_since_last_late,
        sf.external_credit_score  - td.external_credit_score   as d_external_credit_score,
        sf.debit_velocity_7d      - td.debit_velocity_7d       as d_debit_velocity_7d,
        sf.debit_velocity_30d     - td.debit_velocity_30d      as d_debit_velocity_30d,
        sf.new_merchant_cnt_30d   - td.new_merchant_cnt_30d    as d_new_merchant_cnt_30d,
        sf.international_txn_cnt  - td.international_txn_cnt   as d_international_txn_cnt,
        sf.high_risk_merchant_cnt - td.high_risk_merchant_cnt  as d_high_risk_merchant_cnt
    from sf
    full outer join td
        on sf.customer_id = td.customer_id
)

select *
from joined
where missing_in_snowflake
   or missing_in_teradata
   or d_account_overdraft_cnt <> 0
   or d_large_withdrawal_cnt <> 0
   or d_payment_late_cnt <> 0
   or d_months_since_last_late <> 0
   or d_external_credit_score <> 0
   or d_new_merchant_cnt_30d <> 0
   or d_international_txn_cnt <> 0
   or d_high_risk_merchant_cnt <> 0
   or abs(d_nsf_fee_total) > 0.01
   or abs(d_large_withdrawal_amt) > 0.01
   or abs(d_avg_daily_balance_30d) > 0.01
   or abs(d_avg_daily_balance_90d) > 0.01
   or abs(d_debit_velocity_7d) > 0.01
   or abs(d_debit_velocity_30d) > 0.01
   or abs(d_payment_ontime_pct) > 0.01
   or abs(d_balance_volatility) > 0.0001
   or abs(d_credit_util_ratio) > 0.0001
order by customer_id
