-- Delta DDL for the etl_staging.stg_risk_factors staging table.
-- Ported from ddl/01_staging_tables.sql (Teradata STG_RISK_FACTORS).
CREATE TABLE IF NOT EXISTS {table} (
    customer_id            BIGINT  NOT NULL,
    account_overdraft_cnt  INT,
    nsf_fee_total          DECIMAL(15,2),
    large_withdrawal_cnt   INT,
    large_withdrawal_amt   DECIMAL(18,2),
    avg_daily_balance_30d  DECIMAL(15,2),
    avg_daily_balance_90d  DECIMAL(15,2),
    balance_volatility     DECIMAL(10,4),
    credit_util_ratio      DECIMAL(5,4),
    payment_ontime_pct     DECIMAL(5,2),
    payment_late_cnt       INT,
    months_since_last_late INT,
    external_credit_score  INT,
    debit_velocity_7d      DECIMAL(15,2),
    debit_velocity_30d     DECIMAL(15,2),
    new_merchant_cnt_30d   INT,
    international_txn_cnt   INT,
    high_risk_merchant_cnt INT,
    load_ts                TIMESTAMP
) USING DELTA
