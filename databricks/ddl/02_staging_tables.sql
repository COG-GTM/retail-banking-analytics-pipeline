-- =============================================================================
-- Ticket 1 - Staging table DDL (Delta)
-- =============================================================================
-- Delta equivalents of ddl/01_staging_tables.sql. These are the outputs of the
-- ported BTEQ jobs (tickets 4-6). Teradata MULTISET / NO FALLBACK / PRIMARY INDEX
-- and the trailing COLLECT STATISTICS statements are dropped; on Delta, layout is
-- managed by OPTIMIZE / ZORDER and statistics are collected automatically.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${catalog}.${staging_schema}.stg_customer_360 (
    customer_id             BIGINT,
    first_name              STRING,
    last_name               STRING,
    date_of_birth           DATE,
    age                     SMALLINT,
    customer_since          DATE,
    tenure_months           INT,
    customer_status         STRING,
    segment_code            STRING,
    branch_id               INT,
    primary_address         STRING,
    city                    STRING,
    state_code              STRING,
    zip_code                STRING,
    num_accounts            SMALLINT,
    num_active_accounts     SMALLINT,
    has_checking            STRING,
    has_savings             STRING,
    has_credit              STRING,
    has_loan                STRING,
    total_balance           DECIMAL(18,2),
    total_credit_limit      DECIMAL(18,2),
    credit_utilization_pct  DECIMAL(5,2),
    load_ts                 TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${staging_schema}.stg_txn_summary (
    customer_id             BIGINT,
    account_id              BIGINT,
    account_type            STRING,
    summary_period_start    DATE,
    summary_period_end      DATE,
    txn_count_total         INT,
    txn_count_debit         INT,
    txn_count_credit        INT,
    txn_count_fee           INT,
    amt_total_debit         DECIMAL(18,2),
    amt_total_credit        DECIMAL(18,2),
    amt_total_fees          DECIMAL(18,2),
    amt_avg_debit           DECIMAL(15,2),
    amt_avg_credit          DECIMAL(15,2),
    amt_max_single_debit    DECIMAL(15,2),
    amt_max_single_credit   DECIMAL(15,2),
    distinct_merchants      INT,
    top_merchant_category   STRING,
    pct_atm                 DECIMAL(5,2),
    pct_pos                 DECIMAL(5,2),
    pct_web                 DECIMAL(5,2),
    pct_mobile              DECIMAL(5,2),
    days_since_last_txn     INT,
    load_ts                 TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${staging_schema}.stg_risk_factors (
    customer_id             BIGINT,
    account_overdraft_cnt   INT,
    nsf_fee_total           DECIMAL(15,2),
    large_withdrawal_cnt    INT,
    large_withdrawal_amt    DECIMAL(18,2),
    avg_daily_balance_30d   DECIMAL(15,2),
    avg_daily_balance_90d   DECIMAL(15,2),
    balance_volatility      DECIMAL(10,4),
    credit_util_ratio       DECIMAL(5,4),
    payment_ontime_pct      DECIMAL(5,2),
    payment_late_cnt        INT,
    months_since_last_late  INT,
    external_credit_score   INT,
    debit_velocity_7d       DECIMAL(15,2),
    debit_velocity_30d      DECIMAL(15,2),
    new_merchant_cnt_30d    INT,
    international_txn_cnt    INT,
    high_risk_merchant_cnt  INT,
    load_ts                 TIMESTAMP
) USING DELTA;
