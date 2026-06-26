-- =============================================================================
-- Staging Table DDL - ETL Intermediate Tables (Databricks / Delta)
-- =============================================================================
-- Migrated from ddl/01_staging_tables.sql (Teradata, ETL_STAGING_DB).
--
-- These tables are populated by the staging notebooks (databricks/staging/*)
-- and consumed by the analytics notebooks (databricks/analytics/*). The
-- staging notebooks use CREATE OR REPLACE TABLE, so these definitions document
-- the contract and pre-create the tables / schema on first deploy.
--
-- Migration notes mirror 00_source_tables.sql:
--   MULTISET/NO FALLBACK -> USING DELTA; PRIMARY INDEX -> CLUSTER BY;
--   VARCHAR/CHAR -> STRING; TIMESTAMP(6) -> TIMESTAMP; FORMAT dropped;
--   COLLECT STATISTICS dropped (optional ANALYZE TABLE ... COMPUTE STATISTICS).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.etl_staging;

-- -----------------------------------------------------------------------------
-- etl_staging.stg_customer_360
-- Denormalized customer view joining customer, account, and address data.
-- Populated by: databricks/staging/01_stg_customer_360.py
-- Consumed by:  databricks/analytics/01_customer_segments.py
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.etl_staging.stg_customer_360
(
    customer_id             BIGINT          NOT NULL,
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
)
USING DELTA
CLUSTER BY (customer_id);

-- -----------------------------------------------------------------------------
-- etl_staging.stg_txn_summary
-- Aggregated transaction metrics per customer/account over a lookback window.
-- Populated by: databricks/staging/02_stg_txn_summary.py
-- Consumed by:  databricks/analytics/02_txn_analytics.py
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.etl_staging.stg_txn_summary
(
    customer_id             BIGINT          NOT NULL,
    account_id              BIGINT          NOT NULL,
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
)
USING DELTA
CLUSTER BY (customer_id, account_id);

-- -----------------------------------------------------------------------------
-- etl_staging.stg_risk_factors
-- Pre-computed risk indicator features per customer for scoring models.
-- Populated by: databricks/staging/03_stg_risk_factors.py
-- Consumed by:  databricks/analytics/03_risk_scoring.py
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.etl_staging.stg_risk_factors
(
    customer_id             BIGINT          NOT NULL,
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
)
USING DELTA
CLUSTER BY (customer_id);

-- -----------------------------------------------------------------------------
-- etl_staging.etl_run_log
-- Audit table replacing the Teradata ETL_RUN_LOG + SAS WORK.PIPELINE_AUDIT.
-- Written by the log_step() helper in databricks/lib/pipeline_utils.py.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.etl_staging.etl_run_log
(
    job_name        STRING,
    step_name       STRING,
    status          STRING,
    message         STRING,
    row_count       BIGINT,
    start_ts        TIMESTAMP,
    end_ts          TIMESTAMP
)
USING DELTA;

-- Optional: refresh statistics for the cost-based optimizer (no-op equivalent
-- of Teradata COLLECT STATISTICS). Safe to omit; Databricks auto-collects.
-- ANALYZE TABLE {{CATALOG}}.etl_staging.stg_customer_360 COMPUTE STATISTICS FOR COLUMNS customer_id;
-- ANALYZE TABLE {{CATALOG}}.etl_staging.stg_txn_summary  COMPUTE STATISTICS FOR COLUMNS customer_id, account_id;
-- ANALYZE TABLE {{CATALOG}}.etl_staging.stg_risk_factors COMPUTE STATISTICS FOR COLUMNS customer_id;
