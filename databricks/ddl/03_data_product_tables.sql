-- =============================================================================
-- Ticket 1 - Data product table DDL (Delta)
-- =============================================================================
-- Delta equivalents of ddl/02_data_product_tables.sql (tickets 7-10 outputs).
-- Teradata clauses removed; DECIMAL / TIMESTAMP preserved. The Teradata columnar
-- partition on TRANSACTION_ANALYTICS becomes Delta PARTITIONED BY (reporting_period).
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${catalog}.${products_schema}.customer_segments (
    customer_id             BIGINT,
    segment_name            STRING,
    segment_id              SMALLINT,
    subsegment_id           SMALLINT,
    lifetime_value_score    DECIMAL(10,2),
    engagement_score        DECIMAL(5,2),
    digital_adoption_score  DECIMAL(5,2),
    product_breadth_index   DECIMAL(5,2),
    tenure_group            STRING,
    age_group               STRING,
    balance_tier            STRING,
    channel_preference      STRING,
    cross_sell_flag         STRING,
    upsell_flag             STRING,
    retention_risk_flag     STRING,
    model_version           STRING,
    effective_date          DATE,
    load_ts                 TIMESTAMP
) USING DELTA;

-- Teradata: PARTITION BY COLUMN(REPORTING_PERIOD VARCHAR(7))  ->  Delta partitioning
CREATE TABLE IF NOT EXISTS ${catalog}.${products_schema}.transaction_analytics (
    customer_id             BIGINT,
    total_accounts          SMALLINT,
    active_accounts         SMALLINT,
    total_transactions      INT,
    total_debit_amt         DECIMAL(18,2),
    total_credit_amt        DECIMAL(18,2),
    net_cash_flow           DECIMAL(18,2),
    avg_transaction_size    DECIMAL(15,2),
    monthly_spend_trend     STRING,
    spend_percentile        DECIMAL(5,2),
    top_spend_category      STRING,
    digital_txn_pct         DECIMAL(5,2),
    fee_income              DECIMAL(15,2),
    interest_income         DECIMAL(15,2),
    revenue_contribution    DECIMAL(15,2),
    anomaly_flag            STRING,
    model_version           STRING,
    effective_date          DATE,
    load_ts                 TIMESTAMP,
    reporting_period        STRING
) USING DELTA
PARTITIONED BY (reporting_period);

CREATE TABLE IF NOT EXISTS ${catalog}.${products_schema}.customer_risk_scores (
    customer_id                 BIGINT,
    composite_risk_score        DECIMAL(6,2),
    risk_tier                   STRING,
    probability_of_default      DECIMAL(7,6),
    credit_risk_component       DECIMAL(5,2),
    behaviour_risk_component    DECIMAL(5,2),
    velocity_risk_component     DECIMAL(5,2),
    bureau_score_component      DECIMAL(5,2),
    payment_history_component   DECIMAL(5,2),
    primary_risk_driver         STRING,
    secondary_risk_driver       STRING,
    score_delta_30d             DECIMAL(6,2),
    watch_list_flag             STRING,
    review_required_flag        STRING,
    model_version               STRING,
    effective_date              DATE,
    load_ts                     TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${products_schema}.customer_master_profile (
    customer_id             BIGINT,
    full_name               STRING,
    age                     SMALLINT,
    state_code              STRING,
    customer_since          DATE,
    tenure_months           INT,
    customer_status         STRING,
    segment_name            STRING,
    lifetime_value_score    DECIMAL(10,2),
    engagement_score        DECIMAL(5,2),
    total_accounts          SMALLINT,
    active_accounts         SMALLINT,
    total_balance           DECIMAL(18,2),
    total_credit_limit      DECIMAL(18,2),
    credit_utilization_pct  DECIMAL(5,2),
    monthly_transactions    INT,
    monthly_spend           DECIMAL(18,2),
    net_cash_flow           DECIMAL(18,2),
    top_spend_category      STRING,
    digital_txn_pct         DECIMAL(5,2),
    composite_risk_score    DECIMAL(6,2),
    risk_tier               STRING,
    probability_of_default  DECIMAL(7,6),
    watch_list_flag         STRING,
    cross_sell_flag         STRING,
    upsell_flag             STRING,
    retention_risk_flag     STRING,
    model_version           STRING,
    effective_date          DATE,
    load_ts                 TIMESTAMP
) USING DELTA;
