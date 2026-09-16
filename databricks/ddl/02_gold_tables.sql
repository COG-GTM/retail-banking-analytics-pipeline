-- =============================================================================
-- Gold (data product) tables - Delta Lake equivalents of
-- ddl/02_data_product_tables.sql. PRIMARY INDEX -> informational
-- NOT ENFORCED primary keys; PARTITION BY COLUMN -> PARTITIONED BY.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${catalog}.data_products.customer_segments (
    customer_id             BIGINT       NOT NULL COMMENT 'Customer key',
    segment_name            STRING       COMMENT 'Cluster label',
    segment_id              SMALLINT     COMMENT 'Cluster id',
    subsegment_id           SMALLINT     COMMENT 'Reserved',
    lifetime_value_score    DECIMAL(10,2) COMMENT 'LTV score',
    engagement_score        DECIMAL(5,2) COMMENT 'Active acct ratio * 100',
    digital_adoption_score  DECIMAL(5,2) COMMENT 'Reserved (0.0)',
    product_breadth_index   DECIMAL(5,2) COMMENT 'Product breadth * 100',
    tenure_group            STRING       COMMENT 'Tenure band',
    age_group               STRING       COMMENT 'Age band',
    balance_tier            STRING       COMMENT 'Balance band',
    channel_preference      STRING       COMMENT 'Reserved',
    cross_sell_flag         STRING       COMMENT 'Y/N',
    upsell_flag             STRING       COMMENT 'Y/N',
    retention_risk_flag     STRING       COMMENT 'Y/N',
    model_version           STRING       COMMENT 'SEG_V3.2',
    effective_date          DATE         COMMENT 'Run date',
    load_ts                 TIMESTAMP    COMMENT 'Load timestamp'
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

ALTER TABLE ${catalog}.data_products.customer_segments
    ADD CONSTRAINT pk_customer_segments PRIMARY KEY (customer_id) NOT ENFORCED;

CREATE TABLE IF NOT EXISTS ${catalog}.data_products.transaction_analytics (
    customer_id             BIGINT       NOT NULL COMMENT 'Customer key',
    reporting_period        STRING       COMMENT 'YYYY-MM',
    total_accounts          SMALLINT     COMMENT 'Accounts with activity',
    active_accounts         SMALLINT     COMMENT 'Accounts active <=30d',
    total_transactions      INT          COMMENT 'Total txns',
    total_debit_amt         DECIMAL(18,2) COMMENT 'Total debits',
    total_credit_amt        DECIMAL(18,2) COMMENT 'Total credits',
    net_cash_flow           DECIMAL(18,2) COMMENT 'Credit - debit',
    avg_transaction_size    DECIMAL(15,2) COMMENT 'Mean txn size',
    monthly_spend_trend     STRING       COMMENT 'UP/DOWN/STABLE',
    spend_percentile        DECIMAL(5,2) COMMENT 'Percent rank of spend',
    top_spend_category      STRING       COMMENT 'Top merchant category',
    digital_txn_pct         DECIMAL(5,2) COMMENT '% digital channel txns',
    fee_income              DECIMAL(15,2) COMMENT 'Fee income',
    interest_income         DECIMAL(15,2) COMMENT 'Interest income',
    revenue_contribution    DECIMAL(15,2) COMMENT 'Fee + interest income',
    anomaly_flag            STRING       COMMENT 'IQR outlier flag Y/N',
    model_version           STRING       COMMENT 'TXN_V2.1',
    effective_date          DATE         COMMENT 'Run date',
    load_ts                 TIMESTAMP    COMMENT 'Load timestamp'
) USING DELTA
PARTITIONED BY (reporting_period)
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

ALTER TABLE ${catalog}.data_products.transaction_analytics
    ADD CONSTRAINT pk_transaction_analytics
    PRIMARY KEY (customer_id, reporting_period) NOT ENFORCED;

CREATE TABLE IF NOT EXISTS ${catalog}.data_products.customer_risk_scores (
    customer_id                 BIGINT   NOT NULL COMMENT 'Customer key',
    composite_risk_score        DECIMAL(6,2) COMMENT 'Weighted composite 0-100',
    risk_tier                   STRING   COMMENT 'LOW/MODERATE/ELEVATED/HIGH/CRITICAL',
    probability_of_default      DECIMAL(7,6) COMMENT 'LR probability',
    credit_risk_component       DECIMAL(5,2) COMMENT 'Component score',
    behaviour_risk_component    DECIMAL(5,2) COMMENT 'Component score',
    velocity_risk_component     DECIMAL(5,2) COMMENT 'Component score',
    bureau_score_component      DECIMAL(5,2) COMMENT 'Component score',
    payment_history_component   DECIMAL(5,2) COMMENT 'Component score',
    primary_risk_driver         STRING   COMMENT 'Top driver',
    secondary_risk_driver       STRING   COMMENT '2nd driver',
    score_delta_30d             DECIMAL(6,2) COMMENT 'Reserved (0.0)',
    watch_list_flag             STRING   COMMENT 'Y/N',
    review_required_flag        STRING   COMMENT 'Y/N',
    model_version               STRING   COMMENT 'RISK_V4.0',
    effective_date              DATE     COMMENT 'Run date',
    load_ts                     TIMESTAMP COMMENT 'Load timestamp'
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

ALTER TABLE ${catalog}.data_products.customer_risk_scores
    ADD CONSTRAINT pk_customer_risk_scores PRIMARY KEY (customer_id) NOT ENFORCED;

CREATE TABLE IF NOT EXISTS ${catalog}.data_products.customer_master_profile (
    customer_id             BIGINT       NOT NULL COMMENT 'Customer key',
    full_name               STRING       COMMENT 'first + last',
    age                     SMALLINT     COMMENT 'Age in years',
    state_code              STRING       COMMENT 'State',
    customer_since          DATE         COMMENT 'Relationship start',
    tenure_months           INT          COMMENT 'Tenure in months',
    customer_status         STRING       COMMENT 'A/I/C',
    segment_name            STRING       COMMENT 'Default UNCLASSIFIED',
    lifetime_value_score    DECIMAL(10,2) COMMENT 'LTV score',
    engagement_score        DECIMAL(5,2) COMMENT 'Engagement',
    total_accounts          SMALLINT     COMMENT 'Account count',
    active_accounts         SMALLINT     COMMENT 'Active accounts',
    total_balance           DECIMAL(18,2) COMMENT 'Total balance',
    total_credit_limit      DECIMAL(18,2) COMMENT 'Total credit limit',
    credit_utilization_pct  DECIMAL(5,2) COMMENT 'Utilization %',
    monthly_transactions    INT          COMMENT 'Txn count',
    monthly_spend           DECIMAL(18,2) COMMENT 'Debit amount',
    net_cash_flow           DECIMAL(18,2) COMMENT 'Net flow',
    top_spend_category      STRING       COMMENT 'Top merchant category',
    digital_txn_pct         DECIMAL(5,2) COMMENT 'Digital %',
    composite_risk_score    DECIMAL(6,2) COMMENT 'Risk score',
    risk_tier               STRING       COMMENT 'Default UNKNOWN',
    probability_of_default  DECIMAL(7,6) COMMENT 'PD',
    watch_list_flag         STRING       COMMENT 'Y/N',
    cross_sell_flag         STRING       COMMENT 'Y/N',
    upsell_flag             STRING       COMMENT 'Y/N',
    retention_risk_flag     STRING       COMMENT 'Y/N',
    model_version           STRING       COMMENT 'MASTER_V1.5',
    effective_date          DATE         COMMENT 'Run date',
    load_ts                 TIMESTAMP    COMMENT 'Load timestamp'
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

ALTER TABLE ${catalog}.data_products.customer_master_profile
    ADD CONSTRAINT pk_customer_master_profile
    PRIMARY KEY (customer_id) NOT ENFORCED;
