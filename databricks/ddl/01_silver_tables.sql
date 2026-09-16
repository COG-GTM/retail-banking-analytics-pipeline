-- =============================================================================
-- Silver (staging) tables - Delta Lake equivalents of ddl/01_staging_tables.sql
-- MULTISET / NO FALLBACK / PRIMARY INDEX / FORMAT / CHARACTER SET dropped.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${catalog}.etl_staging.stg_customer_360 (
    customer_id             BIGINT       NOT NULL COMMENT 'Customer key',
    first_name              STRING       COMMENT 'Given name',
    last_name               STRING       COMMENT 'Family name',
    date_of_birth           DATE         COMMENT 'Date of birth',
    age                     SMALLINT     COMMENT 'Age in years (boundary diff)',
    customer_since          DATE         COMMENT 'Relationship start date',
    tenure_months           INT          COMMENT 'Months since customer_since',
    customer_status         STRING       COMMENT 'A=Active, I=Inactive, C=Closed',
    segment_code            STRING       COMMENT 'Source segment code',
    branch_id               INT          COMMENT 'Home branch',
    primary_address         STRING       COMMENT 'HOME address line(s)',
    city                    STRING       COMMENT 'City',
    state_code              STRING       COMMENT 'State',
    zip_code                STRING       COMMENT 'ZIP',
    num_accounts            SMALLINT     COMMENT 'Total accounts',
    num_active_accounts     SMALLINT     COMMENT 'Open accounts',
    has_checking            STRING       COMMENT 'Y/N',
    has_savings             STRING       COMMENT 'Y/N',
    has_credit              STRING       COMMENT 'Y/N',
    has_loan                STRING       COMMENT 'Y/N',
    total_balance           DECIMAL(18,2) COMMENT 'Sum of current balances',
    total_credit_limit      DECIMAL(18,2) COMMENT 'Sum of credit limits',
    credit_utilization_pct  DECIMAL(5,2) COMMENT 'Credit balance / limit * 100',
    load_ts                 TIMESTAMP    COMMENT 'Load timestamp'
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS ${catalog}.etl_staging.stg_txn_summary (
    customer_id             BIGINT       NOT NULL COMMENT 'Customer key',
    account_id              BIGINT       NOT NULL COMMENT 'Account key',
    account_type            STRING       COMMENT 'CHECKING/SAVINGS/CREDIT/LOAN',
    summary_period_start    DATE         COMMENT 'Lookback window start',
    summary_period_end      DATE         COMMENT 'Run date',
    txn_count_total         INT          COMMENT 'Posted txn count',
    txn_count_debit         INT          COMMENT 'Debit count',
    txn_count_credit        INT          COMMENT 'Credit count',
    txn_count_fee           INT          COMMENT 'Fee count',
    amt_total_debit         DECIMAL(18,2) COMMENT 'Total debit amount',
    amt_total_credit        DECIMAL(18,2) COMMENT 'Total credit amount',
    amt_total_fees          DECIMAL(18,2) COMMENT 'Total fee amount',
    amt_avg_debit           DECIMAL(15,2) COMMENT 'Avg debit amount',
    amt_avg_credit          DECIMAL(15,2) COMMENT 'Avg credit amount',
    amt_max_single_debit    DECIMAL(15,2) COMMENT 'Max single debit',
    amt_max_single_credit   DECIMAL(15,2) COMMENT 'Max single credit',
    distinct_merchants      INT          COMMENT 'Distinct merchant names',
    top_merchant_category   STRING       COMMENT 'Top merchant category by spend',
    pct_atm                 DECIMAL(5,2) COMMENT '% of txns via ATM',
    pct_pos                 DECIMAL(5,2) COMMENT '% of txns via POS',
    pct_web                 DECIMAL(5,2) COMMENT '% of txns via WEB',
    pct_mobile              DECIMAL(5,2) COMMENT '% of txns via MOB',
    days_since_last_txn     INT          COMMENT 'Days since most recent txn',
    load_ts                 TIMESTAMP    COMMENT 'Load timestamp'
) USING DELTA
CLUSTER BY (customer_id, account_id)
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS ${catalog}.etl_staging.stg_risk_factors (
    customer_id             BIGINT       NOT NULL COMMENT 'Customer key',
    account_overdraft_cnt   INT          COMMENT 'Overdraft occurrences (12m)',
    nsf_fee_total           DECIMAL(15,2) COMMENT 'NSF fees total (12m)',
    large_withdrawal_cnt    INT          COMMENT 'Debits >= 5000 count (12m)',
    large_withdrawal_amt    DECIMAL(18,2) COMMENT 'Debits >= 5000 amount (12m)',
    avg_daily_balance_30d   DECIMAL(15,2) COMMENT 'Avg EOD balance 30d',
    avg_daily_balance_90d   DECIMAL(15,2) COMMENT 'Avg EOD balance 90d',
    balance_volatility      DECIMAL(10,4) COMMENT 'Pop stddev of EOD balance (3m)',
    credit_util_ratio       DECIMAL(5,4) COMMENT 'Credit balance / limit',
    payment_ontime_pct      DECIMAL(5,2) COMMENT 'On-time payments % (24m)',
    payment_late_cnt        INT          COMMENT 'Late payment count (24m)',
    months_since_last_late  INT          COMMENT 'Months since last late payment',
    external_credit_score   INT          COMMENT 'Latest bureau score',
    debit_velocity_7d       DECIMAL(15,2) COMMENT 'Debit outflow 7d',
    debit_velocity_30d      DECIMAL(15,2) COMMENT 'Debit outflow 30d',
    new_merchant_cnt_30d    INT          COMMENT 'Distinct merchants 30d',
    international_txn_cnt   INT          COMMENT 'INTL channel txns (6m)',
    high_risk_merchant_cnt  INT          COMMENT 'High-risk category txns (6m)',
    load_ts                 TIMESTAMP    COMMENT 'Load timestamp'
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
