-- =============================================================================
-- Staging Table DDL - ETL Intermediate Tables (Spark / Delta Lake)
-- =============================================================================
-- These tables are populated by the Spark staging jobs (spark/staging/*.py,
-- migrated from the former BTEQ scripts) and consumed by the Spark data-product
-- jobs. They live in ${DB_STG} and are (re)written each run by the transforms.
--
-- Migrated from Teradata DDL. Dropped physical-storage directives:
--   * MULTISET / NO FALLBACK / PRIMARY INDEX -> dropped (no Spark equivalent)
--   * COLLECT STATISTICS ...                 -> dropped (Spark uses ANALYZE
--       TABLE ... COMPUTE STATISTICS on demand; not part of the schema DDL)
--   * FORMAT '...' display formats           -> dropped (Teradata-specific)
--   * DEFAULT '...'                          -> dropped (set in transform jobs)
-- Type mapping: INTEGER->INT, TIMESTAMP(6)->TIMESTAMP; DATE/DECIMAL/SMALLINT/
--   BIGINT/CHAR(n)/VARCHAR(n) preserved to stay faithful to column intent.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ${DB_STG};

-- -----------------------------------------------------------------------------
-- STG_CUSTOMER_360
-- Denormalized customer view joining customer, account, and address data.
-- Populated by: spark/staging/01_stg_customer_360.py
-- Consumed by:  spark/data_products/01_customer_segments.py
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_STG}.STG_CUSTOMER_360
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    FIRST_NAME              VARCHAR(60),
    LAST_NAME               VARCHAR(60),
    DATE_OF_BIRTH           DATE,
    AGE                     SMALLINT,
    CUSTOMER_SINCE          DATE,
    TENURE_MONTHS           INT,
    CUSTOMER_STATUS         CHAR(1),
    SEGMENT_CODE            VARCHAR(10),
    BRANCH_ID               INT,
    PRIMARY_ADDRESS         VARCHAR(200),
    CITY                    VARCHAR(60),
    STATE_CODE              CHAR(2),
    ZIP_CODE                VARCHAR(10),
    NUM_ACCOUNTS            SMALLINT,
    NUM_ACTIVE_ACCOUNTS     SMALLINT,
    HAS_CHECKING            CHAR(1),
    HAS_SAVINGS             CHAR(1),
    HAS_CREDIT              CHAR(1),
    HAS_LOAN                CHAR(1),
    TOTAL_BALANCE           DECIMAL(18,2),
    TOTAL_CREDIT_LIMIT      DECIMAL(18,2),
    CREDIT_UTILIZATION_PCT  DECIMAL(5,2),
    LOAD_TS                 TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- STG_TXN_SUMMARY
-- Aggregated transaction metrics per customer over configurable lookback.
-- Populated by: spark/staging/02_stg_txn_summary.py
-- Consumed by:  spark/data_products/02_transaction_analytics.py
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_STG}.STG_TXN_SUMMARY
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    ACCOUNT_ID              BIGINT          NOT NULL,
    ACCOUNT_TYPE            VARCHAR(20),
    SUMMARY_PERIOD_START    DATE,
    SUMMARY_PERIOD_END      DATE,
    TXN_COUNT_TOTAL         INT,
    TXN_COUNT_DEBIT         INT,
    TXN_COUNT_CREDIT        INT,
    TXN_COUNT_FEE           INT,
    AMT_TOTAL_DEBIT         DECIMAL(18,2),
    AMT_TOTAL_CREDIT        DECIMAL(18,2),
    AMT_TOTAL_FEES          DECIMAL(18,2),
    AMT_AVG_DEBIT           DECIMAL(15,2),
    AMT_AVG_CREDIT          DECIMAL(15,2),
    AMT_MAX_SINGLE_DEBIT    DECIMAL(15,2),
    AMT_MAX_SINGLE_CREDIT   DECIMAL(15,2),
    DISTINCT_MERCHANTS      INT,
    TOP_MERCHANT_CATEGORY   VARCHAR(60),
    PCT_ATM                 DECIMAL(5,2),
    PCT_POS                 DECIMAL(5,2),
    PCT_WEB                 DECIMAL(5,2),
    PCT_MOBILE              DECIMAL(5,2),
    DAYS_SINCE_LAST_TXN     INT,
    LOAD_TS                 TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- STG_RISK_FACTORS
-- Pre-computed risk indicator features per customer for scoring models.
-- Populated by: spark/staging/03_stg_risk_factors.py
-- Consumed by:  spark/data_products/03_risk_scoring.py
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_STG}.STG_RISK_FACTORS
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    ACCOUNT_OVERDRAFT_CNT   INT,
    NSF_FEE_TOTAL           DECIMAL(15,2),
    LARGE_WITHDRAWAL_CNT    INT,
    LARGE_WITHDRAWAL_AMT    DECIMAL(18,2),
    AVG_DAILY_BALANCE_30D   DECIMAL(15,2),
    AVG_DAILY_BALANCE_90D   DECIMAL(15,2),
    BALANCE_VOLATILITY      DECIMAL(10,4),
    CREDIT_UTIL_RATIO       DECIMAL(5,4),
    PAYMENT_ONTIME_PCT      DECIMAL(5,2),
    PAYMENT_LATE_CNT        INT,
    MONTHS_SINCE_LAST_LATE  INT,
    EXTERNAL_CREDIT_SCORE   INT,
    DEBIT_VELOCITY_7D       DECIMAL(15,2),
    DEBIT_VELOCITY_30D      DECIMAL(15,2),
    NEW_MERCHANT_CNT_30D    INT,
    INTERNATIONAL_TXN_CNT   INT,
    HIGH_RISK_MERCHANT_CNT  INT,
    LOAD_TS                 TIMESTAMP
)
USING DELTA;
