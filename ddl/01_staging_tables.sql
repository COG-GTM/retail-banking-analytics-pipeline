-- =============================================================================
-- Staging Table DDL - ETL Intermediate Tables
-- =============================================================================
-- These tables are populated by the BTEQ scripts and consumed by SAS.
-- They live in ETL_STAGING_DB and are DROP/CREATE'd each run.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- STG_CUSTOMER_360
-- Denormalized customer view joining customer, account, and address data.
-- Populated by: bteq/01_stg_customer_360.bteq
-- Consumed by:  sas/01_sas_customer_segments.sas
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE ETL_STAGING_DB.STG_CUSTOMER_360, NO FALLBACK
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    FIRST_NAME              VARCHAR(60),
    LAST_NAME               VARCHAR(60),
    DATE_OF_BIRTH           DATE            FORMAT 'YYYY-MM-DD',
    AGE                     SMALLINT,
    CUSTOMER_SINCE          DATE            FORMAT 'YYYY-MM-DD',
    TENURE_MONTHS           INTEGER,
    CUSTOMER_STATUS         CHAR(1),
    SEGMENT_CODE            VARCHAR(10),
    BRANCH_ID               INTEGER,
    PRIMARY_ADDRESS         VARCHAR(200),
    CITY                    VARCHAR(60),
    STATE_CODE              CHAR(2),
    ZIP_CODE                VARCHAR(10),
    NUM_ACCOUNTS            SMALLINT,
    NUM_ACTIVE_ACCOUNTS     SMALLINT,
    HAS_CHECKING            CHAR(1)         DEFAULT 'N',
    HAS_SAVINGS             CHAR(1)         DEFAULT 'N',
    HAS_CREDIT              CHAR(1)         DEFAULT 'N',
    HAS_LOAN                CHAR(1)         DEFAULT 'N',
    TOTAL_BALANCE           DECIMAL(18,2),
    TOTAL_CREDIT_LIMIT      DECIMAL(18,2),
    CREDIT_UTILIZATION_PCT  DECIMAL(5,2),
    LOAD_TS                 TIMESTAMP(6)
)
PRIMARY INDEX (CUSTOMER_ID);

-- -----------------------------------------------------------------------------
-- STG_TXN_SUMMARY
-- Aggregated transaction metrics per customer over configurable lookback.
-- Populated by: bteq/02_stg_txn_summary.bteq
-- Consumed by:  sas/02_sas_txn_analytics.sas
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE ETL_STAGING_DB.STG_TXN_SUMMARY, NO FALLBACK
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    ACCOUNT_ID              BIGINT          NOT NULL,
    ACCOUNT_TYPE            VARCHAR(20),
    SUMMARY_PERIOD_START    DATE            FORMAT 'YYYY-MM-DD',
    SUMMARY_PERIOD_END      DATE            FORMAT 'YYYY-MM-DD',
    TXN_COUNT_TOTAL         INTEGER,
    TXN_COUNT_DEBIT         INTEGER,
    TXN_COUNT_CREDIT        INTEGER,
    TXN_COUNT_FEE           INTEGER,
    AMT_TOTAL_DEBIT         DECIMAL(18,2),
    AMT_TOTAL_CREDIT        DECIMAL(18,2),
    AMT_TOTAL_FEES          DECIMAL(18,2),
    AMT_AVG_DEBIT           DECIMAL(15,2),
    AMT_AVG_CREDIT          DECIMAL(15,2),
    AMT_MAX_SINGLE_DEBIT    DECIMAL(15,2),
    AMT_MAX_SINGLE_CREDIT   DECIMAL(15,2),
    DISTINCT_MERCHANTS      INTEGER,
    TOP_MERCHANT_CATEGORY   VARCHAR(60),
    PCT_ATM                 DECIMAL(5,2),
    PCT_POS                 DECIMAL(5,2),
    PCT_WEB                 DECIMAL(5,2),
    PCT_MOBILE              DECIMAL(5,2),
    DAYS_SINCE_LAST_TXN     INTEGER,
    LOAD_TS                 TIMESTAMP(6)
)
PRIMARY INDEX (CUSTOMER_ID, ACCOUNT_ID);

-- -----------------------------------------------------------------------------
-- STG_RISK_FACTORS
-- Pre-computed risk indicator features per customer for scoring models.
-- Populated by: bteq/03_stg_risk_factors.bteq
-- Consumed by:  sas/03_sas_risk_scoring.sas
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE ETL_STAGING_DB.STG_RISK_FACTORS, NO FALLBACK
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    ACCOUNT_OVERDRAFT_CNT   INTEGER,
    NSF_FEE_TOTAL           DECIMAL(15,2),
    LARGE_WITHDRAWAL_CNT    INTEGER,
    LARGE_WITHDRAWAL_AMT    DECIMAL(18,2),
    AVG_DAILY_BALANCE_30D   DECIMAL(15,2),
    AVG_DAILY_BALANCE_90D   DECIMAL(15,2),
    BALANCE_VOLATILITY      DECIMAL(10,4),
    CREDIT_UTIL_RATIO       DECIMAL(5,4),
    PAYMENT_ONTIME_PCT      DECIMAL(5,2),
    PAYMENT_LATE_CNT        INTEGER,
    MONTHS_SINCE_LAST_LATE  INTEGER,
    EXTERNAL_CREDIT_SCORE   INTEGER,
    DEBIT_VELOCITY_7D       DECIMAL(15,2),
    DEBIT_VELOCITY_30D      DECIMAL(15,2),
    NEW_MERCHANT_CNT_30D    INTEGER,
    INTERNATIONAL_TXN_CNT   INTEGER,
    HIGH_RISK_MERCHANT_CNT  INTEGER,
    LOAD_TS                 TIMESTAMP(6)
)
PRIMARY INDEX (CUSTOMER_ID);

COLLECT STATISTICS COLUMN (CUSTOMER_ID) ON ETL_STAGING_DB.STG_CUSTOMER_360;
COLLECT STATISTICS COLUMN (CUSTOMER_ID) ON ETL_STAGING_DB.STG_TXN_SUMMARY;
COLLECT STATISTICS COLUMN (CUSTOMER_ID, ACCOUNT_ID) ON ETL_STAGING_DB.STG_TXN_SUMMARY;
COLLECT STATISTICS COLUMN (CUSTOMER_ID) ON ETL_STAGING_DB.STG_RISK_FACTORS;
