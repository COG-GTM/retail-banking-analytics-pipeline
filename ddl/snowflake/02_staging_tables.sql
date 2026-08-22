-- =============================================================================
-- Snowflake Staging Table DDL - ETL Intermediate Tables
-- =============================================================================
-- Migrated from ddl/01_staging_tables.sql (Teradata).
-- These tables are populated by the migrated ETL layer and consumed by the
-- migrated analytics layer. They live in the ETL_STAGING schema and are
-- refreshed each run.
--
-- Usage: snowsql -f ddl/snowflake/02_staging_tables.sql -D env=DEV
--
-- Teradata COLLECT STATISTICS statements are dropped: Snowflake maintains
-- micro-partition metadata automatically, so there is no manual equivalent.
-- =============================================================================

!set variable_substitution=true

USE DATABASE RETAIL_BANKING_&{env};

-- -----------------------------------------------------------------------------
-- ETL_STAGING.STG_CUSTOMER_360
-- Denormalized customer view joining customer, account, and address data.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ETL_STAGING.STG_CUSTOMER_360
(
    CUSTOMER_ID             NUMBER(19,0)    NOT NULL,
    FIRST_NAME              VARCHAR(60),
    LAST_NAME               VARCHAR(60),
    DATE_OF_BIRTH           DATE,
    AGE                     NUMBER(5,0),
    CUSTOMER_SINCE          DATE,
    TENURE_MONTHS           NUMBER(10,0),
    CUSTOMER_STATUS         VARCHAR(1),
    SEGMENT_CODE            VARCHAR(10),
    BRANCH_ID               NUMBER(10,0),
    PRIMARY_ADDRESS         VARCHAR(200),
    CITY                    VARCHAR(60),
    STATE_CODE              VARCHAR(2),
    ZIP_CODE                VARCHAR(10),
    NUM_ACCOUNTS            NUMBER(5,0),
    NUM_ACTIVE_ACCOUNTS     NUMBER(5,0),
    HAS_CHECKING            VARCHAR(1)      DEFAULT 'N',
    HAS_SAVINGS             VARCHAR(1)      DEFAULT 'N',
    HAS_CREDIT              VARCHAR(1)      DEFAULT 'N',
    HAS_LOAN                VARCHAR(1)      DEFAULT 'N',
    TOTAL_BALANCE           NUMBER(18,2),
    TOTAL_CREDIT_LIMIT      NUMBER(18,2),
    CREDIT_UTILIZATION_PCT  NUMBER(5,2),
    LOAD_TS                 TIMESTAMP_NTZ(6)
)
COMMENT = 'Denormalized customer 360 staging table (migrated from ETL_STAGING_DB.STG_CUSTOMER_360)';

-- -----------------------------------------------------------------------------
-- ETL_STAGING.STG_TXN_SUMMARY
-- Aggregated transaction metrics per customer over configurable lookback.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ETL_STAGING.STG_TXN_SUMMARY
(
    CUSTOMER_ID             NUMBER(19,0)    NOT NULL,
    ACCOUNT_ID              NUMBER(19,0)    NOT NULL,
    ACCOUNT_TYPE            VARCHAR(20),
    SUMMARY_PERIOD_START    DATE,
    SUMMARY_PERIOD_END      DATE,
    TXN_COUNT_TOTAL         NUMBER(10,0),
    TXN_COUNT_DEBIT         NUMBER(10,0),
    TXN_COUNT_CREDIT        NUMBER(10,0),
    TXN_COUNT_FEE           NUMBER(10,0),
    AMT_TOTAL_DEBIT         NUMBER(18,2),
    AMT_TOTAL_CREDIT        NUMBER(18,2),
    AMT_TOTAL_FEES          NUMBER(18,2),
    AMT_AVG_DEBIT           NUMBER(15,2),
    AMT_AVG_CREDIT          NUMBER(15,2),
    AMT_MAX_SINGLE_DEBIT    NUMBER(15,2),
    AMT_MAX_SINGLE_CREDIT   NUMBER(15,2),
    DISTINCT_MERCHANTS      NUMBER(10,0),
    TOP_MERCHANT_CATEGORY   VARCHAR(60),
    PCT_ATM                 NUMBER(5,2),
    PCT_POS                 NUMBER(5,2),
    PCT_WEB                 NUMBER(5,2),
    PCT_MOBILE              NUMBER(5,2),
    DAYS_SINCE_LAST_TXN     NUMBER(10,0),
    LOAD_TS                 TIMESTAMP_NTZ(6)
)
COMMENT = 'Per-account transaction summary staging table (migrated from ETL_STAGING_DB.STG_TXN_SUMMARY)';

-- -----------------------------------------------------------------------------
-- ETL_STAGING.STG_RISK_FACTORS
-- Pre-computed risk indicator features per customer for scoring models.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ETL_STAGING.STG_RISK_FACTORS
(
    CUSTOMER_ID             NUMBER(19,0)    NOT NULL,
    ACCOUNT_OVERDRAFT_CNT   NUMBER(10,0),
    NSF_FEE_TOTAL           NUMBER(15,2),
    LARGE_WITHDRAWAL_CNT    NUMBER(10,0),
    LARGE_WITHDRAWAL_AMT    NUMBER(18,2),
    AVG_DAILY_BALANCE_30D   NUMBER(15,2),
    AVG_DAILY_BALANCE_90D   NUMBER(15,2),
    BALANCE_VOLATILITY      NUMBER(10,4),
    CREDIT_UTIL_RATIO       NUMBER(5,4),
    PAYMENT_ONTIME_PCT      NUMBER(5,2),
    PAYMENT_LATE_CNT        NUMBER(10,0),
    MONTHS_SINCE_LAST_LATE  NUMBER(10,0),
    EXTERNAL_CREDIT_SCORE   NUMBER(10,0),
    DEBIT_VELOCITY_7D       NUMBER(15,2),
    DEBIT_VELOCITY_30D      NUMBER(15,2),
    NEW_MERCHANT_CNT_30D    NUMBER(10,0),
    INTERNATIONAL_TXN_CNT   NUMBER(10,0),
    HIGH_RISK_MERCHANT_CNT  NUMBER(10,0),
    LOAD_TS                 TIMESTAMP_NTZ(6)
)
COMMENT = 'Risk feature staging table (migrated from ETL_STAGING_DB.STG_RISK_FACTORS)';
