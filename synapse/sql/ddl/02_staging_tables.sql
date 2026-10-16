-- =============================================================================
-- AZSYN-01: Staging tables (stg.*) - Synapse dedicated SQL pool
-- =============================================================================
-- Translated from ddl/01_staging_tables.sql (Teradata) per AZSYN contract v1.
-- stg.ETL_RUN_LOG has no legacy DDL; its definition is frozen by the contract.
--
-- The STG_* tables are rebuilt by CTAS in synapse/sql/procs/stg.usp_load_*.sql
-- (IF OBJECT_ID drop -> CTAS -> CREATE STATISTICS). This file is the reference
-- definition (names, order, types the CTAS must produce) and can pre-provision
-- an empty environment. Note: CTAS does not carry column DEFAULTs, so the
-- DEFAULT 'N' flags below only apply to pre-provisioned tables.
--
--   PRIMARY INDEX (CUSTOMER_ID[, ACCOUNT_ID]) -> DISTRIBUTION = HASH(CUSTOMER_ID), CCI
--   COLLECT STATISTICS COLUMN (...)          -> CREATE STATISTICS st_<TABLE>_<COLS>
--   TIMESTAMP(6) -> DATETIME2(6); FORMAT clauses dropped.
-- Idempotent and non-destructive: objects are created only if missing.
-- =============================================================================

-- stg.STG_CUSTOMER_360  (populated by bteq/01_stg_customer_360.bteq)
IF OBJECT_ID(N'stg.STG_CUSTOMER_360', N'U') IS NULL
CREATE TABLE stg.STG_CUSTOMER_360
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    FIRST_NAME              VARCHAR(60)     NULL,
    LAST_NAME               VARCHAR(60)     NULL,
    DATE_OF_BIRTH           DATE            NULL,
    AGE                     SMALLINT        NULL,
    CUSTOMER_SINCE          DATE            NULL,
    TENURE_MONTHS           INT             NULL,
    CUSTOMER_STATUS         CHAR(1)         NULL,
    SEGMENT_CODE            VARCHAR(10)     NULL,
    BRANCH_ID               INT             NULL,
    PRIMARY_ADDRESS         VARCHAR(200)    NULL,
    CITY                    VARCHAR(60)     NULL,
    STATE_CODE              CHAR(2)         NULL,
    ZIP_CODE                VARCHAR(10)     NULL,
    NUM_ACCOUNTS            SMALLINT        NULL,
    NUM_ACTIVE_ACCOUNTS     SMALLINT        NULL,
    HAS_CHECKING            CHAR(1)         NULL DEFAULT 'N',
    HAS_SAVINGS             CHAR(1)         NULL DEFAULT 'N',
    HAS_CREDIT              CHAR(1)         NULL DEFAULT 'N',
    HAS_LOAN                CHAR(1)         NULL DEFAULT 'N',
    TOTAL_BALANCE           DECIMAL(18,2)   NULL,
    TOTAL_CREDIT_LIMIT      DECIMAL(18,2)   NULL,
    CREDIT_UTILIZATION_PCT  DECIMAL(5,2)    NULL,
    LOAD_TS                 DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- stg.STG_TXN_SUMMARY  (populated by bteq/02_stg_txn_summary.bteq)
-- Teradata PI was (CUSTOMER_ID, ACCOUNT_ID); hashed on CUSTOMER_ID only so it
-- co-locates with the other customer-keyed tables (contract rule).
IF OBJECT_ID(N'stg.STG_TXN_SUMMARY', N'U') IS NULL
CREATE TABLE stg.STG_TXN_SUMMARY
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    ACCOUNT_ID              BIGINT          NOT NULL,
    ACCOUNT_TYPE            VARCHAR(20)     NULL,
    SUMMARY_PERIOD_START    DATE            NULL,
    SUMMARY_PERIOD_END      DATE            NULL,
    TXN_COUNT_TOTAL         INT             NULL,
    TXN_COUNT_DEBIT         INT             NULL,
    TXN_COUNT_CREDIT        INT             NULL,
    TXN_COUNT_FEE           INT             NULL,
    AMT_TOTAL_DEBIT         DECIMAL(18,2)   NULL,
    AMT_TOTAL_CREDIT        DECIMAL(18,2)   NULL,
    AMT_TOTAL_FEES          DECIMAL(18,2)   NULL,
    AMT_AVG_DEBIT           DECIMAL(15,2)   NULL,
    AMT_AVG_CREDIT          DECIMAL(15,2)   NULL,
    AMT_MAX_SINGLE_DEBIT    DECIMAL(15,2)   NULL,
    AMT_MAX_SINGLE_CREDIT   DECIMAL(15,2)   NULL,
    DISTINCT_MERCHANTS      INT             NULL,
    TOP_MERCHANT_CATEGORY   VARCHAR(60)     NULL,
    PCT_ATM                 DECIMAL(5,2)    NULL,
    PCT_POS                 DECIMAL(5,2)    NULL,
    PCT_WEB                 DECIMAL(5,2)    NULL,
    PCT_MOBILE              DECIMAL(5,2)    NULL,
    DAYS_SINCE_LAST_TXN     INT             NULL,
    LOAD_TS                 DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- stg.STG_RISK_FACTORS  (populated by bteq/03_stg_risk_factors.bteq)
IF OBJECT_ID(N'stg.STG_RISK_FACTORS', N'U') IS NULL
CREATE TABLE stg.STG_RISK_FACTORS
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    ACCOUNT_OVERDRAFT_CNT   INT             NULL,
    NSF_FEE_TOTAL           DECIMAL(15,2)   NULL,
    LARGE_WITHDRAWAL_CNT    INT             NULL,
    LARGE_WITHDRAWAL_AMT    DECIMAL(18,2)   NULL,
    AVG_DAILY_BALANCE_30D   DECIMAL(15,2)   NULL,
    AVG_DAILY_BALANCE_90D   DECIMAL(15,2)   NULL,
    BALANCE_VOLATILITY      DECIMAL(10,4)   NULL,
    CREDIT_UTIL_RATIO       DECIMAL(5,4)    NULL,
    PAYMENT_ONTIME_PCT      DECIMAL(5,2)    NULL,
    PAYMENT_LATE_CNT        INT             NULL,
    MONTHS_SINCE_LAST_LATE  INT             NULL,
    EXTERNAL_CREDIT_SCORE   INT             NULL,
    DEBIT_VELOCITY_7D       DECIMAL(15,2)   NULL,
    DEBIT_VELOCITY_30D      DECIMAL(15,2)   NULL,
    NEW_MERCHANT_CNT_30D    INT             NULL,
    INTERNATIONAL_TXN_CNT   INT             NULL,
    HIGH_RISK_MERCHANT_CNT  INT             NULL,
    LOAD_TS                 DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- stg.ETL_RUN_LOG  (no legacy DDL; frozen by AZSYN contract v1)
-- Append-only audit log written by every proc / Spark job: small, insert-heavy.
IF OBJECT_ID(N'stg.ETL_RUN_LOG', N'U') IS NULL
CREATE TABLE stg.ETL_RUN_LOG
(
    JOB_NAME    VARCHAR(100)    NOT NULL,
    STEP_NAME   VARCHAR(100)    NULL,
    STATUS      VARCHAR(20)     NOT NULL,
    ROW_COUNT   BIGINT          NULL,
    START_TS    DATETIME2(6)    NULL,
    END_TS      DATETIME2(6)    NULL,
    MESSAGE     VARCHAR(4000)   NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
GO

-- Statistics (was COLLECT STATISTICS in ddl/01_staging_tables.sql).
-- The staging procs re-create these after each CTAS.
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_STG_CUSTOMER_360_CUSTOMER_ID' AND object_id = OBJECT_ID(N'stg.STG_CUSTOMER_360'))
    CREATE STATISTICS st_STG_CUSTOMER_360_CUSTOMER_ID ON stg.STG_CUSTOMER_360 (CUSTOMER_ID);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_STG_TXN_SUMMARY_CUSTOMER_ID' AND object_id = OBJECT_ID(N'stg.STG_TXN_SUMMARY'))
    CREATE STATISTICS st_STG_TXN_SUMMARY_CUSTOMER_ID ON stg.STG_TXN_SUMMARY (CUSTOMER_ID);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_STG_TXN_SUMMARY_CUSTOMER_ID_ACCOUNT_ID' AND object_id = OBJECT_ID(N'stg.STG_TXN_SUMMARY'))
    CREATE STATISTICS st_STG_TXN_SUMMARY_CUSTOMER_ID_ACCOUNT_ID ON stg.STG_TXN_SUMMARY (CUSTOMER_ID, ACCOUNT_ID);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_STG_RISK_FACTORS_CUSTOMER_ID' AND object_id = OBJECT_ID(N'stg.STG_RISK_FACTORS'))
    CREATE STATISTICS st_STG_RISK_FACTORS_CUSTOMER_ID ON stg.STG_RISK_FACTORS (CUSTOMER_ID);
GO
