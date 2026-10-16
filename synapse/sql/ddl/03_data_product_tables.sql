-- =============================================================================
-- AZSYN-01: Data product tables (dp.*) - Synapse dedicated SQL pool
-- =============================================================================
-- Translated from ddl/02_data_product_tables.sql (Teradata) per AZSYN contract v1.
-- Written by the Spark jobs (synapse/spark/jobs/*.py) through write_table().
--
--   PRIMARY INDEX (CUSTOMER_ID)              -> DISTRIBUTION = HASH(CUSTOMER_ID), CCI
--   PARTITION BY COLUMN (TRANSACTION_ANALYTICS) -> dropped; CCI is already columnar
--   COLLECT STATISTICS COLUMN (...)          -> CREATE STATISTICS st_<TABLE>_<COL>
--   CHARACTER SET / CASESPECIFIC / FORMAT    -> dropped; TIMESTAMP(6) -> DATETIME2(6)
-- Idempotent and non-destructive: objects are created only if missing.
-- =============================================================================

-- dp.CUSTOMER_SEGMENTS  (sas/01_sas_customer_segments.sas)
IF OBJECT_ID(N'dp.CUSTOMER_SEGMENTS', N'U') IS NULL
CREATE TABLE dp.CUSTOMER_SEGMENTS
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    SEGMENT_NAME            VARCHAR(40)     NULL,
    SEGMENT_ID              SMALLINT        NULL,
    SUBSEGMENT_ID           SMALLINT        NULL,
    LIFETIME_VALUE_SCORE    DECIMAL(10,2)   NULL,
    ENGAGEMENT_SCORE        DECIMAL(5,2)    NULL,
    DIGITAL_ADOPTION_SCORE  DECIMAL(5,2)    NULL,
    PRODUCT_BREADTH_INDEX   DECIMAL(5,2)    NULL,
    TENURE_GROUP            VARCHAR(20)     NULL,
    AGE_GROUP               VARCHAR(20)     NULL,
    BALANCE_TIER            VARCHAR(20)     NULL,
    CHANNEL_PREFERENCE      VARCHAR(10)     NULL,
    CROSS_SELL_FLAG         CHAR(1)         NULL DEFAULT 'N',
    UPSELL_FLAG             CHAR(1)         NULL DEFAULT 'N',
    RETENTION_RISK_FLAG     CHAR(1)         NULL DEFAULT 'N',
    MODEL_VERSION           VARCHAR(20)     NULL,
    EFFECTIVE_DATE          DATE            NULL,
    LOAD_TS                 DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- dp.TRANSACTION_ANALYTICS  (sas/02_sas_txn_analytics.sas)
-- Teradata PARTITION BY COLUMN(REPORTING_PERIOD) (column partitioning) is dropped:
-- the clustered columnstore index already stores data by column.
IF OBJECT_ID(N'dp.TRANSACTION_ANALYTICS', N'U') IS NULL
CREATE TABLE dp.TRANSACTION_ANALYTICS
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    REPORTING_PERIOD            VARCHAR(7)      NULL,   -- YYYY-MM
    TOTAL_ACCOUNTS              SMALLINT        NULL,
    ACTIVE_ACCOUNTS             SMALLINT        NULL,
    TOTAL_TRANSACTIONS          INT             NULL,
    TOTAL_DEBIT_AMT             DECIMAL(18,2)   NULL,
    TOTAL_CREDIT_AMT            DECIMAL(18,2)   NULL,
    NET_CASH_FLOW               DECIMAL(18,2)   NULL,
    AVG_TRANSACTION_SIZE        DECIMAL(15,2)   NULL,
    MONTHLY_SPEND_TREND         VARCHAR(10)     NULL,   -- UP, DOWN, STABLE
    SPEND_PERCENTILE            DECIMAL(5,2)    NULL,
    TOP_SPEND_CATEGORY          VARCHAR(60)     NULL,
    DIGITAL_TXN_PCT             DECIMAL(5,2)    NULL,
    FEE_INCOME                  DECIMAL(15,2)   NULL,
    INTEREST_INCOME             DECIMAL(15,2)   NULL,
    REVENUE_CONTRIBUTION        DECIMAL(15,2)   NULL,
    ANOMALY_FLAG                CHAR(1)         NULL DEFAULT 'N',
    MODEL_VERSION               VARCHAR(20)     NULL,
    EFFECTIVE_DATE              DATE            NULL,
    LOAD_TS                     DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- dp.CUSTOMER_RISK_SCORES  (sas/03_sas_risk_scoring.sas)
IF OBJECT_ID(N'dp.CUSTOMER_RISK_SCORES', N'U') IS NULL
CREATE TABLE dp.CUSTOMER_RISK_SCORES
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    COMPOSITE_RISK_SCORE        DECIMAL(6,2)    NULL,
    RISK_TIER                   VARCHAR(20)     NULL,   -- LOW, MODERATE, ELEVATED, HIGH, CRITICAL
    PROBABILITY_OF_DEFAULT      DECIMAL(7,6)    NULL,
    CREDIT_RISK_COMPONENT       DECIMAL(5,2)    NULL,
    BEHAVIOUR_RISK_COMPONENT    DECIMAL(5,2)    NULL,
    VELOCITY_RISK_COMPONENT     DECIMAL(5,2)    NULL,
    BUREAU_SCORE_COMPONENT      DECIMAL(5,2)    NULL,
    PAYMENT_HISTORY_COMPONENT   DECIMAL(5,2)    NULL,
    PRIMARY_RISK_DRIVER         VARCHAR(40)     NULL,
    SECONDARY_RISK_DRIVER       VARCHAR(40)     NULL,
    SCORE_DELTA_30D             DECIMAL(6,2)    NULL,
    WATCH_LIST_FLAG             CHAR(1)         NULL DEFAULT 'N',
    REVIEW_REQUIRED_FLAG        CHAR(1)         NULL DEFAULT 'N',
    MODEL_VERSION               VARCHAR(20)     NULL,
    EFFECTIVE_DATE              DATE            NULL,
    LOAD_TS                     DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- dp.CUSTOMER_MASTER_PROFILE  (sas/04_sas_data_products.sas) - golden record
IF OBJECT_ID(N'dp.CUSTOMER_MASTER_PROFILE', N'U') IS NULL
CREATE TABLE dp.CUSTOMER_MASTER_PROFILE
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    FULL_NAME                   VARCHAR(120)    NULL,
    AGE                         SMALLINT        NULL,
    STATE_CODE                  CHAR(2)         NULL,
    CUSTOMER_SINCE              DATE            NULL,
    TENURE_MONTHS               INT             NULL,
    CUSTOMER_STATUS             CHAR(1)         NULL,
    -- Segment Data
    SEGMENT_NAME                VARCHAR(40)     NULL,
    LIFETIME_VALUE_SCORE        DECIMAL(10,2)   NULL,
    ENGAGEMENT_SCORE            DECIMAL(5,2)    NULL,
    -- Account Summary
    TOTAL_ACCOUNTS              SMALLINT        NULL,
    ACTIVE_ACCOUNTS             SMALLINT        NULL,
    TOTAL_BALANCE               DECIMAL(18,2)   NULL,
    TOTAL_CREDIT_LIMIT          DECIMAL(18,2)   NULL,
    CREDIT_UTILIZATION_PCT      DECIMAL(5,2)    NULL,
    -- Transaction Summary
    MONTHLY_TRANSACTIONS        INT             NULL,
    MONTHLY_SPEND               DECIMAL(18,2)   NULL,
    NET_CASH_FLOW               DECIMAL(18,2)   NULL,
    TOP_SPEND_CATEGORY          VARCHAR(60)     NULL,
    DIGITAL_TXN_PCT             DECIMAL(5,2)    NULL,
    -- Risk Profile
    COMPOSITE_RISK_SCORE        DECIMAL(6,2)    NULL,
    RISK_TIER                   VARCHAR(20)     NULL,
    PROBABILITY_OF_DEFAULT      DECIMAL(7,6)    NULL,
    WATCH_LIST_FLAG             CHAR(1)         NULL DEFAULT 'N',
    -- Actionable Flags
    CROSS_SELL_FLAG             CHAR(1)         NULL DEFAULT 'N',
    UPSELL_FLAG                 CHAR(1)         NULL DEFAULT 'N',
    RETENTION_RISK_FLAG         CHAR(1)         NULL DEFAULT 'N',
    -- Metadata
    MODEL_VERSION               VARCHAR(20)     NULL,
    EFFECTIVE_DATE              DATE            NULL,
    LOAD_TS                     DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Statistics (was COLLECT STATISTICS in ddl/02_data_product_tables.sql)
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_CUSTOMER_SEGMENTS_CUSTOMER_ID' AND object_id = OBJECT_ID(N'dp.CUSTOMER_SEGMENTS'))
    CREATE STATISTICS st_CUSTOMER_SEGMENTS_CUSTOMER_ID ON dp.CUSTOMER_SEGMENTS (CUSTOMER_ID);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_TRANSACTION_ANALYTICS_CUSTOMER_ID' AND object_id = OBJECT_ID(N'dp.TRANSACTION_ANALYTICS'))
    CREATE STATISTICS st_TRANSACTION_ANALYTICS_CUSTOMER_ID ON dp.TRANSACTION_ANALYTICS (CUSTOMER_ID);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_CUSTOMER_RISK_SCORES_CUSTOMER_ID' AND object_id = OBJECT_ID(N'dp.CUSTOMER_RISK_SCORES'))
    CREATE STATISTICS st_CUSTOMER_RISK_SCORES_CUSTOMER_ID ON dp.CUSTOMER_RISK_SCORES (CUSTOMER_ID);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_CUSTOMER_MASTER_PROFILE_CUSTOMER_ID' AND object_id = OBJECT_ID(N'dp.CUSTOMER_MASTER_PROFILE'))
    CREATE STATISTICS st_CUSTOMER_MASTER_PROFILE_CUSTOMER_ID ON dp.CUSTOMER_MASTER_PROFILE (CUSTOMER_ID);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_CUSTOMER_SEGMENTS_SEGMENT_NAME' AND object_id = OBJECT_ID(N'dp.CUSTOMER_SEGMENTS'))
    CREATE STATISTICS st_CUSTOMER_SEGMENTS_SEGMENT_NAME ON dp.CUSTOMER_SEGMENTS (SEGMENT_NAME);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_CUSTOMER_RISK_SCORES_RISK_TIER' AND object_id = OBJECT_ID(N'dp.CUSTOMER_RISK_SCORES'))
    CREATE STATISTICS st_CUSTOMER_RISK_SCORES_RISK_TIER ON dp.CUSTOMER_RISK_SCORES (RISK_TIER);
GO
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'st_TRANSACTION_ANALYTICS_REPORTING_PERIOD' AND object_id = OBJECT_ID(N'dp.TRANSACTION_ANALYTICS'))
    CREATE STATISTICS st_TRANSACTION_ANALYTICS_REPORTING_PERIOD ON dp.TRANSACTION_ANALYTICS (REPORTING_PERIOD);
GO
