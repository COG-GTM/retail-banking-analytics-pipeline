-- =============================================================================
-- Data Product Table DDL - Certified Analytical Outputs
-- =============================================================================
-- These are the final, consumable data products produced by the SAS pipeline.
-- They are the "contract" with downstream consumers (dashboards, reports, APIs).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DP.CUSTOMER_SEGMENTS
-- Behavioural customer segmentation produced by k-means clustering in SAS.
-- Refresh: Daily
-- Consumers: Marketing campaign engine, CRM dashboards
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS, NO FALLBACK
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    SEGMENT_NAME            VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    SEGMENT_ID              SMALLINT,
    SUBSEGMENT_ID           SMALLINT,
    LIFETIME_VALUE_SCORE    DECIMAL(10,2),
    ENGAGEMENT_SCORE        DECIMAL(5,2),
    DIGITAL_ADOPTION_SCORE  DECIMAL(5,2),
    PRODUCT_BREADTH_INDEX   DECIMAL(5,2),
    TENURE_GROUP            VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    AGE_GROUP               VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    BALANCE_TIER            VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    CHANNEL_PREFERENCE      VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    CROSS_SELL_FLAG         CHAR(1)         DEFAULT 'N',
    UPSELL_FLAG             CHAR(1)         DEFAULT 'N',
    RETENTION_RISK_FLAG     CHAR(1)         DEFAULT 'N',
    MODEL_VERSION           VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    EFFECTIVE_DATE          DATE            FORMAT 'YYYY-MM-DD',
    LOAD_TS                 TIMESTAMP(6)
)
PRIMARY INDEX (CUSTOMER_ID);

-- -----------------------------------------------------------------------------
-- DP.TRANSACTION_ANALYTICS
-- Per-customer transaction behaviour summary with trend indicators.
-- Refresh: Daily
-- Consumers: Finance reporting, BI dashboards, Fraud team
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS, NO FALLBACK
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    REPORTING_PERIOD            VARCHAR(7)      CHARACTER SET LATIN NOT CASESPECIFIC,  -- YYYY-MM
    TOTAL_ACCOUNTS              SMALLINT,
    ACTIVE_ACCOUNTS             SMALLINT,
    TOTAL_TRANSACTIONS          INTEGER,
    TOTAL_DEBIT_AMT             DECIMAL(18,2),
    TOTAL_CREDIT_AMT            DECIMAL(18,2),
    NET_CASH_FLOW               DECIMAL(18,2),
    AVG_TRANSACTION_SIZE        DECIMAL(15,2),
    MONTHLY_SPEND_TREND         VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- UP, DOWN, STABLE
    SPEND_PERCENTILE            DECIMAL(5,2),
    TOP_SPEND_CATEGORY          VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    DIGITAL_TXN_PCT             DECIMAL(5,2),
    FEE_INCOME                  DECIMAL(15,2),
    INTEREST_INCOME             DECIMAL(15,2),
    REVENUE_CONTRIBUTION        DECIMAL(15,2),
    ANOMALY_FLAG                CHAR(1)         DEFAULT 'N',
    MODEL_VERSION               VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    EFFECTIVE_DATE              DATE            FORMAT 'YYYY-MM-DD',
    LOAD_TS                     TIMESTAMP(6)
)
PRIMARY INDEX (CUSTOMER_ID)
PARTITION BY COLUMN(REPORTING_PERIOD VARCHAR(7));

-- -----------------------------------------------------------------------------
-- DP.CUSTOMER_RISK_SCORES
-- Composite risk scores combining internal analytics with bureau data.
-- Refresh: Daily
-- Consumers: Credit decisioning, Collections, Compliance
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES, NO FALLBACK
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    COMPOSITE_RISK_SCORE        DECIMAL(6,2),
    RISK_TIER                   VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- LOW, MODERATE, ELEVATED, HIGH, CRITICAL
    PROBABILITY_OF_DEFAULT      DECIMAL(7,6),
    CREDIT_RISK_COMPONENT       DECIMAL(5,2),
    BEHAVIOUR_RISK_COMPONENT    DECIMAL(5,2),
    VELOCITY_RISK_COMPONENT     DECIMAL(5,2),
    BUREAU_SCORE_COMPONENT      DECIMAL(5,2),
    PAYMENT_HISTORY_COMPONENT   DECIMAL(5,2),
    PRIMARY_RISK_DRIVER         VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    SECONDARY_RISK_DRIVER       VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    SCORE_DELTA_30D             DECIMAL(6,2),
    WATCH_LIST_FLAG             CHAR(1)         DEFAULT 'N',
    REVIEW_REQUIRED_FLAG        CHAR(1)         DEFAULT 'N',
    MODEL_VERSION               VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    EFFECTIVE_DATE              DATE            FORMAT 'YYYY-MM-DD',
    LOAD_TS                     TIMESTAMP(6)
)
PRIMARY INDEX (CUSTOMER_ID);

-- -----------------------------------------------------------------------------
-- DP.CUSTOMER_MASTER_PROFILE
-- Golden record: the single unified view joining all data products.
-- Refresh: Daily (after all upstream products complete)
-- Consumers: Enterprise-wide; canonical customer reference
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE, NO FALLBACK
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    FULL_NAME                   VARCHAR(120)    CHARACTER SET LATIN NOT CASESPECIFIC,
    AGE                         SMALLINT,
    STATE_CODE                  CHAR(2)         CHARACTER SET LATIN NOT CASESPECIFIC,
    CUSTOMER_SINCE              DATE            FORMAT 'YYYY-MM-DD',
    TENURE_MONTHS               INTEGER,
    CUSTOMER_STATUS             CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC,
    -- Segment Data
    SEGMENT_NAME                VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    LIFETIME_VALUE_SCORE        DECIMAL(10,2),
    ENGAGEMENT_SCORE            DECIMAL(5,2),
    -- Account Summary
    TOTAL_ACCOUNTS              SMALLINT,
    ACTIVE_ACCOUNTS             SMALLINT,
    TOTAL_BALANCE               DECIMAL(18,2),
    TOTAL_CREDIT_LIMIT          DECIMAL(18,2),
    CREDIT_UTILIZATION_PCT      DECIMAL(5,2),
    -- Transaction Summary
    MONTHLY_TRANSACTIONS        INTEGER,
    MONTHLY_SPEND               DECIMAL(18,2),
    NET_CASH_FLOW               DECIMAL(18,2),
    TOP_SPEND_CATEGORY          VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    DIGITAL_TXN_PCT             DECIMAL(5,2),
    -- Risk Profile
    COMPOSITE_RISK_SCORE        DECIMAL(6,2),
    RISK_TIER                   VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    PROBABILITY_OF_DEFAULT      DECIMAL(7,6),
    WATCH_LIST_FLAG             CHAR(1)         DEFAULT 'N',
    -- Actionable Flags
    CROSS_SELL_FLAG             CHAR(1)         DEFAULT 'N',
    UPSELL_FLAG                 CHAR(1)         DEFAULT 'N',
    RETENTION_RISK_FLAG         CHAR(1)         DEFAULT 'N',
    -- Metadata
    MODEL_VERSION               VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    EFFECTIVE_DATE              DATE            FORMAT 'YYYY-MM-DD',
    LOAD_TS                     TIMESTAMP(6)
)
PRIMARY INDEX (CUSTOMER_ID);

-- Collect statistics on all data product tables
COLLECT STATISTICS COLUMN (CUSTOMER_ID)       ON DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS;
COLLECT STATISTICS COLUMN (CUSTOMER_ID)       ON DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS;
COLLECT STATISTICS COLUMN (CUSTOMER_ID)       ON DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES;
COLLECT STATISTICS COLUMN (CUSTOMER_ID)       ON DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE;
COLLECT STATISTICS COLUMN (SEGMENT_NAME)      ON DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS;
COLLECT STATISTICS COLUMN (RISK_TIER)         ON DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES;
COLLECT STATISTICS COLUMN (REPORTING_PERIOD)  ON DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS;
