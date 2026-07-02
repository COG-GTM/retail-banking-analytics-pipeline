-- =============================================================================
-- Data Product Table DDL - Certified Analytical Outputs (Spark / Delta Lake)
-- =============================================================================
-- These are the final, consumable data products produced by the Spark
-- data-product jobs (spark/data_products/*.py, migrated from the former SAS
-- programs). They are the "contract" with downstream consumers (dashboards,
-- reports, APIs) and live in ${DB_DP}.
--
-- Migrated from Teradata DDL. Dropped physical-storage directives:
--   * MULTISET / NO FALLBACK / PRIMARY INDEX -> dropped (no Spark equivalent)
--   * COLLECT STATISTICS ...                 -> dropped (use ANALYZE TABLE on
--       demand; not part of the schema DDL)
--   * FORMAT '...' / CHARACTER SET LATIN ...  -> dropped (Teradata-specific)
--   * DEFAULT '...' flags                     -> dropped (set in transform jobs)
-- Partitioning:
--   * TRANSACTION_ANALYTICS: Teradata PARTITION BY COLUMN(REPORTING_PERIOD)
--       maps to Spark PARTITIONED BY (REPORTING_PERIOD) -- monthly (YYYY-MM).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ${DB_DP};

-- -----------------------------------------------------------------------------
-- CUSTOMER_SEGMENTS
-- Behavioural customer segmentation produced by k-means clustering.
-- Refresh: Daily | Consumers: Marketing campaign engine, CRM dashboards
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_DP}.CUSTOMER_SEGMENTS
(
    CUSTOMER_ID             BIGINT          NOT NULL,
    SEGMENT_NAME            VARCHAR(40),
    SEGMENT_ID              SMALLINT,
    SUBSEGMENT_ID           SMALLINT,
    LIFETIME_VALUE_SCORE    DECIMAL(10,2),
    ENGAGEMENT_SCORE        DECIMAL(5,2),
    DIGITAL_ADOPTION_SCORE  DECIMAL(5,2),
    PRODUCT_BREADTH_INDEX   DECIMAL(5,2),
    TENURE_GROUP            VARCHAR(20),
    AGE_GROUP               VARCHAR(20),
    BALANCE_TIER            VARCHAR(20),
    CHANNEL_PREFERENCE      VARCHAR(10),
    CROSS_SELL_FLAG         CHAR(1),
    UPSELL_FLAG             CHAR(1),
    RETENTION_RISK_FLAG     CHAR(1),
    MODEL_VERSION           VARCHAR(20),
    EFFECTIVE_DATE          DATE,
    LOAD_TS                 TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- TRANSACTION_ANALYTICS
-- Per-customer transaction behaviour summary with trend indicators.
-- Refresh: Daily | Consumers: Finance reporting, BI dashboards, Fraud team
-- Partitioned by REPORTING_PERIOD (YYYY-MM) -- see header note.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_DP}.TRANSACTION_ANALYTICS
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    TOTAL_ACCOUNTS              SMALLINT,
    ACTIVE_ACCOUNTS             SMALLINT,
    TOTAL_TRANSACTIONS          INT,
    TOTAL_DEBIT_AMT             DECIMAL(18,2),
    TOTAL_CREDIT_AMT            DECIMAL(18,2),
    NET_CASH_FLOW               DECIMAL(18,2),
    AVG_TRANSACTION_SIZE        DECIMAL(15,2),
    MONTHLY_SPEND_TREND         VARCHAR(10),                                    -- UP, DOWN, STABLE
    SPEND_PERCENTILE            DECIMAL(5,2),
    TOP_SPEND_CATEGORY          VARCHAR(60),
    DIGITAL_TXN_PCT             DECIMAL(5,2),
    FEE_INCOME                  DECIMAL(15,2),
    INTEREST_INCOME             DECIMAL(15,2),
    REVENUE_CONTRIBUTION        DECIMAL(15,2),
    ANOMALY_FLAG                CHAR(1),
    MODEL_VERSION               VARCHAR(20),
    EFFECTIVE_DATE              DATE,
    LOAD_TS                     TIMESTAMP,
    REPORTING_PERIOD            VARCHAR(7)                                      -- YYYY-MM (partition key)
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD);

-- -----------------------------------------------------------------------------
-- CUSTOMER_RISK_SCORES
-- Composite risk scores combining internal analytics with bureau data.
-- Refresh: Daily | Consumers: Credit decisioning, Collections, Compliance
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_DP}.CUSTOMER_RISK_SCORES
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    COMPOSITE_RISK_SCORE        DECIMAL(6,2),
    RISK_TIER                   VARCHAR(20),                                    -- LOW, MODERATE, ELEVATED, HIGH, CRITICAL
    PROBABILITY_OF_DEFAULT      DECIMAL(7,6),
    CREDIT_RISK_COMPONENT       DECIMAL(5,2),
    BEHAVIOUR_RISK_COMPONENT    DECIMAL(5,2),
    VELOCITY_RISK_COMPONENT     DECIMAL(5,2),
    BUREAU_SCORE_COMPONENT      DECIMAL(5,2),
    PAYMENT_HISTORY_COMPONENT   DECIMAL(5,2),
    PRIMARY_RISK_DRIVER         VARCHAR(40),
    SECONDARY_RISK_DRIVER       VARCHAR(40),
    SCORE_DELTA_30D             DECIMAL(6,2),
    WATCH_LIST_FLAG             CHAR(1),
    REVIEW_REQUIRED_FLAG        CHAR(1),
    MODEL_VERSION               VARCHAR(20),
    EFFECTIVE_DATE              DATE,
    LOAD_TS                     TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- CUSTOMER_MASTER_PROFILE
-- Golden record: the single unified view joining all data products.
-- Refresh: Daily (after all upstream products complete)
-- Consumers: Enterprise-wide; canonical customer reference
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_DP}.CUSTOMER_MASTER_PROFILE
(
    CUSTOMER_ID                 BIGINT          NOT NULL,
    FULL_NAME                   VARCHAR(120),
    AGE                         SMALLINT,
    STATE_CODE                  CHAR(2),
    CUSTOMER_SINCE              DATE,
    TENURE_MONTHS               INT,
    CUSTOMER_STATUS             CHAR(1),
    -- Segment Data
    SEGMENT_NAME                VARCHAR(40),
    LIFETIME_VALUE_SCORE        DECIMAL(10,2),
    ENGAGEMENT_SCORE            DECIMAL(5,2),
    -- Account Summary
    TOTAL_ACCOUNTS              SMALLINT,
    ACTIVE_ACCOUNTS             SMALLINT,
    TOTAL_BALANCE               DECIMAL(18,2),
    TOTAL_CREDIT_LIMIT          DECIMAL(18,2),
    CREDIT_UTILIZATION_PCT      DECIMAL(5,2),
    -- Transaction Summary
    MONTHLY_TRANSACTIONS        INT,
    MONTHLY_SPEND               DECIMAL(18,2),
    NET_CASH_FLOW               DECIMAL(18,2),
    TOP_SPEND_CATEGORY          VARCHAR(60),
    DIGITAL_TXN_PCT             DECIMAL(5,2),
    -- Risk Profile
    COMPOSITE_RISK_SCORE        DECIMAL(6,2),
    RISK_TIER                   VARCHAR(20),
    PROBABILITY_OF_DEFAULT      DECIMAL(7,6),
    WATCH_LIST_FLAG             CHAR(1),
    -- Actionable Flags
    CROSS_SELL_FLAG             CHAR(1),
    UPSELL_FLAG                 CHAR(1),
    RETENTION_RISK_FLAG         CHAR(1),
    -- Metadata
    MODEL_VERSION               VARCHAR(20),
    EFFECTIVE_DATE              DATE,
    LOAD_TS                     TIMESTAMP
)
USING DELTA;
