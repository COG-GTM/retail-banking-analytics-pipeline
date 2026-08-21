-- =============================================================================
-- Snowflake Data Product Table DDL - Certified Analytical Outputs
-- Migrated from ddl/02_data_product_tables.sql (Teradata)
-- =============================================================================
-- These are the final, consumable data products produced by the analytics
-- pipeline and are the "contract" with downstream consumers. They are created
-- with IF NOT EXISTS so re-running the script in DEV/UAT/PROD never drops
-- published history; column changes go through a versioned ALTER script.
--
-- Prerequisite: run ddl/snowflake/00_databases_schemas.sql first.
-- Type mapping rationale: docs/modernization/snowflake_type_mapping.md
-- =============================================================================

USE DATABASE IDENTIFIER($DB_RETAIL);

-- -----------------------------------------------------------------------------
-- DATA_PRODUCTS.CUSTOMER_SEGMENTS  (was DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS)
-- Behavioural customer segmentation produced by k-means clustering.
-- Refresh: Daily | Consumers: Marketing campaign engine, CRM dashboards
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DATA_PRODUCTS.CUSTOMER_SEGMENTS
(
    CUSTOMER_ID             NUMBER(19,0)    NOT NULL,
    SEGMENT_NAME            VARCHAR(40),
    SEGMENT_ID              NUMBER(5,0),
    SUBSEGMENT_ID           NUMBER(5,0),
    LIFETIME_VALUE_SCORE    NUMBER(10,2),
    ENGAGEMENT_SCORE        NUMBER(5,2),
    DIGITAL_ADOPTION_SCORE  NUMBER(5,2),
    PRODUCT_BREADTH_INDEX   NUMBER(5,2),
    TENURE_GROUP            VARCHAR(20),
    AGE_GROUP               VARCHAR(20),
    BALANCE_TIER            VARCHAR(20),
    CHANNEL_PREFERENCE      VARCHAR(10),
    CROSS_SELL_FLAG         VARCHAR(1)      DEFAULT 'N',
    UPSELL_FLAG             VARCHAR(1)      DEFAULT 'N',
    RETENTION_RISK_FLAG     VARCHAR(1)      DEFAULT 'N',
    MODEL_VERSION           VARCHAR(20),
    EFFECTIVE_DATE          DATE,
    LOAD_TS                 TIMESTAMP_NTZ(6)
);

-- -----------------------------------------------------------------------------
-- DATA_PRODUCTS.TRANSACTION_ANALYTICS  (was DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS)
-- Per-customer transaction behaviour summary with trend indicators.
-- Teradata PARTITION BY COLUMN(REPORTING_PERIOD) becomes a clustering key:
-- consumers filter almost exclusively by reporting month.
-- Refresh: Daily | Consumers: Finance reporting, BI dashboards, Fraud team
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DATA_PRODUCTS.TRANSACTION_ANALYTICS
(
    CUSTOMER_ID                 NUMBER(19,0)    NOT NULL,
    REPORTING_PERIOD            VARCHAR(7),                 -- YYYY-MM
    TOTAL_ACCOUNTS              NUMBER(5,0),
    ACTIVE_ACCOUNTS             NUMBER(5,0),
    TOTAL_TRANSACTIONS          NUMBER(10,0),
    TOTAL_DEBIT_AMT             NUMBER(18,2),
    TOTAL_CREDIT_AMT            NUMBER(18,2),
    NET_CASH_FLOW               NUMBER(18,2),
    AVG_TRANSACTION_SIZE        NUMBER(15,2),
    MONTHLY_SPEND_TREND         VARCHAR(10),                -- UP, DOWN, STABLE
    SPEND_PERCENTILE            NUMBER(5,2),
    TOP_SPEND_CATEGORY          VARCHAR(60),
    DIGITAL_TXN_PCT             NUMBER(5,2),
    FEE_INCOME                  NUMBER(15,2),
    INTEREST_INCOME             NUMBER(15,2),
    REVENUE_CONTRIBUTION        NUMBER(15,2),
    ANOMALY_FLAG                VARCHAR(1)      DEFAULT 'N',
    MODEL_VERSION               VARCHAR(20),
    EFFECTIVE_DATE              DATE,
    LOAD_TS                     TIMESTAMP_NTZ(6)
)
CLUSTER BY (REPORTING_PERIOD);

-- -----------------------------------------------------------------------------
-- DATA_PRODUCTS.CUSTOMER_RISK_SCORES  (was DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES)
-- Composite risk scores combining internal analytics with bureau data.
-- Refresh: Daily | Consumers: Credit decisioning, Collections, Compliance
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DATA_PRODUCTS.CUSTOMER_RISK_SCORES
(
    CUSTOMER_ID                 NUMBER(19,0)    NOT NULL,
    COMPOSITE_RISK_SCORE        NUMBER(6,2),
    RISK_TIER                   VARCHAR(20),                -- LOW, MODERATE, ELEVATED, HIGH, CRITICAL
    PROBABILITY_OF_DEFAULT      NUMBER(7,6),
    CREDIT_RISK_COMPONENT       NUMBER(5,2),
    BEHAVIOUR_RISK_COMPONENT    NUMBER(5,2),
    VELOCITY_RISK_COMPONENT     NUMBER(5,2),
    BUREAU_SCORE_COMPONENT      NUMBER(5,2),
    PAYMENT_HISTORY_COMPONENT   NUMBER(5,2),
    PRIMARY_RISK_DRIVER         VARCHAR(40),
    SECONDARY_RISK_DRIVER       VARCHAR(40),
    SCORE_DELTA_30D             NUMBER(6,2),
    WATCH_LIST_FLAG             VARCHAR(1)      DEFAULT 'N',
    REVIEW_REQUIRED_FLAG        VARCHAR(1)      DEFAULT 'N',
    MODEL_VERSION               VARCHAR(20),
    EFFECTIVE_DATE              DATE,
    LOAD_TS                     TIMESTAMP_NTZ(6)
);

-- -----------------------------------------------------------------------------
-- DATA_PRODUCTS.CUSTOMER_MASTER_PROFILE  (was DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE)
-- Golden record: the single unified view joining all data products.
-- Refresh: Daily (after all upstream products complete) | Consumers: Enterprise-wide
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DATA_PRODUCTS.CUSTOMER_MASTER_PROFILE
(
    CUSTOMER_ID                 NUMBER(19,0)    NOT NULL,
    FULL_NAME                   VARCHAR(120),
    AGE                         NUMBER(5,0),
    STATE_CODE                  VARCHAR(2),
    CUSTOMER_SINCE              DATE,
    TENURE_MONTHS               NUMBER(10,0),
    CUSTOMER_STATUS             VARCHAR(1),
    -- Segment Data
    SEGMENT_NAME                VARCHAR(40),
    LIFETIME_VALUE_SCORE        NUMBER(10,2),
    ENGAGEMENT_SCORE            NUMBER(5,2),
    -- Account Summary
    TOTAL_ACCOUNTS              NUMBER(5,0),
    ACTIVE_ACCOUNTS             NUMBER(5,0),
    TOTAL_BALANCE               NUMBER(18,2),
    TOTAL_CREDIT_LIMIT          NUMBER(18,2),
    CREDIT_UTILIZATION_PCT      NUMBER(5,2),
    -- Transaction Summary
    MONTHLY_TRANSACTIONS        NUMBER(10,0),
    MONTHLY_SPEND               NUMBER(18,2),
    NET_CASH_FLOW               NUMBER(18,2),
    TOP_SPEND_CATEGORY          VARCHAR(60),
    DIGITAL_TXN_PCT             NUMBER(5,2),
    -- Risk Profile
    COMPOSITE_RISK_SCORE        NUMBER(6,2),
    RISK_TIER                   VARCHAR(20),
    PROBABILITY_OF_DEFAULT      NUMBER(7,6),
    WATCH_LIST_FLAG             VARCHAR(1)      DEFAULT 'N',
    -- Actionable Flags
    CROSS_SELL_FLAG             VARCHAR(1)      DEFAULT 'N',
    UPSELL_FLAG                 VARCHAR(1)      DEFAULT 'N',
    RETENTION_RISK_FLAG         VARCHAR(1)      DEFAULT 'N',
    -- Metadata
    MODEL_VERSION               VARCHAR(20),
    EFFECTIVE_DATE              DATE,
    LOAD_TS                     TIMESTAMP_NTZ(6)
);

-- The Teradata COLLECT STATISTICS block on the data product tables is dropped:
-- Snowflake maintains column statistics automatically.
