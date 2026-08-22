-- =============================================================================
-- Snowflake DDL for the CUSTOMER_RISK_SCORES data product (MBA-2209 / TICKET-08)
-- =============================================================================
-- Teradata source: ddl/02_data_product_tables.sql
--   * MULTISET / NO FALLBACK / PRIMARY INDEX have no Snowflake equivalent and are
--     dropped; clustering is unnecessary at this table size.
--   * CHARACTER SET LATIN NOT CASESPECIFIC -> plain VARCHAR (Snowflake is UTF-8
--     and case-sensitive on comparison; downstream joins use CUSTOMER_ID).
--   * TIMESTAMP(6) -> TIMESTAMP_NTZ(6).
--   * COLLECT STATISTICS is not needed (Snowflake maintains micro-partition stats).
-- Database/schema names are supplied by the deployment tooling from TICKET-01 /
-- TICKET-02; the placeholders below default to ETL/DATA_PRODUCTS conventions.
-- =============================================================================

USE DATABASE IDENTIFIER($SNOWFLAKE_DATABASE);
USE SCHEMA IDENTIFIER($SNOWFLAKE_DATA_PRODUCT_SCHEMA);

CREATE TABLE IF NOT EXISTS CUSTOMER_RISK_SCORES
(
    CUSTOMER_ID                 NUMBER(38,0)    NOT NULL,
    COMPOSITE_RISK_SCORE        NUMBER(6,2),
    RISK_TIER                   VARCHAR(20),          -- LOW, MODERATE, ELEVATED, HIGH, CRITICAL
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
-- Model auditability: one row per scoring run with the coefficients and the
-- variable set retained by the stepwise selection (SAS wrote these only to the
-- job log via PROC LOGISTIC output).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RISK_MODEL_RUNS
(
    RUN_TIMESTAMP   VARCHAR(20)     NOT NULL,
    MODEL_VERSION   VARCHAR(20)     NOT NULL,
    MODEL_AUDIT     VARCHAR         NOT NULL      -- JSON: coefficients, p-values, steps
);
