-- =============================================================================
-- Source Table DDL - Retail Banking Operational Systems (Databricks / Delta)
-- =============================================================================
-- Migrated from ddl/00_source_tables.sql (Teradata).
--
-- These tables represent the upstream operational sources that feed the
-- analytics pipeline. They are owned by the core banking platform team.
-- In Teradata they lived in four databases (CORE_BANKING_DB, TXN_PROCESSING_DB);
-- on Databricks they become Unity Catalog schemas under a single catalog.
--
-- Migration notes:
--   * CREATE MULTISET TABLE ... NO FALLBACK   -> CREATE TABLE ... USING DELTA
--   * PRIMARY INDEX (...)                      -> CLUSTER BY (liquid clustering)
--   * VARCHAR(n)/CHAR(n)                        -> STRING
--   * SMALLINT/INTEGER/BIGINT/DECIMAL(p,s)      -> kept as-is
--   * TIMESTAMP(6)                              -> TIMESTAMP
--   * Teradata FORMAT clauses                   -> dropped
--   * COLLECT STATISTICS                        -> dropped (optional ANALYZE TABLE)
--   * PARTITION BY RANGE_N(... month)           -> partitioning handled by Delta;
--                                                 use CLUSTER BY for data skipping
--
-- The {{CATALOG}} token is substituted with the configured Unity Catalog name
-- (default: retail_banking) by databricks/setup/00_setup_unity_catalog.py.
-- This DDL primarily documents the source contract; the demo loader can also
-- populate these tables with synthetic data for an end-to-end local run.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS {{CATALOG}};

CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.core_banking;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.txn_processing;

-- -----------------------------------------------------------------------------
-- core_banking.customers
-- Master customer record from the core banking platform.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.core_banking.customers
(
    customer_id         BIGINT          NOT NULL,
    first_name          STRING,
    last_name           STRING,
    date_of_birth       DATE,
    ssn_hash            STRING,
    email               STRING,
    phone_primary       STRING,
    customer_since      DATE,
    customer_status     STRING          COMMENT 'A=Active, I=Inactive, C=Closed',
    segment_code        STRING,
    branch_id           INT,
    created_ts          TIMESTAMP,
    updated_ts          TIMESTAMP
)
USING DELTA
CLUSTER BY (customer_id);

-- -----------------------------------------------------------------------------
-- core_banking.accounts
-- Account-level detail (checking, savings, credit, loan).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.core_banking.accounts
(
    account_id          BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,
    account_type        STRING          COMMENT 'CHECKING, SAVINGS, CREDIT, LOAN',
    account_status      STRING          COMMENT 'O=Open, C=Closed, F=Frozen',
    open_date           DATE,
    close_date          DATE,
    current_balance     DECIMAL(15,2),
    available_balance   DECIMAL(15,2),
    credit_limit        DECIMAL(15,2),
    interest_rate       DECIMAL(5,4),
    branch_id           INT,
    created_ts          TIMESTAMP,
    updated_ts          TIMESTAMP
)
USING DELTA
CLUSTER BY (account_id);

-- -----------------------------------------------------------------------------
-- core_banking.addresses
-- Customer mailing and residential addresses.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.core_banking.addresses
(
    address_id          BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,
    address_type        STRING          COMMENT 'MAIL, HOME, WORK',
    address_line_1      STRING,
    address_line_2      STRING,
    city                STRING,
    state_code          STRING,
    zip_code            STRING,
    country_code        STRING,
    is_primary          STRING,
    effective_date      DATE,
    expiration_date     DATE,
    created_ts          TIMESTAMP,
    updated_ts          TIMESTAMP
)
USING DELTA
CLUSTER BY (address_id);

-- -----------------------------------------------------------------------------
-- txn_processing.transactions
-- Individual financial transactions across all account types.
-- In Teradata this was range-partitioned by TRANSACTION_DATE (monthly). On
-- Delta, CLUSTER BY on the most-filtered columns provides equivalent skipping.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.txn_processing.transactions
(
    transaction_id      BIGINT          NOT NULL,
    account_id          BIGINT          NOT NULL,
    transaction_type_cd STRING,
    transaction_date    DATE,
    transaction_ts      TIMESTAMP,
    amount              DECIMAL(15,2),
    running_balance     DECIMAL(15,2),
    merchant_name       STRING,
    merchant_category   STRING,
    channel_code        STRING          COMMENT 'ATM, POS, WEB, MOB, ACH, WIRE',
    reference_num       STRING,
    status_code         STRING          COMMENT 'P=Posted, R=Reversed, H=Hold',
    created_ts          TIMESTAMP
)
USING DELTA
CLUSTER BY (transaction_date, account_id);

-- -----------------------------------------------------------------------------
-- txn_processing.transaction_types
-- Reference/lookup table for transaction type codes.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.txn_processing.transaction_types
(
    transaction_type_cd STRING          NOT NULL,
    description         STRING,
    category            STRING          COMMENT 'DEBIT, CREDIT, FEE, INTEREST',
    is_revenue          STRING,
    effective_date      DATE,
    expiration_date     DATE
)
USING DELTA
CLUSTER BY (transaction_type_cd);

-- -----------------------------------------------------------------------------
-- core_banking.customer_bureau_scores
-- External credit bureau scores (latest snapshot per customer).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{CATALOG}}.core_banking.customer_bureau_scores
(
    customer_id            BIGINT,
    external_credit_score  INT,
    report_date            DATE
)
USING DELTA
CLUSTER BY (customer_id);
