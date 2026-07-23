-- =============================================================================
-- Source Table DDL (Delta) - Retail Banking Operational Systems
-- =============================================================================
-- Delta port of ddl/00_source_tables.sql. These are the upstream operational
-- sources that feed the analytics pipeline. In Databricks they land as managed
-- Delta tables that double as the local test-seed target.
--   Teradata CORE_BANKING_DB   -> ${catalog}.${schema_core}
--   Teradata TXN_PROCESSING_DB -> ${catalog}.${schema_txn}
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Master customer record from the core banking platform.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_core}.customers (
    customer_id         BIGINT      NOT NULL,
    first_name          STRING,
    last_name           STRING,
    date_of_birth       DATE,
    ssn_hash            STRING,
    email               STRING,
    phone_primary       STRING,
    customer_since      DATE,
    customer_status     STRING      COMMENT 'A=Active, I=Inactive, C=Closed',
    segment_code        STRING,
    branch_id           INT,
    created_ts          TIMESTAMP,
    updated_ts          TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- Account-level detail (checking, savings, credit, loan).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_core}.accounts (
    account_id          BIGINT      NOT NULL,
    customer_id         BIGINT      NOT NULL,
    account_type        STRING      COMMENT 'CHECKING, SAVINGS, CREDIT, LOAN',
    account_status      STRING      COMMENT 'O=Open, C=Closed, F=Frozen',
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
USING DELTA;

-- -----------------------------------------------------------------------------
-- Customer mailing and residential addresses.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_core}.addresses (
    address_id          BIGINT      NOT NULL,
    customer_id         BIGINT      NOT NULL,
    address_type        STRING      COMMENT 'MAIL, HOME, WORK',
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
USING DELTA;

-- -----------------------------------------------------------------------------
-- Individual financial transactions across all account types.
-- Legacy Teradata partitioned on TRANSACTION_DATE (monthly RANGE_N); the
-- transaction summary is aggregated downstream so no partition is required here.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_txn}.transactions (
    transaction_id      BIGINT      NOT NULL,
    account_id          BIGINT      NOT NULL,
    transaction_type_cd STRING,
    transaction_date    DATE,
    transaction_ts      TIMESTAMP,
    amount              DECIMAL(15,2),
    running_balance     DECIMAL(15,2),
    merchant_name       STRING,
    merchant_category   STRING,
    channel_code        STRING      COMMENT 'ATM, POS, WEB, MOB, ACH, WIRE',
    reference_num       STRING,
    status_code         STRING      COMMENT 'P=Posted, R=Reversed, H=Hold',
    created_ts          TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- Reference/lookup table for transaction type codes.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_txn}.transaction_types (
    transaction_type_cd STRING      NOT NULL,
    description         STRING,
    category            STRING      COMMENT 'DEBIT, CREDIT, FEE, INTEREST',
    is_revenue          STRING,
    effective_date      DATE,
    expiration_date     DATE
)
USING DELTA;
