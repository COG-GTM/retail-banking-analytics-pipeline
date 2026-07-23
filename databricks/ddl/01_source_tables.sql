-- =============================================================================
-- Ticket 1 - Source table DDL (Delta)
-- =============================================================================
-- Delta equivalents of ddl/00_source_tables.sql. Teradata-specific clauses
-- (MULTISET, NO FALLBACK, CHARACTER SET ... NOT CASESPECIFIC, FORMAT,
-- PRIMARY INDEX, PARTITION BY RANGE_N) are removed; DECIMAL / TIMESTAMP types
-- are preserved. VARCHAR/CHAR become STRING; INTEGER becomes INT.
-- These upstream tables are owned by the core banking platform; here they are
-- created so source data (e.g. the sample CSVs) can be landed for the pipeline.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${catalog}.${core_schema}.customers (
    customer_id      BIGINT,
    first_name       STRING,
    last_name        STRING,
    date_of_birth    DATE,
    ssn_hash         STRING,
    email            STRING,
    phone_primary    STRING,
    customer_since   DATE,
    customer_status  STRING,   -- A=Active, I=Inactive, C=Closed
    segment_code     STRING,
    branch_id        INT,
    created_ts       TIMESTAMP,
    updated_ts       TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${core_schema}.accounts (
    account_id        BIGINT,
    customer_id       BIGINT,
    account_type      STRING,   -- CHECKING, SAVINGS, CREDIT, LOAN
    account_status    STRING,   -- O=Open, C=Closed, F=Frozen
    open_date         DATE,
    close_date        DATE,
    current_balance   DECIMAL(15,2),
    available_balance DECIMAL(15,2),
    credit_limit      DECIMAL(15,2),
    interest_rate     DECIMAL(5,4),
    branch_id         INT,
    created_ts        TIMESTAMP,
    updated_ts        TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${core_schema}.addresses (
    address_id      BIGINT,
    customer_id     BIGINT,
    address_type    STRING,   -- MAIL, HOME, WORK
    address_line_1  STRING,
    address_line_2  STRING,
    city            STRING,
    state_code      STRING,
    zip_code        STRING,
    country_code    STRING,
    is_primary      STRING,
    effective_date  DATE,
    expiration_date DATE,
    created_ts      TIMESTAMP,
    updated_ts      TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${core_schema}.customer_bureau_scores (
    customer_id           BIGINT,
    external_credit_score INT,
    report_date           DATE
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${txn_schema}.transactions (
    transaction_id      BIGINT,
    account_id          BIGINT,
    transaction_type_cd STRING,
    transaction_date    DATE,
    transaction_ts      TIMESTAMP,
    amount              DECIMAL(15,2),
    running_balance     DECIMAL(15,2),
    merchant_name       STRING,
    merchant_category   STRING,
    channel_code        STRING,   -- ATM, POS, WEB, MOB, ACH, WIRE, INTL
    reference_num       STRING,
    status_code         STRING,   -- P=Posted, R=Reversed, H=Hold
    created_ts          TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${txn_schema}.transaction_types (
    transaction_type_cd STRING,
    description         STRING,
    category            STRING,   -- DEBIT, CREDIT, FEE, INTEREST
    is_revenue          STRING,
    effective_date      DATE,
    expiration_date     DATE
) USING DELTA;
