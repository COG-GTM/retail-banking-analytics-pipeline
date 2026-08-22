-- =============================================================================
-- Snowflake Source Table DDL - Retail Banking Operational Systems
-- =============================================================================
-- Migrated from ddl/00_source_tables.sql (Teradata).
-- These tables represent the upstream operational sources that feed the
-- analytics pipeline. They are owned by the core banking platform team.
-- This DDL is for DOCUMENTATION / lower-environment bootstrap only.
--
-- Usage: snowsql -f ddl/snowflake/01_source_tables.sql -D env=DEV
-- Idempotent: CREATE TABLE IF NOT EXISTS never drops existing operational data.
-- =============================================================================

!set variable_substitution=true

USE DATABASE RETAIL_BANKING_&{env};

-- -----------------------------------------------------------------------------
-- CORE_BANKING.CUSTOMERS
-- Master customer record from the core banking platform.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CORE_BANKING.CUSTOMERS
(
    CUSTOMER_ID         NUMBER(19,0)    NOT NULL,
    FIRST_NAME          VARCHAR(60),
    LAST_NAME           VARCHAR(60),
    DATE_OF_BIRTH       DATE,
    SSN_HASH            VARCHAR(64),
    EMAIL               VARCHAR(120),
    PHONE_PRIMARY       VARCHAR(20),
    CUSTOMER_SINCE      DATE,
    CUSTOMER_STATUS     VARCHAR(1),                 -- A=Active, I=Inactive, C=Closed
    SEGMENT_CODE        VARCHAR(10),
    BRANCH_ID           NUMBER(10,0),
    CREATED_TS          TIMESTAMP_NTZ(6),
    UPDATED_TS          TIMESTAMP_NTZ(6)
)
COMMENT = 'Master customer record (migrated from CORE_BANKING_DB.CUSTOMERS)';

-- -----------------------------------------------------------------------------
-- CORE_BANKING.ACCOUNTS
-- Account-level detail (checking, savings, credit, loan).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CORE_BANKING.ACCOUNTS
(
    ACCOUNT_ID          NUMBER(19,0)    NOT NULL,
    CUSTOMER_ID         NUMBER(19,0)    NOT NULL,
    ACCOUNT_TYPE        VARCHAR(20),                -- CHECKING, SAVINGS, CREDIT, LOAN
    ACCOUNT_STATUS      VARCHAR(1),                 -- O=Open, C=Closed, F=Frozen
    OPEN_DATE           DATE,
    CLOSE_DATE          DATE,
    CURRENT_BALANCE     NUMBER(15,2),
    AVAILABLE_BALANCE   NUMBER(15,2),
    CREDIT_LIMIT        NUMBER(15,2),
    INTEREST_RATE       NUMBER(5,4),
    BRANCH_ID           NUMBER(10,0),
    CREATED_TS          TIMESTAMP_NTZ(6),
    UPDATED_TS          TIMESTAMP_NTZ(6)
)
COMMENT = 'Account-level detail (migrated from CORE_BANKING_DB.ACCOUNTS)';

-- -----------------------------------------------------------------------------
-- CORE_BANKING.ADDRESSES
-- Customer mailing and residential addresses.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CORE_BANKING.ADDRESSES
(
    ADDRESS_ID          NUMBER(19,0)    NOT NULL,
    CUSTOMER_ID         NUMBER(19,0)    NOT NULL,
    ADDRESS_TYPE        VARCHAR(10),                -- MAIL, HOME, WORK
    ADDRESS_LINE_1      VARCHAR(100),
    ADDRESS_LINE_2      VARCHAR(100),
    CITY                VARCHAR(60),
    STATE_CODE          VARCHAR(2),
    ZIP_CODE            VARCHAR(10),
    COUNTRY_CODE        VARCHAR(2)      DEFAULT 'US',
    IS_PRIMARY          VARCHAR(1)      DEFAULT 'N',
    EFFECTIVE_DATE      DATE,
    EXPIRATION_DATE     DATE,
    CREATED_TS          TIMESTAMP_NTZ(6),
    UPDATED_TS          TIMESTAMP_NTZ(6)
)
COMMENT = 'Customer addresses (migrated from CORE_BANKING_DB.ADDRESSES)';

-- -----------------------------------------------------------------------------
-- TXN_PROCESSING.TRANSACTIONS
-- Individual financial transactions across all account types.
-- Teradata PARTITION BY RANGE_N(TRANSACTION_DATE ... EACH INTERVAL '1' MONTH)
-- is replaced by a Snowflake clustering key on TRANSACTION_DATE: every
-- downstream BTEQ/SAS read filters on a transaction date window.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS TXN_PROCESSING.TRANSACTIONS
(
    TRANSACTION_ID      NUMBER(19,0)    NOT NULL,
    ACCOUNT_ID          NUMBER(19,0)    NOT NULL,
    TRANSACTION_TYPE_CD VARCHAR(10),
    TRANSACTION_DATE    DATE,
    TRANSACTION_TS      TIMESTAMP_NTZ(6),
    AMOUNT              NUMBER(15,2),
    RUNNING_BALANCE     NUMBER(15,2),
    MERCHANT_NAME       VARCHAR(100),
    MERCHANT_CATEGORY   VARCHAR(60),
    CHANNEL_CODE        VARCHAR(10),                -- ATM, POS, WEB, MOB, ACH, WIRE
    REFERENCE_NUM       VARCHAR(40),
    STATUS_CODE         VARCHAR(1),                 -- P=Posted, R=Reversed, H=Hold
    CREATED_TS          TIMESTAMP_NTZ(6)
)
CLUSTER BY (TRANSACTION_DATE)
COMMENT = 'Financial transactions (migrated from TXN_PROCESSING_DB.TRANSACTIONS)';

-- -----------------------------------------------------------------------------
-- TXN_PROCESSING.TRANSACTION_TYPES
-- Reference/lookup table for transaction type codes.
-- The Teradata UNIQUE PRIMARY INDEX is expressed as an (unenforced) Snowflake
-- primary key so the uniqueness contract stays documented in the catalogue.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS TXN_PROCESSING.TRANSACTION_TYPES
(
    TRANSACTION_TYPE_CD VARCHAR(10)     NOT NULL,
    DESCRIPTION         VARCHAR(60),
    CATEGORY            VARCHAR(30),                -- DEBIT, CREDIT, FEE, INTEREST
    IS_REVENUE          VARCHAR(1)      DEFAULT 'N',
    EFFECTIVE_DATE      DATE,
    EXPIRATION_DATE     DATE,
    CONSTRAINT PK_TRANSACTION_TYPES PRIMARY KEY (TRANSACTION_TYPE_CD)
)
COMMENT = 'Transaction type reference data (migrated from TXN_PROCESSING_DB.TRANSACTION_TYPES)';
