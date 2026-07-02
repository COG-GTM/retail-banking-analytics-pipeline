-- =============================================================================
-- Source Table DDL - Retail Banking Operational Systems (Spark / Delta Lake)
-- =============================================================================
-- These tables represent the upstream operational sources that feed the
-- analytics pipeline. They are owned by the core banking platform team.
-- This DDL is for DOCUMENTATION only; we do not create these tables (the
-- bootstrap in spark/apply_ddl.py intentionally skips this file).
--
-- Migrated from Teradata DDL. Physical-storage directives have no Spark
-- equivalent and were dropped:
--   * MULTISET / NO FALLBACK            -> dropped (Teradata physical/replication)
--   * PRIMARY INDEX / UNIQUE PRIMARY IX -> dropped (Teradata data distribution)
--   * CHARACTER SET LATIN NOT CASESPECIFIC / FORMAT '...' -> dropped (Teradata)
--   * DEFAULT '...'                     -> dropped (handled in transform jobs)
-- Type mapping: INTEGER->INT, TIMESTAMP(6)->TIMESTAMP, DATE/DECIMAL/SMALLINT/
--   BIGINT unchanged, VARCHAR(n)/CHAR(n) preserved for documentation of intent.
--
-- Placeholders (${DB_CORE}, ${DB_TXN}, ...) are resolved from the environment
-- (config/pipeline_config.cfg) at apply time -- no hardcoded schema names.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ${DB_CORE}.CUSTOMERS
-- Master customer record from the core banking platform.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_CORE}.CUSTOMERS
(
    CUSTOMER_ID         BIGINT          NOT NULL,
    FIRST_NAME          VARCHAR(60),
    LAST_NAME           VARCHAR(60),
    DATE_OF_BIRTH       DATE,
    SSN_HASH            CHAR(64),
    EMAIL               VARCHAR(120),
    PHONE_PRIMARY       VARCHAR(20),
    CUSTOMER_SINCE      DATE,
    CUSTOMER_STATUS     CHAR(1),                                                 -- A=Active, I=Inactive, C=Closed
    SEGMENT_CODE        VARCHAR(10),
    BRANCH_ID           INT,
    CREATED_TS          TIMESTAMP,
    UPDATED_TS          TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- ${DB_CORE}.ACCOUNTS
-- Account-level detail (checking, savings, credit, loan).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_CORE}.ACCOUNTS
(
    ACCOUNT_ID          BIGINT          NOT NULL,
    CUSTOMER_ID         BIGINT          NOT NULL,
    ACCOUNT_TYPE        VARCHAR(20),                                            -- CHECKING, SAVINGS, CREDIT, LOAN
    ACCOUNT_STATUS      CHAR(1),                                                -- O=Open, C=Closed, F=Frozen
    OPEN_DATE           DATE,
    CLOSE_DATE          DATE,
    CURRENT_BALANCE     DECIMAL(15,2),
    AVAILABLE_BALANCE   DECIMAL(15,2),
    CREDIT_LIMIT        DECIMAL(15,2),
    INTEREST_RATE       DECIMAL(5,4),
    BRANCH_ID           INT,
    CREATED_TS          TIMESTAMP,
    UPDATED_TS          TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- ${DB_CORE}.ADDRESSES
-- Customer mailing and residential addresses.
-- Dropped Teradata column DEFAULTs: COUNTRY_CODE DEFAULT 'US', IS_PRIMARY 'N'.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_CORE}.ADDRESSES
(
    ADDRESS_ID          BIGINT          NOT NULL,
    CUSTOMER_ID         BIGINT          NOT NULL,
    ADDRESS_TYPE        VARCHAR(10),                                            -- MAIL, HOME, WORK
    ADDRESS_LINE_1      VARCHAR(100),
    ADDRESS_LINE_2      VARCHAR(100),
    CITY                VARCHAR(60),
    STATE_CODE          CHAR(2),
    ZIP_CODE            VARCHAR(10),
    COUNTRY_CODE        CHAR(2),
    IS_PRIMARY          CHAR(1),
    EFFECTIVE_DATE      DATE,
    EXPIRATION_DATE     DATE,
    CREATED_TS          TIMESTAMP,
    UPDATED_TS          TIMESTAMP
)
USING DELTA;

-- -----------------------------------------------------------------------------
-- ${DB_TXN}.TRANSACTIONS
-- Individual financial transactions across all account types.
-- Teradata: PARTITION BY RANGE_N(TRANSACTION_DATE ... EACH INTERVAL '1' MONTH).
-- Spark equivalent: PARTITIONED BY (TRANSACTION_DATE). Delta data-skipping
-- prunes date ranges; the monthly grain of the Teradata RANGE_N is preserved
-- as partitioning intent (a coarser month column can be derived in the load
-- job if month-level partition folders are preferred). Schema columns are kept
-- identical to the source; no extra column is introduced.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_TXN}.TRANSACTIONS
(
    TRANSACTION_ID      BIGINT          NOT NULL,
    ACCOUNT_ID          BIGINT          NOT NULL,
    TRANSACTION_TYPE_CD VARCHAR(10),
    TRANSACTION_TS      TIMESTAMP,
    AMOUNT              DECIMAL(15,2),
    RUNNING_BALANCE     DECIMAL(15,2),
    MERCHANT_NAME       VARCHAR(100),
    MERCHANT_CATEGORY   VARCHAR(60),
    CHANNEL_CODE        VARCHAR(10),                                            -- ATM, POS, WEB, MOB, ACH, WIRE
    REFERENCE_NUM       VARCHAR(40),
    STATUS_CODE         CHAR(1),                                                -- P=Posted, R=Reversed, H=Hold
    CREATED_TS          TIMESTAMP,
    TRANSACTION_DATE    DATE                                                    -- partition key (see note above)
)
USING DELTA
PARTITIONED BY (TRANSACTION_DATE);

-- -----------------------------------------------------------------------------
-- ${DB_TXN}.TRANSACTION_TYPES
-- Reference/lookup table for transaction type codes.
-- Teradata UNIQUE PRIMARY INDEX (TRANSACTION_TYPE_CD) dropped (no Spark equiv);
-- uniqueness is now enforced by the loading transform, not the storage layer.
-- Dropped Teradata column DEFAULT: IS_REVENUE DEFAULT 'N'.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${DB_TXN}.TRANSACTION_TYPES
(
    TRANSACTION_TYPE_CD VARCHAR(10)     NOT NULL,
    DESCRIPTION         VARCHAR(60),
    CATEGORY            VARCHAR(30),                                            -- DEBIT, CREDIT, FEE, INTEREST
    IS_REVENUE          CHAR(1),
    EFFECTIVE_DATE      DATE,
    EXPIRATION_DATE     DATE
)
USING DELTA;
