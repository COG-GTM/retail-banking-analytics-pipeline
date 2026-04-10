-- =============================================================================
-- Source Table DDL - Retail Banking Operational Systems
-- =============================================================================
-- These tables represent the upstream operational sources that feed the
-- analytics pipeline. They are owned by the core banking platform team.
-- This DDL is for DOCUMENTATION only; we do not create these tables.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- CORE_BANKING_DB.CUSTOMERS
-- Master customer record from the core banking platform.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE CORE_BANKING_DB.CUSTOMERS, NO FALLBACK
(
    CUSTOMER_ID         BIGINT          NOT NULL,
    FIRST_NAME          VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    LAST_NAME           VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    DATE_OF_BIRTH       DATE            FORMAT 'YYYY-MM-DD',
    SSN_HASH            CHAR(64)        CHARACTER SET LATIN NOT CASESPECIFIC,
    EMAIL               VARCHAR(120)    CHARACTER SET LATIN NOT CASESPECIFIC,
    PHONE_PRIMARY       VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    CUSTOMER_SINCE      DATE            FORMAT 'YYYY-MM-DD',
    CUSTOMER_STATUS     CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC,  -- A=Active, I=Inactive, C=Closed
    SEGMENT_CODE        VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    BRANCH_ID           INTEGER,
    CREATED_TS          TIMESTAMP(6),
    UPDATED_TS          TIMESTAMP(6)
)
PRIMARY INDEX (CUSTOMER_ID);

-- -----------------------------------------------------------------------------
-- CORE_BANKING_DB.ACCOUNTS
-- Account-level detail (checking, savings, credit, loan).
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE CORE_BANKING_DB.ACCOUNTS, NO FALLBACK
(
    ACCOUNT_ID          BIGINT          NOT NULL,
    CUSTOMER_ID         BIGINT          NOT NULL,
    ACCOUNT_TYPE        VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- CHECKING, SAVINGS, CREDIT, LOAN
    ACCOUNT_STATUS      CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC,  -- O=Open, C=Closed, F=Frozen
    OPEN_DATE           DATE            FORMAT 'YYYY-MM-DD',
    CLOSE_DATE          DATE            FORMAT 'YYYY-MM-DD',
    CURRENT_BALANCE     DECIMAL(15,2),
    AVAILABLE_BALANCE   DECIMAL(15,2),
    CREDIT_LIMIT        DECIMAL(15,2),
    INTEREST_RATE       DECIMAL(5,4),
    BRANCH_ID           INTEGER,
    CREATED_TS          TIMESTAMP(6),
    UPDATED_TS          TIMESTAMP(6)
)
PRIMARY INDEX (ACCOUNT_ID);

-- -----------------------------------------------------------------------------
-- CORE_BANKING_DB.ADDRESSES
-- Customer mailing and residential addresses.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE CORE_BANKING_DB.ADDRESSES, NO FALLBACK
(
    ADDRESS_ID          BIGINT          NOT NULL,
    CUSTOMER_ID         BIGINT          NOT NULL,
    ADDRESS_TYPE        VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- MAIL, HOME, WORK
    ADDRESS_LINE_1      VARCHAR(100)    CHARACTER SET LATIN NOT CASESPECIFIC,
    ADDRESS_LINE_2      VARCHAR(100)    CHARACTER SET LATIN NOT CASESPECIFIC,
    CITY                VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    STATE_CODE          CHAR(2)         CHARACTER SET LATIN NOT CASESPECIFIC,
    ZIP_CODE            VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    COUNTRY_CODE        CHAR(2)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'US',
    IS_PRIMARY          CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'N',
    EFFECTIVE_DATE      DATE            FORMAT 'YYYY-MM-DD',
    EXPIRATION_DATE     DATE            FORMAT 'YYYY-MM-DD',
    CREATED_TS          TIMESTAMP(6),
    UPDATED_TS          TIMESTAMP(6)
)
PRIMARY INDEX (ADDRESS_ID);

-- -----------------------------------------------------------------------------
-- TXN_PROCESSING_DB.TRANSACTIONS
-- Individual financial transactions across all account types.
-- Partitioned by TRANSACTION_DATE for performance.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE TXN_PROCESSING_DB.TRANSACTIONS, NO FALLBACK
(
    TRANSACTION_ID      BIGINT          NOT NULL,
    ACCOUNT_ID          BIGINT          NOT NULL,
    TRANSACTION_TYPE_CD VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    TRANSACTION_DATE    DATE            FORMAT 'YYYY-MM-DD',
    TRANSACTION_TS      TIMESTAMP(6),
    AMOUNT              DECIMAL(15,2),
    RUNNING_BALANCE     DECIMAL(15,2),
    MERCHANT_NAME       VARCHAR(100)    CHARACTER SET LATIN NOT CASESPECIFIC,
    MERCHANT_CATEGORY   VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    CHANNEL_CODE        VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- ATM, POS, WEB, MOB, ACH, WIRE
    REFERENCE_NUM       VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    STATUS_CODE         CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC,  -- P=Posted, R=Reversed, H=Hold
    CREATED_TS          TIMESTAMP(6)
)
PRIMARY INDEX (TRANSACTION_ID)
PARTITION BY RANGE_N(TRANSACTION_DATE BETWEEN DATE '2020-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' MONTH);

-- -----------------------------------------------------------------------------
-- TXN_PROCESSING_DB.TRANSACTION_TYPES
-- Reference/lookup table for transaction type codes.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE TXN_PROCESSING_DB.TRANSACTION_TYPES, NO FALLBACK
(
    TRANSACTION_TYPE_CD VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    DESCRIPTION         VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    CATEGORY            VARCHAR(30)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- DEBIT, CREDIT, FEE, INTEREST
    IS_REVENUE          CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'N',
    EFFECTIVE_DATE      DATE            FORMAT 'YYYY-MM-DD',
    EXPIRATION_DATE     DATE            FORMAT 'YYYY-MM-DD'
)
UNIQUE PRIMARY INDEX (TRANSACTION_TYPE_CD);
