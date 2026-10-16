-- =============================================================================
-- AZSYN-01: Source tables (core.*, txn.*) - Synapse dedicated SQL pool
-- =============================================================================
-- Translated from ddl/00_source_tables.sql (Teradata) per AZSYN contract v1.
-- core.CUSTOMER_BUREAU_SCORES has no legacy DDL; its definition is frozen by the
-- contract (used by bteq/03_stg_risk_factors.bteq).
--
-- Mapping applied:
--   MULTISET TABLE, NO FALLBACK          -> dropped (Synapse tables allow duplicates)
--   PRIMARY INDEX (CUSTOMER_ID)          -> DISTRIBUTION = HASH(CUSTOMER_ID)
--   PRIMARY INDEX (TRANSACTION_ID)       -> DISTRIBUTION = HASH(ACCOUNT_ID) (contract)
--   UNIQUE PRIMARY INDEX (lookup)        -> DISTRIBUTION = REPLICATE + CLUSTERED INDEX
--   PARTITION BY RANGE_N(... EACH 1 MONTH) -> PARTITION (... RANGE RIGHT FOR VALUES monthly)
--   CHARACTER SET LATIN NOT CASESPECIFIC -> dropped (default pool collation is CI)
--   FORMAT 'YYYY-MM-DD'                  -> dropped
--   TIMESTAMP(6)                         -> DATETIME2(6)
--   DEFAULT 'x'                          -> column DEFAULT 'x'
-- Idempotent and non-destructive: each table is created only if it is missing.
-- Load data with synapse/sql/load/copy_into_source_tables.sql.
-- =============================================================================

-- core.CUSTOMERS  (was CORE_BANKING_DB.CUSTOMERS, PI CUSTOMER_ID)
IF OBJECT_ID(N'core.CUSTOMERS', N'U') IS NULL
CREATE TABLE core.CUSTOMERS
(
    CUSTOMER_ID         BIGINT          NOT NULL,
    FIRST_NAME          VARCHAR(60)     NULL,
    LAST_NAME           VARCHAR(60)     NULL,
    DATE_OF_BIRTH       DATE            NULL,
    SSN_HASH            CHAR(64)        NULL,
    EMAIL               VARCHAR(120)    NULL,
    PHONE_PRIMARY       VARCHAR(20)     NULL,
    CUSTOMER_SINCE      DATE            NULL,
    CUSTOMER_STATUS     CHAR(1)         NULL,   -- A=Active, I=Inactive, C=Closed
    SEGMENT_CODE        VARCHAR(10)     NULL,
    BRANCH_ID           INT             NULL,
    CREATED_TS          DATETIME2(6)    NULL,
    UPDATED_TS          DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- core.ACCOUNTS  (was CORE_BANKING_DB.ACCOUNTS, PI ACCOUNT_ID)
IF OBJECT_ID(N'core.ACCOUNTS', N'U') IS NULL
CREATE TABLE core.ACCOUNTS
(
    ACCOUNT_ID          BIGINT          NOT NULL,
    CUSTOMER_ID         BIGINT          NOT NULL,
    ACCOUNT_TYPE        VARCHAR(20)     NULL,   -- CHECKING, SAVINGS, CREDIT, LOAN
    ACCOUNT_STATUS      CHAR(1)         NULL,   -- O=Open, C=Closed, F=Frozen
    OPEN_DATE           DATE            NULL,
    CLOSE_DATE          DATE            NULL,
    CURRENT_BALANCE     DECIMAL(15,2)   NULL,
    AVAILABLE_BALANCE   DECIMAL(15,2)   NULL,
    CREDIT_LIMIT        DECIMAL(15,2)   NULL,
    INTEREST_RATE       DECIMAL(5,4)    NULL,
    BRANCH_ID           INT             NULL,
    CREATED_TS          DATETIME2(6)    NULL,
    UPDATED_TS          DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- core.ADDRESSES  (was CORE_BANKING_DB.ADDRESSES, PI ADDRESS_ID)
IF OBJECT_ID(N'core.ADDRESSES', N'U') IS NULL
CREATE TABLE core.ADDRESSES
(
    ADDRESS_ID          BIGINT          NOT NULL,
    CUSTOMER_ID         BIGINT          NOT NULL,
    ADDRESS_TYPE        VARCHAR(10)     NULL,   -- MAIL, HOME, WORK
    ADDRESS_LINE_1      VARCHAR(100)    NULL,
    ADDRESS_LINE_2      VARCHAR(100)    NULL,
    CITY                VARCHAR(60)     NULL,
    STATE_CODE          CHAR(2)         NULL,
    ZIP_CODE            VARCHAR(10)     NULL,
    COUNTRY_CODE        CHAR(2)         NULL DEFAULT 'US',
    IS_PRIMARY          CHAR(1)         NULL DEFAULT 'N',
    EFFECTIVE_DATE      DATE            NULL,
    EXPIRATION_DATE     DATE            NULL,
    CREATED_TS          DATETIME2(6)    NULL,
    UPDATED_TS          DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- core.CUSTOMER_BUREAU_SCORES  (no legacy DDL; frozen by AZSYN contract v1)
IF OBJECT_ID(N'core.CUSTOMER_BUREAU_SCORES', N'U') IS NULL
CREATE TABLE core.CUSTOMER_BUREAU_SCORES
(
    CUSTOMER_ID             BIGINT      NOT NULL,
    EXTERNAL_CREDIT_SCORE   SMALLINT    NULL,
    REPORT_DATE             DATE        NULL
)
WITH
(
    DISTRIBUTION = HASH(CUSTOMER_ID),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- txn.TRANSACTIONS  (was TXN_PROCESSING_DB.TRANSACTIONS, PI TRANSACTION_ID,
--   PARTITION BY RANGE_N(TRANSACTION_DATE BETWEEN 2020-01-01 AND 2030-12-31 EACH 1 MONTH))
-- RANGE RIGHT boundaries on the first of each month: partition N holds
-- [boundary N-1, boundary N). Rows before 2020-01-01 land in the first partition,
-- rows on/after 2030-12-01 in the last one (Teradata had no NO RANGE partition and
-- would reject them).
IF OBJECT_ID(N'txn.TRANSACTIONS', N'U') IS NULL
CREATE TABLE txn.TRANSACTIONS
(
    TRANSACTION_ID      BIGINT          NOT NULL,
    ACCOUNT_ID          BIGINT          NOT NULL,
    TRANSACTION_TYPE_CD VARCHAR(10)     NULL,
    TRANSACTION_DATE    DATE            NULL,
    TRANSACTION_TS      DATETIME2(6)    NULL,
    AMOUNT              DECIMAL(15,2)   NULL,
    RUNNING_BALANCE     DECIMAL(15,2)   NULL,
    MERCHANT_NAME       VARCHAR(100)    NULL,
    MERCHANT_CATEGORY   VARCHAR(60)     NULL,
    CHANNEL_CODE        VARCHAR(10)     NULL,   -- ATM, POS, WEB, MOB, ACH, WIRE
    REFERENCE_NUM       VARCHAR(40)     NULL,
    STATUS_CODE         CHAR(1)         NULL,   -- P=Posted, R=Reversed, H=Hold
    CREATED_TS          DATETIME2(6)    NULL
)
WITH
(
    DISTRIBUTION = HASH(ACCOUNT_ID),
    CLUSTERED COLUMNSTORE INDEX,
    PARTITION
    (
        TRANSACTION_DATE RANGE RIGHT FOR VALUES
        (
            '2020-01-01', '2020-02-01', '2020-03-01', '2020-04-01', '2020-05-01', '2020-06-01',
            '2020-07-01', '2020-08-01', '2020-09-01', '2020-10-01', '2020-11-01', '2020-12-01',
            '2021-01-01', '2021-02-01', '2021-03-01', '2021-04-01', '2021-05-01', '2021-06-01',
            '2021-07-01', '2021-08-01', '2021-09-01', '2021-10-01', '2021-11-01', '2021-12-01',
            '2022-01-01', '2022-02-01', '2022-03-01', '2022-04-01', '2022-05-01', '2022-06-01',
            '2022-07-01', '2022-08-01', '2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01',
            '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01',
            '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
            '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01',
            '2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
            '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01', '2025-05-01', '2025-06-01',
            '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01',
            '2026-01-01', '2026-02-01', '2026-03-01', '2026-04-01', '2026-05-01', '2026-06-01',
            '2026-07-01', '2026-08-01', '2026-09-01', '2026-10-01', '2026-11-01', '2026-12-01',
            '2027-01-01', '2027-02-01', '2027-03-01', '2027-04-01', '2027-05-01', '2027-06-01',
            '2027-07-01', '2027-08-01', '2027-09-01', '2027-10-01', '2027-11-01', '2027-12-01',
            '2028-01-01', '2028-02-01', '2028-03-01', '2028-04-01', '2028-05-01', '2028-06-01',
            '2028-07-01', '2028-08-01', '2028-09-01', '2028-10-01', '2028-11-01', '2028-12-01',
            '2029-01-01', '2029-02-01', '2029-03-01', '2029-04-01', '2029-05-01', '2029-06-01',
            '2029-07-01', '2029-08-01', '2029-09-01', '2029-10-01', '2029-11-01', '2029-12-01',
            '2030-01-01', '2030-02-01', '2030-03-01', '2030-04-01', '2030-05-01', '2030-06-01',
            '2030-07-01', '2030-08-01', '2030-09-01', '2030-10-01', '2030-11-01', '2030-12-01'
        )
    )
);
GO

-- txn.TRANSACTION_TYPES  (was TXN_PROCESSING_DB.TRANSACTION_TYPES, UPI TRANSACTION_TYPE_CD)
-- 8-row lookup: replicated to every compute node; rowstore clustered index
-- because CCI is wasteful for tiny tables. Uniqueness is not enforced.
IF OBJECT_ID(N'txn.TRANSACTION_TYPES', N'U') IS NULL
CREATE TABLE txn.TRANSACTION_TYPES
(
    TRANSACTION_TYPE_CD VARCHAR(10)     NOT NULL,
    DESCRIPTION         VARCHAR(60)     NULL,
    CATEGORY            VARCHAR(30)     NULL,   -- DEBIT, CREDIT, FEE, INTEREST
    IS_REVENUE          CHAR(1)         NULL DEFAULT 'N',
    EFFECTIVE_DATE      DATE            NULL,
    EXPIRATION_DATE     DATE            NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED INDEX (TRANSACTION_TYPE_CD)
);
GO
