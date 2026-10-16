-- =============================================================================
-- AZSYN-01: Load source tables (core.*, txn.*) from ADLS Gen2 with COPY INTO
-- =============================================================================
-- Replaces the upstream feeds that populated CORE_BANKING_DB / TXN_PROCESSING_DB.
-- Run after synapse/sql/ddl/00_schemas.sql, 01_source_tables.sql and 02_staging_tables.sql
-- (stg.ETL_RUN_LOG); the script stops before touching any table if the log table is missing.
--
-- ADLS layout mirrors the repo (only the root is configurable):
--   https://<StorageAccount>.dfs.core.windows.net/<Container>/<RootFolder>/01_source_tables/<table>.csv
--   e.g. abfss://raw@stretailbanking.dfs.core.windows.net/data/01_source_tables/customers.csv
--
-- CSV format (data/01_source_tables/*.csv):
--   UTF-8, comma-separated, LF line endings, one header row, '"' quoting
--   (e.g. "Macdonald, Willis and Silva"). Empty field = NULL.
--   DATE      -> yyyy-mm-dd                    (DATEFORMAT = 'ymd')
--   DATETIME2 -> yyyy-mm-dd hh:mi:ss[.ffffff]  (CREATED_TS has 6 fractional digits,
--                                               TRANSACTION_TS has none)
--
-- COPY INTO needs a literal FROM path, so each statement is built with dynamic SQL.
-- Every COPY INTO maps CSV fields by explicit position (column list "<COL> <n>"),
-- which pins the header order instead of relying on table column order.
--
-- Auth: the workspace managed identity needs "Storage Blob Data Reader" on the
-- container. For SAS auth replace @Credential with
--   N'CREDENTIAL = (IDENTITY = ''Shared Access Signature'', SECRET = ''<sas>'')'.
--
-- All-or-nothing: every file is first COPY'd into an empty ROUND_ROBIN heap load table
-- (stg.LOAD_<TABLE>). Only after all six loads succeed are the published tables replaced
-- (@Truncate = 1) or appended to (@Truncate = 0) in one transaction. Any failure rolls back,
-- so the published core.*/txn.* tables keep their previous contents. DELETE is used instead
-- of TRUNCATE inside the transaction. Load tables are dropped at the end of every run.
-- Each table is logged to stg.ETL_RUN_LOG (JOB_NAME 'copy_into_source_tables', STEP_NAME = table,
-- ROW_COUNT = rows loaded from the file); a failure logs one FAILED row and re-throws.
-- =============================================================================

-- ---- Parameters (override per environment / from the pipeline Script activity) ----
DECLARE @StorageAccount NVARCHAR(128) = N'<storage_account>';
DECLARE @Container      NVARCHAR(128) = N'<container>';
DECLARE @RootFolder     NVARCHAR(400) = N'data';
DECLARE @Credential     NVARCHAR(1000) = N'CREDENTIAL = (IDENTITY = ''Managed Identity'')';
DECLARE @Truncate       BIT = 1;

-- ---- Derived ----
DECLARE @SourceRoot NVARCHAR(1000) = N'https://' + @StorageAccount + N'.dfs.core.windows.net/'
                                   + @Container + N'/' + @RootFolder + N'/01_source_tables';
DECLARE @CopyOptions NVARCHAR(4000) = N'
        FILE_TYPE       = ''CSV'',
        FIRSTROW        = 2,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR   = ''0x0A'',
        FIELDQUOTE      = ''"'',
        ENCODING        = ''UTF8'',
        DATEFORMAT      = ''ymd'',
        MAXERRORS       = 0,
        ' + @Credential;
DECLARE @sql       NVARCHAR(MAX);
DECLARE @step      VARCHAR(100);
DECLARE @start_ts  DATETIME2(6) = SYSUTCDATETIME();

IF OBJECT_ID(N'stg.ETL_RUN_LOG', N'U') IS NULL
    THROW 50001, N'stg.ETL_RUN_LOG is missing: run synapse/sql/ddl/02_staging_tables.sql first', 1;

BEGIN TRY
    -- 1. Load every file into an empty load table (published tables untouched).

    -- core.CUSTOMERS <- customers.csv
    SET @step = 'core.CUSTOMERS';
    IF OBJECT_ID(N'stg.LOAD_CUSTOMERS', N'U') IS NOT NULL DROP TABLE stg.LOAD_CUSTOMERS;
    CREATE TABLE stg.LOAD_CUSTOMERS WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS SELECT * FROM core.CUSTOMERS WHERE 1 = 0;
    SET @sql = N'COPY INTO stg.LOAD_CUSTOMERS
    (CUSTOMER_ID 1, FIRST_NAME 2, LAST_NAME 3, DATE_OF_BIRTH 4, SSN_HASH 5, EMAIL 6,
     PHONE_PRIMARY 7, CUSTOMER_SINCE 8, CUSTOMER_STATUS 9, SEGMENT_CODE 10, BRANCH_ID 11,
     CREATED_TS 12, UPDATED_TS 13)
    FROM ''' + @SourceRoot + N'/customers.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- core.ACCOUNTS <- accounts.csv
    SET @step = 'core.ACCOUNTS';
    IF OBJECT_ID(N'stg.LOAD_ACCOUNTS', N'U') IS NOT NULL DROP TABLE stg.LOAD_ACCOUNTS;
    CREATE TABLE stg.LOAD_ACCOUNTS WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS SELECT * FROM core.ACCOUNTS WHERE 1 = 0;
    SET @sql = N'COPY INTO stg.LOAD_ACCOUNTS
    (ACCOUNT_ID 1, CUSTOMER_ID 2, ACCOUNT_TYPE 3, ACCOUNT_STATUS 4, OPEN_DATE 5, CLOSE_DATE 6,
     CURRENT_BALANCE 7, AVAILABLE_BALANCE 8, CREDIT_LIMIT 9, INTEREST_RATE 10, BRANCH_ID 11,
     CREATED_TS 12, UPDATED_TS 13)
    FROM ''' + @SourceRoot + N'/accounts.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- core.ADDRESSES <- addresses.csv
    SET @step = 'core.ADDRESSES';
    IF OBJECT_ID(N'stg.LOAD_ADDRESSES', N'U') IS NOT NULL DROP TABLE stg.LOAD_ADDRESSES;
    CREATE TABLE stg.LOAD_ADDRESSES WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS SELECT * FROM core.ADDRESSES WHERE 1 = 0;
    SET @sql = N'COPY INTO stg.LOAD_ADDRESSES
    (ADDRESS_ID 1, CUSTOMER_ID 2, ADDRESS_TYPE 3, ADDRESS_LINE_1 4, ADDRESS_LINE_2 5, CITY 6,
     STATE_CODE 7, ZIP_CODE 8, COUNTRY_CODE 9, IS_PRIMARY 10, EFFECTIVE_DATE 11,
     EXPIRATION_DATE 12, CREATED_TS 13, UPDATED_TS 14)
    FROM ''' + @SourceRoot + N'/addresses.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- core.CUSTOMER_BUREAU_SCORES <- customer_bureau_scores.csv
    SET @step = 'core.CUSTOMER_BUREAU_SCORES';
    IF OBJECT_ID(N'stg.LOAD_CUSTOMER_BUREAU_SCORES', N'U') IS NOT NULL DROP TABLE stg.LOAD_CUSTOMER_BUREAU_SCORES;
    CREATE TABLE stg.LOAD_CUSTOMER_BUREAU_SCORES WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS SELECT * FROM core.CUSTOMER_BUREAU_SCORES WHERE 1 = 0;
    SET @sql = N'COPY INTO stg.LOAD_CUSTOMER_BUREAU_SCORES
    (CUSTOMER_ID 1, EXTERNAL_CREDIT_SCORE 2, REPORT_DATE 3)
    FROM ''' + @SourceRoot + N'/customer_bureau_scores.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- txn.TRANSACTIONS <- transactions.csv
    SET @step = 'txn.TRANSACTIONS';
    IF OBJECT_ID(N'stg.LOAD_TRANSACTIONS', N'U') IS NOT NULL DROP TABLE stg.LOAD_TRANSACTIONS;
    CREATE TABLE stg.LOAD_TRANSACTIONS WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS SELECT * FROM txn.TRANSACTIONS WHERE 1 = 0;
    SET @sql = N'COPY INTO stg.LOAD_TRANSACTIONS
    (TRANSACTION_ID 1, ACCOUNT_ID 2, TRANSACTION_TYPE_CD 3, TRANSACTION_DATE 4, TRANSACTION_TS 5,
     AMOUNT 6, RUNNING_BALANCE 7, MERCHANT_NAME 8, MERCHANT_CATEGORY 9, CHANNEL_CODE 10,
     REFERENCE_NUM 11, STATUS_CODE 12, CREATED_TS 13)
    FROM ''' + @SourceRoot + N'/transactions.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- txn.TRANSACTION_TYPES <- transaction_types.csv
    SET @step = 'txn.TRANSACTION_TYPES';
    IF OBJECT_ID(N'stg.LOAD_TRANSACTION_TYPES', N'U') IS NOT NULL DROP TABLE stg.LOAD_TRANSACTION_TYPES;
    CREATE TABLE stg.LOAD_TRANSACTION_TYPES WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS SELECT * FROM txn.TRANSACTION_TYPES WHERE 1 = 0;
    SET @sql = N'COPY INTO stg.LOAD_TRANSACTION_TYPES
    (TRANSACTION_TYPE_CD 1, DESCRIPTION 2, CATEGORY 3, IS_REVENUE 4, EFFECTIVE_DATE 5,
     EXPIRATION_DATE 6)
    FROM ''' + @SourceRoot + N'/transaction_types.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- 2. Publish all six tables atomically.
    SET @step = 'PUBLISH';
    BEGIN TRANSACTION;
    IF @Truncate = 1 DELETE FROM core.CUSTOMERS;
    IF @Truncate = 1 DELETE FROM core.ACCOUNTS;
    IF @Truncate = 1 DELETE FROM core.ADDRESSES;
    IF @Truncate = 1 DELETE FROM core.CUSTOMER_BUREAU_SCORES;
    IF @Truncate = 1 DELETE FROM txn.TRANSACTIONS;
    IF @Truncate = 1 DELETE FROM txn.TRANSACTION_TYPES;
    INSERT INTO core.CUSTOMERS
        (CUSTOMER_ID, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, SSN_HASH, EMAIL, PHONE_PRIMARY,
         CUSTOMER_SINCE, CUSTOMER_STATUS, SEGMENT_CODE, BRANCH_ID, CREATED_TS, UPDATED_TS)
    SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, SSN_HASH, EMAIL, PHONE_PRIMARY,
           CUSTOMER_SINCE, CUSTOMER_STATUS, SEGMENT_CODE, BRANCH_ID, CREATED_TS, UPDATED_TS
    FROM stg.LOAD_CUSTOMERS;
    INSERT INTO core.ACCOUNTS
        (ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, ACCOUNT_STATUS, OPEN_DATE, CLOSE_DATE,
         CURRENT_BALANCE, AVAILABLE_BALANCE, CREDIT_LIMIT, INTEREST_RATE, BRANCH_ID, CREATED_TS,
         UPDATED_TS)
    SELECT ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, ACCOUNT_STATUS, OPEN_DATE, CLOSE_DATE,
           CURRENT_BALANCE, AVAILABLE_BALANCE, CREDIT_LIMIT, INTEREST_RATE, BRANCH_ID, CREATED_TS,
           UPDATED_TS
    FROM stg.LOAD_ACCOUNTS;
    INSERT INTO core.ADDRESSES
        (ADDRESS_ID, CUSTOMER_ID, ADDRESS_TYPE, ADDRESS_LINE_1, ADDRESS_LINE_2, CITY,
         STATE_CODE, ZIP_CODE, COUNTRY_CODE, IS_PRIMARY, EFFECTIVE_DATE, EXPIRATION_DATE,
         CREATED_TS, UPDATED_TS)
    SELECT ADDRESS_ID, CUSTOMER_ID, ADDRESS_TYPE, ADDRESS_LINE_1, ADDRESS_LINE_2, CITY,
           STATE_CODE, ZIP_CODE, COUNTRY_CODE, IS_PRIMARY, EFFECTIVE_DATE, EXPIRATION_DATE,
           CREATED_TS, UPDATED_TS
    FROM stg.LOAD_ADDRESSES;
    INSERT INTO core.CUSTOMER_BUREAU_SCORES
        (CUSTOMER_ID, EXTERNAL_CREDIT_SCORE, REPORT_DATE)
    SELECT CUSTOMER_ID, EXTERNAL_CREDIT_SCORE, REPORT_DATE
    FROM stg.LOAD_CUSTOMER_BUREAU_SCORES;
    INSERT INTO txn.TRANSACTIONS
        (TRANSACTION_ID, ACCOUNT_ID, TRANSACTION_TYPE_CD, TRANSACTION_DATE, TRANSACTION_TS,
         AMOUNT, RUNNING_BALANCE, MERCHANT_NAME, MERCHANT_CATEGORY, CHANNEL_CODE, REFERENCE_NUM,
         STATUS_CODE, CREATED_TS)
    SELECT TRANSACTION_ID, ACCOUNT_ID, TRANSACTION_TYPE_CD, TRANSACTION_DATE, TRANSACTION_TS,
           AMOUNT, RUNNING_BALANCE, MERCHANT_NAME, MERCHANT_CATEGORY, CHANNEL_CODE, REFERENCE_NUM,
           STATUS_CODE, CREATED_TS
    FROM stg.LOAD_TRANSACTIONS;
    INSERT INTO txn.TRANSACTION_TYPES
        (TRANSACTION_TYPE_CD, DESCRIPTION, CATEGORY, IS_REVENUE, EFFECTIVE_DATE,
         EXPIRATION_DATE)
    SELECT TRANSACTION_TYPE_CD, DESCRIPTION, CATEGORY, IS_REVENUE, EFFECTIVE_DATE,
           EXPIRATION_DATE
    FROM stg.LOAD_TRANSACTION_TYPES;
    COMMIT TRANSACTION;

    -- 3. Audit: one SUCCESS row per table with the rows loaded from its file.
    INSERT INTO stg.ETL_RUN_LOG (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
    SELECT 'copy_into_source_tables', t.STEP_NAME, 'SUCCESS', t.ROW_COUNT, @start_ts, SYSUTCDATETIME(), NULL
    FROM (
        SELECT 'core.CUSTOMERS' AS STEP_NAME, COUNT_BIG(*) AS ROW_COUNT FROM stg.LOAD_CUSTOMERS
        UNION ALL SELECT 'core.ACCOUNTS', COUNT_BIG(*) FROM stg.LOAD_ACCOUNTS
        UNION ALL SELECT 'core.ADDRESSES', COUNT_BIG(*) FROM stg.LOAD_ADDRESSES
        UNION ALL SELECT 'core.CUSTOMER_BUREAU_SCORES', COUNT_BIG(*) FROM stg.LOAD_CUSTOMER_BUREAU_SCORES
        UNION ALL SELECT 'txn.TRANSACTIONS', COUNT_BIG(*) FROM stg.LOAD_TRANSACTIONS
        UNION ALL SELECT 'txn.TRANSACTION_TYPES', COUNT_BIG(*) FROM stg.LOAD_TRANSACTION_TYPES
    ) AS t;

    IF OBJECT_ID(N'stg.LOAD_CUSTOMERS', N'U') IS NOT NULL DROP TABLE stg.LOAD_CUSTOMERS;
    IF OBJECT_ID(N'stg.LOAD_ACCOUNTS', N'U') IS NOT NULL DROP TABLE stg.LOAD_ACCOUNTS;
    IF OBJECT_ID(N'stg.LOAD_ADDRESSES', N'U') IS NOT NULL DROP TABLE stg.LOAD_ADDRESSES;
    IF OBJECT_ID(N'stg.LOAD_CUSTOMER_BUREAU_SCORES', N'U') IS NOT NULL DROP TABLE stg.LOAD_CUSTOMER_BUREAU_SCORES;
    IF OBJECT_ID(N'stg.LOAD_TRANSACTIONS', N'U') IS NOT NULL DROP TABLE stg.LOAD_TRANSACTIONS;
    IF OBJECT_ID(N'stg.LOAD_TRANSACTION_TYPES', N'U') IS NOT NULL DROP TABLE stg.LOAD_TRANSACTION_TYPES;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    DECLARE @err VARCHAR(4000) = LEFT(ERROR_MESSAGE(), 4000);
    IF OBJECT_ID(N'stg.LOAD_CUSTOMERS', N'U') IS NOT NULL DROP TABLE stg.LOAD_CUSTOMERS;
    IF OBJECT_ID(N'stg.LOAD_ACCOUNTS', N'U') IS NOT NULL DROP TABLE stg.LOAD_ACCOUNTS;
    IF OBJECT_ID(N'stg.LOAD_ADDRESSES', N'U') IS NOT NULL DROP TABLE stg.LOAD_ADDRESSES;
    IF OBJECT_ID(N'stg.LOAD_CUSTOMER_BUREAU_SCORES', N'U') IS NOT NULL DROP TABLE stg.LOAD_CUSTOMER_BUREAU_SCORES;
    IF OBJECT_ID(N'stg.LOAD_TRANSACTIONS', N'U') IS NOT NULL DROP TABLE stg.LOAD_TRANSACTIONS;
    IF OBJECT_ID(N'stg.LOAD_TRANSACTION_TYPES', N'U') IS NOT NULL DROP TABLE stg.LOAD_TRANSACTION_TYPES;
    INSERT INTO stg.ETL_RUN_LOG (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
    VALUES ('copy_into_source_tables', @step, 'FAILED', NULL, @start_ts, SYSUTCDATETIME(), @err);
    THROW;
END CATCH;
