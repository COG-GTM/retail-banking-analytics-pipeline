-- =============================================================================
-- AZSYN-01: Load source tables (core.*, txn.*) from ADLS Gen2 with COPY INTO
-- =============================================================================
-- Replaces the upstream feeds that populated CORE_BANKING_DB / TXN_PROCESSING_DB.
-- Run after synapse/sql/ddl/00_schemas.sql and 01_source_tables.sql.
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
-- Re-runnable: with @Truncate = 1 each table is truncated before loading.
-- Each table load is logged to stg.ETL_RUN_LOG (JOB_NAME 'copy_into_source_tables').
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

BEGIN TRY
    -- core.CUSTOMERS <- customers.csv
    SET @step = 'core.CUSTOMERS';
    IF @Truncate = 1 TRUNCATE TABLE core.CUSTOMERS;
    SET @sql = N'COPY INTO core.CUSTOMERS
    (CUSTOMER_ID 1, FIRST_NAME 2, LAST_NAME 3, DATE_OF_BIRTH 4, SSN_HASH 5, EMAIL 6,
     PHONE_PRIMARY 7, CUSTOMER_SINCE 8, CUSTOMER_STATUS 9, SEGMENT_CODE 10, BRANCH_ID 11,
     CREATED_TS 12, UPDATED_TS 13)
    FROM ''' + @SourceRoot + N'/customers.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- core.ACCOUNTS <- accounts.csv
    SET @step = 'core.ACCOUNTS';
    IF @Truncate = 1 TRUNCATE TABLE core.ACCOUNTS;
    SET @sql = N'COPY INTO core.ACCOUNTS
    (ACCOUNT_ID 1, CUSTOMER_ID 2, ACCOUNT_TYPE 3, ACCOUNT_STATUS 4, OPEN_DATE 5, CLOSE_DATE 6,
     CURRENT_BALANCE 7, AVAILABLE_BALANCE 8, CREDIT_LIMIT 9, INTEREST_RATE 10, BRANCH_ID 11,
     CREATED_TS 12, UPDATED_TS 13)
    FROM ''' + @SourceRoot + N'/accounts.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- core.ADDRESSES <- addresses.csv
    SET @step = 'core.ADDRESSES';
    IF @Truncate = 1 TRUNCATE TABLE core.ADDRESSES;
    SET @sql = N'COPY INTO core.ADDRESSES
    (ADDRESS_ID 1, CUSTOMER_ID 2, ADDRESS_TYPE 3, ADDRESS_LINE_1 4, ADDRESS_LINE_2 5, CITY 6,
     STATE_CODE 7, ZIP_CODE 8, COUNTRY_CODE 9, IS_PRIMARY 10, EFFECTIVE_DATE 11,
     EXPIRATION_DATE 12, CREATED_TS 13, UPDATED_TS 14)
    FROM ''' + @SourceRoot + N'/addresses.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- core.CUSTOMER_BUREAU_SCORES <- customer_bureau_scores.csv
    SET @step = 'core.CUSTOMER_BUREAU_SCORES';
    IF @Truncate = 1 TRUNCATE TABLE core.CUSTOMER_BUREAU_SCORES;
    SET @sql = N'COPY INTO core.CUSTOMER_BUREAU_SCORES
    (CUSTOMER_ID 1, EXTERNAL_CREDIT_SCORE 2, REPORT_DATE 3)
    FROM ''' + @SourceRoot + N'/customer_bureau_scores.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- txn.TRANSACTIONS <- transactions.csv
    SET @step = 'txn.TRANSACTIONS';
    IF @Truncate = 1 TRUNCATE TABLE txn.TRANSACTIONS;
    SET @sql = N'COPY INTO txn.TRANSACTIONS
    (TRANSACTION_ID 1, ACCOUNT_ID 2, TRANSACTION_TYPE_CD 3, TRANSACTION_DATE 4, TRANSACTION_TS 5,
     AMOUNT 6, RUNNING_BALANCE 7, MERCHANT_NAME 8, MERCHANT_CATEGORY 9, CHANNEL_CODE 10,
     REFERENCE_NUM 11, STATUS_CODE 12, CREATED_TS 13)
    FROM ''' + @SourceRoot + N'/transactions.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- txn.TRANSACTION_TYPES <- transaction_types.csv
    SET @step = 'txn.TRANSACTION_TYPES';
    IF @Truncate = 1 TRUNCATE TABLE txn.TRANSACTION_TYPES;
    SET @sql = N'COPY INTO txn.TRANSACTION_TYPES
    (TRANSACTION_TYPE_CD 1, DESCRIPTION 2, CATEGORY 3, IS_REVENUE 4, EFFECTIVE_DATE 5,
     EXPIRATION_DATE 6)
    FROM ''' + @SourceRoot + N'/transaction_types.csv''
    WITH (' + @CopyOptions + N');';
    EXEC sp_executesql @sql;

    -- Audit: one SUCCESS row per table with the loaded row count.
    INSERT INTO stg.ETL_RUN_LOG (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
    SELECT 'copy_into_source_tables', t.STEP_NAME, 'SUCCESS', t.ROW_COUNT, @start_ts, SYSUTCDATETIME(), NULL
    FROM (
        SELECT 'core.CUSTOMERS' AS STEP_NAME, COUNT_BIG(*) AS ROW_COUNT FROM core.CUSTOMERS
        UNION ALL SELECT 'core.ACCOUNTS', COUNT_BIG(*) FROM core.ACCOUNTS
        UNION ALL SELECT 'core.ADDRESSES', COUNT_BIG(*) FROM core.ADDRESSES
        UNION ALL SELECT 'core.CUSTOMER_BUREAU_SCORES', COUNT_BIG(*) FROM core.CUSTOMER_BUREAU_SCORES
        UNION ALL SELECT 'txn.TRANSACTIONS', COUNT_BIG(*) FROM txn.TRANSACTIONS
        UNION ALL SELECT 'txn.TRANSACTION_TYPES', COUNT_BIG(*) FROM txn.TRANSACTION_TYPES
    ) AS t;
END TRY
BEGIN CATCH
    DECLARE @err VARCHAR(4000) = LEFT(ERROR_MESSAGE(), 4000);
    INSERT INTO stg.ETL_RUN_LOG (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
    VALUES ('copy_into_source_tables', @step, 'FAILED', NULL, @start_ts, SYSUTCDATETIME(), @err);
    THROW;
END CATCH;
