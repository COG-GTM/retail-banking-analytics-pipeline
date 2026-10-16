/*
Purpose: Aggregate posted transactions into per-account summary metrics.
Legacy source: bteq/02_stg_txn_summary.bteq
Contract: AZSYN contract v1; output matches ddl/01_staging_tables.sql.
Notes on deviations: Equal merchant-category spend is resolved by category
name ascending; load and audit timestamps use UTC.
*/
CREATE OR ALTER PROCEDURE stg.usp_load_stg_txn_summary
    @AsOfDate DATE = NULL,
    @LookbackMonths INT = 12
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @PeriodStart DATE;
    DECLARE @PeriodEnd DATE;
    DECLARE @StartTs DATETIME2(6);
    DECLARE @EndTs DATETIME2(6);
    DECLARE @RowCount BIGINT;

    SET @AsOfDate = COALESCE(@AsOfDate, CAST(GETUTCDATE() AS DATE));
    SET @LookbackMonths = COALESCE(@LookbackMonths, 12);
    SET @StartTs = SYSUTCDATETIME();

    BEGIN TRY
        SET @PeriodStart = DATEADD(MONTH, -@LookbackMonths, @AsOfDate);
        SET @PeriodEnd = @AsOfDate;

        IF OBJECT_ID('stg.STG_TXN_SUMMARY') IS NOT NULL
            DROP TABLE stg.STG_TXN_SUMMARY;

        -- VT_RUN_PARAMS CROSS JOIN is replaced with the procedure date variables.
        CREATE TABLE stg.STG_TXN_SUMMARY
        WITH (DISTRIBUTION = HASH(CUSTOMER_ID), CLUSTERED COLUMNSTORE INDEX)
        AS
        SELECT
            ISNULL(CAST(acct.CUSTOMER_ID AS BIGINT), 0)  AS CUSTOMER_ID,
            ISNULL(CAST(acct.ACCOUNT_ID  AS BIGINT), 0)  AS ACCOUNT_ID,
            CAST(acct.ACCOUNT_TYPE AS VARCHAR(20))       AS ACCOUNT_TYPE,
            CAST(@PeriodStart AS DATE)                   AS SUMMARY_PERIOD_START,
            CAST(@PeriodEnd   AS DATE)                   AS SUMMARY_PERIOD_END,
            CAST(COUNT(*) AS INT)                        AS TXN_COUNT_TOTAL,
            CAST(SUM(CASE WHEN tt.CATEGORY = 'DEBIT'  THEN 1 ELSE 0 END) AS INT) AS TXN_COUNT_DEBIT,
            CAST(SUM(CASE WHEN tt.CATEGORY = 'CREDIT' THEN 1 ELSE 0 END) AS INT) AS TXN_COUNT_CREDIT,
            CAST(SUM(CASE WHEN tt.CATEGORY = 'FEE'    THEN 1 ELSE 0 END) AS INT) AS TXN_COUNT_FEE,
            CAST(SUM(CASE WHEN tt.CATEGORY = 'DEBIT'  THEN ABS(t.AMOUNT) ELSE 0 END) AS DECIMAL(18,2)) AS AMT_TOTAL_DEBIT,
            CAST(SUM(CASE WHEN tt.CATEGORY = 'CREDIT' THEN t.AMOUNT      ELSE 0 END) AS DECIMAL(18,2)) AS AMT_TOTAL_CREDIT,
            CAST(SUM(CASE WHEN tt.CATEGORY = 'FEE'    THEN ABS(t.AMOUNT) ELSE 0 END) AS DECIMAL(18,2)) AS AMT_TOTAL_FEES,
            CAST(AVG(CASE WHEN tt.CATEGORY = 'DEBIT'  THEN ABS(t.AMOUNT) ELSE NULL END) AS DECIMAL(15,2)) AS AMT_AVG_DEBIT,
            CAST(AVG(CASE WHEN tt.CATEGORY = 'CREDIT' THEN t.AMOUNT      ELSE NULL END) AS DECIMAL(15,2)) AS AMT_AVG_CREDIT,
            CAST(MAX(CASE WHEN tt.CATEGORY = 'DEBIT'  THEN ABS(t.AMOUNT) ELSE 0 END) AS DECIMAL(15,2)) AS AMT_MAX_SINGLE_DEBIT,
            CAST(MAX(CASE WHEN tt.CATEGORY = 'CREDIT' THEN t.AMOUNT      ELSE 0 END) AS DECIMAL(15,2)) AS AMT_MAX_SINGLE_CREDIT,
            CAST(COUNT(DISTINCT t.MERCHANT_NAME) AS INT) AS DISTINCT_MERCHANTS,
            CAST(MAX(top_cat.MERCHANT_CATEGORY) AS VARCHAR(60)) AS TOP_MERCHANT_CATEGORY,
            -- NULLIF replaces Teradata NULLIFZERO for the percentage denominator.
            CAST(SUM(CASE WHEN t.CHANNEL_CODE = 'ATM' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS PCT_ATM,
            CAST(SUM(CASE WHEN t.CHANNEL_CODE = 'POS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS PCT_POS,
            CAST(SUM(CASE WHEN t.CHANNEL_CODE = 'WEB' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS PCT_WEB,
            CAST(SUM(CASE WHEN t.CHANNEL_CODE IN ('MOB') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS PCT_MOBILE,
            -- DATEDIFF replaces Teradata CURRENT_DATE - date arithmetic.
            CAST(DATEDIFF(DAY, MAX(t.TRANSACTION_DATE), @AsOfDate) AS INT) AS DAYS_SINCE_LAST_TXN,
            CAST(SYSUTCDATETIME() AS DATETIME2(6))       AS LOAD_TS
        FROM txn.TRANSACTIONS t
        INNER JOIN core.ACCOUNTS acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
        INNER JOIN txn.TRANSACTION_TYPES tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
        -- QUALIFY is replaced by pre-aggregation and ROW_NUMBER with a deterministic tie-break.
        LEFT JOIN (
            SELECT ranked.ACCOUNT_ID, ranked.MERCHANT_CATEGORY
            FROM (
                SELECT cat.ACCOUNT_ID, cat.MERCHANT_CATEGORY,
                       ROW_NUMBER() OVER (PARTITION BY cat.ACCOUNT_ID
                                          ORDER BY cat.CATEGORY_SPEND DESC, cat.MERCHANT_CATEGORY ASC) AS RN
                FROM (
                    SELECT t2.ACCOUNT_ID, t2.MERCHANT_CATEGORY, SUM(ABS(t2.AMOUNT)) AS CATEGORY_SPEND
                    FROM txn.TRANSACTIONS t2
                    WHERE t2.TRANSACTION_DATE BETWEEN @PeriodStart AND @PeriodEnd
                      AND t2.STATUS_CODE = 'P'
                      AND t2.MERCHANT_CATEGORY IS NOT NULL
                    GROUP BY t2.ACCOUNT_ID, t2.MERCHANT_CATEGORY
                ) cat
            ) ranked
            WHERE ranked.RN = 1
        ) top_cat ON t.ACCOUNT_ID = top_cat.ACCOUNT_ID
        WHERE t.TRANSACTION_DATE BETWEEN @PeriodStart AND @PeriodEnd
          AND t.STATUS_CODE = 'P'
        GROUP BY acct.CUSTOMER_ID, acct.ACCOUNT_ID, acct.ACCOUNT_TYPE, top_cat.MERCHANT_CATEGORY;

        CREATE STATISTICS st_STG_TXN_SUMMARY_CUSTOMER_ID
            ON stg.STG_TXN_SUMMARY (CUSTOMER_ID);
        CREATE STATISTICS st_STG_TXN_SUMMARY_CUSTOMER_ID_ACCOUNT_ID
            ON stg.STG_TXN_SUMMARY (CUSTOMER_ID, ACCOUNT_ID);
        CREATE STATISTICS st_STG_TXN_SUMMARY_ACCOUNT_TYPE
            ON stg.STG_TXN_SUMMARY (ACCOUNT_TYPE);

        SELECT @RowCount = COUNT_BIG(*) FROM stg.STG_TXN_SUMMARY;
        IF @RowCount = 0
        BEGIN
            ;THROW 50099, 'STG_TXN_SUMMARY loaded 0 rows', 1;
        END;

        SET @EndTs = SYSUTCDATETIME();
        INSERT INTO stg.ETL_RUN_LOG
            (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
        VALUES
            ('02_stg_txn_summary', 'FULL_LOAD', 'SUCCESS', @RowCount, @StartTs, @EndTs, NULL);
    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg VARCHAR(4000);
        SET @ErrMsg = LEFT(ERROR_MESSAGE(), 4000);
        SET @EndTs = SYSUTCDATETIME();

        INSERT INTO stg.ETL_RUN_LOG
            (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
        VALUES
            ('02_stg_txn_summary', 'FULL_LOAD', 'FAILED', @RowCount, @StartTs, @EndTs, @ErrMsg);

        THROW;
    END CATCH;
END
