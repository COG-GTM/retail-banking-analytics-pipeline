-- =============================================================================
-- Procedure: stg.usp_load_stg_customer_360
-- Legacy:    bteq/01_stg_customer_360.bteq (Teradata BTEQ)
-- Target:    stg.STG_CUSTOMER_360 (Azure Synapse dedicated SQL pool)
-- Sources:   core.CUSTOMERS, core.ACCOUNTS, core.ADDRESSES
-- Pattern:   AZSYN contract v1 staging proc (drop -> CTAS -> stats -> guard -> log)
-- =============================================================================
CREATE OR ALTER PROCEDURE stg.usp_load_stg_customer_360
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @AsOfDate = COALESCE(@AsOfDate, CAST(GETUTCDATE() AS DATE));

    DECLARE @StartTs  DATETIME2(6) = SYSUTCDATETIME();
    DECLARE @LoadTs   DATETIME2(6) = SYSUTCDATETIME();
    DECLARE @RowCount BIGINT;
    DECLARE @ErrMsg   VARCHAR(4000);

    BEGIN TRY
        IF OBJECT_ID('stg.STG_CUSTOMER_360') IS NOT NULL
            DROP TABLE stg.STG_CUSTOMER_360;

        CREATE TABLE stg.STG_CUSTOMER_360
        WITH (DISTRIBUTION = HASH(CUSTOMER_ID), CLUSTERED COLUMNSTORE INDEX)
        AS
        SELECT
            ISNULL(CAST(c.CUSTOMER_ID AS BIGINT), 0)                        AS CUSTOMER_ID,
            CAST(c.FIRST_NAME AS VARCHAR(60))                               AS FIRST_NAME,
            CAST(c.LAST_NAME AS VARCHAR(60))                                AS LAST_NAME,
            CAST(c.DATE_OF_BIRTH AS DATE)                                   AS DATE_OF_BIRTH,
            -- Teradata: CAST((CURRENT_DATE - DOB) / 365.25 AS SMALLINT).
            -- INTEGER / DECIMAL(5,2) yields a scale-2 decimal (rounded), then CAST truncates.
            CAST(ROUND(CAST(DATEDIFF(DAY, c.DATE_OF_BIRTH, @AsOfDate) AS DECIMAL(18,0)) / 365.25, 2)
                 AS SMALLINT)                                               AS AGE,
            CAST(c.CUSTOMER_SINCE AS DATE)                                  AS CUSTOMER_SINCE,
            -- Teradata: CAST(MONTHS_BETWEEN(CURRENT_DATE, CUSTOMER_SINCE) AS INTEGER).
            -- Whole months when day-of-month matches or both dates are month-end,
            -- otherwise whole months + (day1 - day2) / 31; CAST truncates toward zero.
            CAST(
                CASE
                    WHEN DAY(@AsOfDate) = DAY(c.CUSTOMER_SINCE)
                      OR (@AsOfDate = EOMONTH(@AsOfDate) AND c.CUSTOMER_SINCE = EOMONTH(c.CUSTOMER_SINCE))
                    THEN CAST((YEAR(@AsOfDate) - YEAR(c.CUSTOMER_SINCE)) * 12
                              + (MONTH(@AsOfDate) - MONTH(c.CUSTOMER_SINCE)) AS DECIMAL(18,6))
                    ELSE CAST((YEAR(@AsOfDate) - YEAR(c.CUSTOMER_SINCE)) * 12
                              + (MONTH(@AsOfDate) - MONTH(c.CUSTOMER_SINCE)) AS DECIMAL(18,6))
                         + CAST(DAY(@AsOfDate) - DAY(c.CUSTOMER_SINCE) AS DECIMAL(18,6)) / 31
                END
            AS INT)                                                         AS TENURE_MONTHS,
            CAST(c.CUSTOMER_STATUS AS CHAR(1))                              AS CUSTOMER_STATUS,
            CAST(c.SEGMENT_CODE AS VARCHAR(10))                             AS SEGMENT_CODE,
            CAST(c.BRANCH_ID AS INT)                                        AS BRANCH_ID,
            CAST(LTRIM(RTRIM(a.ADDRESS_LINE_1))
                 + COALESCE(', ' + LTRIM(RTRIM(a.ADDRESS_LINE_2)), '')
                 AS VARCHAR(200))                                           AS PRIMARY_ADDRESS,
            CAST(a.CITY AS VARCHAR(60))                                     AS CITY,
            CAST(a.STATE_CODE AS CHAR(2))                                   AS STATE_CODE,
            CAST(a.ZIP_CODE AS VARCHAR(10))                                 AS ZIP_CODE,
            CAST(acct_agg.NUM_ACCOUNTS AS SMALLINT)                         AS NUM_ACCOUNTS,
            CAST(acct_agg.NUM_ACTIVE_ACCOUNTS AS SMALLINT)                  AS NUM_ACTIVE_ACCOUNTS,
            CAST(acct_agg.HAS_CHECKING AS CHAR(1))                          AS HAS_CHECKING,
            CAST(acct_agg.HAS_SAVINGS AS CHAR(1))                           AS HAS_SAVINGS,
            CAST(acct_agg.HAS_CREDIT AS CHAR(1))                            AS HAS_CREDIT,
            CAST(acct_agg.HAS_LOAN AS CHAR(1))                              AS HAS_LOAN,
            CAST(acct_agg.TOTAL_BALANCE AS DECIMAL(18,2))                   AS TOTAL_BALANCE,
            CAST(acct_agg.TOTAL_CREDIT_LIMIT AS DECIMAL(18,2))              AS TOTAL_CREDIT_LIMIT,
            CAST(
                CASE
                    WHEN acct_agg.TOTAL_CREDIT_LIMIT > 0
                    THEN CAST(acct_agg.CREDIT_BALANCE AS DECIMAL(38,10))
                         / acct_agg.TOTAL_CREDIT_LIMIT * 100
                    ELSE 0.00
                END
            AS DECIMAL(5,2))                                                AS CREDIT_UTILIZATION_PCT,
            @LoadTs                                                         AS LOAD_TS
        FROM core.CUSTOMERS c
        -- Primary address: most recent current HOME address (Teradata QUALIFY ROW_NUMBER() = 1)
        LEFT JOIN (
            SELECT CUSTOMER_ID, ADDRESS_LINE_1, ADDRESS_LINE_2, CITY, STATE_CODE, ZIP_CODE
            FROM (
                SELECT
                    CUSTOMER_ID, ADDRESS_LINE_1, ADDRESS_LINE_2, CITY, STATE_CODE, ZIP_CODE,
                    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY EFFECTIVE_DATE DESC) AS RN
                FROM core.ADDRESSES
                WHERE ADDRESS_TYPE = 'HOME'
                  AND (EXPIRATION_DATE IS NULL OR EXPIRATION_DATE > @AsOfDate)
            ) ranked
            WHERE RN = 1
        ) a
            ON c.CUSTOMER_ID = a.CUSTOMER_ID
        -- Account portfolio roll-up
        LEFT JOIN (
            SELECT
                CUSTOMER_ID,
                COUNT(*)                                                         AS NUM_ACCOUNTS,
                SUM(CASE WHEN ACCOUNT_STATUS = 'O' THEN 1 ELSE 0 END)          AS NUM_ACTIVE_ACCOUNTS,
                MAX(CASE WHEN ACCOUNT_TYPE = 'CHECKING' THEN 'Y' ELSE 'N' END) AS HAS_CHECKING,
                MAX(CASE WHEN ACCOUNT_TYPE = 'SAVINGS'  THEN 'Y' ELSE 'N' END) AS HAS_SAVINGS,
                MAX(CASE WHEN ACCOUNT_TYPE = 'CREDIT'   THEN 'Y' ELSE 'N' END) AS HAS_CREDIT,
                MAX(CASE WHEN ACCOUNT_TYPE = 'LOAN'     THEN 'Y' ELSE 'N' END) AS HAS_LOAN,
                SUM(COALESCE(CURRENT_BALANCE, 0))                                AS TOTAL_BALANCE,
                SUM(CASE WHEN ACCOUNT_TYPE = 'CREDIT'
                         THEN COALESCE(CREDIT_LIMIT, 0) ELSE 0 END)              AS TOTAL_CREDIT_LIMIT,
                SUM(CASE WHEN ACCOUNT_TYPE = 'CREDIT'
                         THEN COALESCE(CURRENT_BALANCE, 0) ELSE 0 END)           AS CREDIT_BALANCE
            FROM core.ACCOUNTS
            GROUP BY CUSTOMER_ID
        ) acct_agg
            ON c.CUSTOMER_ID = acct_agg.CUSTOMER_ID
        WHERE c.CUSTOMER_STATUS IN ('A', 'I');   -- exclude closed customers

        CREATE STATISTICS st_STG_CUSTOMER_360_CUSTOMER_ID     ON stg.STG_CUSTOMER_360 (CUSTOMER_ID);
        CREATE STATISTICS st_STG_CUSTOMER_360_STATE_CODE      ON stg.STG_CUSTOMER_360 (STATE_CODE);
        CREATE STATISTICS st_STG_CUSTOMER_360_CUSTOMER_STATUS ON stg.STG_CUSTOMER_360 (CUSTOMER_STATUS);
        CREATE STATISTICS st_STG_CUSTOMER_360_SEGMENT_CODE    ON stg.STG_CUSTOMER_360 (SEGMENT_CODE);

        SELECT @RowCount = COUNT_BIG(*) FROM stg.STG_CUSTOMER_360;

        IF @RowCount = 0
            THROW 50099, 'STG_CUSTOMER_360 loaded 0 rows', 1;

        INSERT INTO stg.ETL_RUN_LOG (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
        SELECT '01_stg_customer_360', 'FULL_LOAD', 'SUCCESS', @RowCount, @StartTs, SYSUTCDATETIME(), NULL;
    END TRY
    BEGIN CATCH
        SET @ErrMsg = LEFT(ERROR_MESSAGE(), 4000);

        INSERT INTO stg.ETL_RUN_LOG (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
        SELECT '01_stg_customer_360', 'FULL_LOAD', 'FAILED', @RowCount, @StartTs, SYSUTCDATETIME(), @ErrMsg;

        THROW;
    END CATCH
END;
