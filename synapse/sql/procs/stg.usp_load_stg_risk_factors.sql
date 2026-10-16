/* =============================================================================
   Procedure: stg.usp_load_stg_risk_factors
   Legacy:    bteq/03_stg_risk_factors.bteq  (Teradata BTEQ)
   Target:    stg.STG_RISK_FACTORS  (Azure Synapse dedicated SQL pool)
   Purpose:   Per-customer risk feature vector for the risk scoring model:
              overdraft/NSF, large withdrawals, balance level and volatility,
              credit utilisation, payment history, bureau score, debit
              velocity and merchant risk indicators.
   Usage:     EXEC stg.usp_load_stg_risk_factors @AsOfDate = '2026-04-10';
              @AsOfDate replaces Teradata CURRENT_DATE (default: today, UTC).

   Teradata MONTHS_BETWEEN(d1, d2) is reproduced inline as
       (YEAR(d1) - YEAR(d2)) * 12 + MONTH(d1) - MONTH(d2)
       + (DAY(d1) - DAY(d2)) / 31.0
   with the fraction forced to 0 when both dates share the day of month or are
   both month-ends. DATEDIFF(MONTH, ..) counts boundaries and is not equivalent.
   CAST(... AS INT) truncates toward zero, matching Teradata (INTEGER).
   ============================================================================= */
CREATE OR ALTER PROCEDURE stg.usp_load_stg_risk_factors
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SET @AsOfDate = COALESCE(@AsOfDate, CAST(GETUTCDATE() AS DATE));

    DECLARE @StartTs  DATETIME2(6) = SYSUTCDATETIME();
    DECLARE @RowCount BIGINT;
    DECLARE @ErrMsg   VARCHAR(4000);

    DECLARE @Minus3M  DATE = DATEADD(MONTH, -3,  @AsOfDate);
    DECLARE @Minus6M  DATE = DATEADD(MONTH, -6,  @AsOfDate);
    DECLARE @Minus12M DATE = DATEADD(MONTH, -12, @AsOfDate);
    DECLARE @Minus24M DATE = DATEADD(MONTH, -24, @AsOfDate);
    DECLARE @Minus7D  DATE = DATEADD(DAY,   -7,  @AsOfDate);
    DECLARE @Minus30D DATE = DATEADD(DAY,   -30, @AsOfDate);
    DECLARE @Minus90D DATE = DATEADD(DAY,   -90, @AsOfDate);

    BEGIN TRY
        /* -----------------------------------------------------------------
           Work table 1: last posted running balance per account per day
           over the last 3 months (QUALIFY -> ROW_NUMBER in derived table).
           ----------------------------------------------------------------- */
        IF OBJECT_ID('stg.WRK_DAILY_BALANCE') IS NOT NULL
            DROP TABLE stg.WRK_DAILY_BALANCE;

        CREATE TABLE stg.WRK_DAILY_BALANCE
        WITH (DISTRIBUTION = HASH(CUSTOMER_ID), HEAP)
        AS
        SELECT
            CUSTOMER_ID,
            ACCOUNT_ID,
            TRANSACTION_DATE,
            EOD_BALANCE
        FROM (
            SELECT
                acct.CUSTOMER_ID,
                t.ACCOUNT_ID,
                t.TRANSACTION_DATE,
                t.RUNNING_BALANCE AS EOD_BALANCE,
                ROW_NUMBER() OVER (
                    PARTITION BY t.ACCOUNT_ID, t.TRANSACTION_DATE
                    ORDER BY t.TRANSACTION_TS DESC, t.TRANSACTION_ID DESC
                ) AS RN
            FROM txn.TRANSACTIONS t
            INNER JOIN core.ACCOUNTS acct
                ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            WHERE t.TRANSACTION_DATE >= @Minus3M
              AND t.STATUS_CODE = 'P'
        ) d
        WHERE RN = 1;

        CREATE STATISTICS st_WRK_DAILY_BALANCE_CUSTOMER_ID
            ON stg.WRK_DAILY_BALANCE (CUSTOMER_ID);

        /* -----------------------------------------------------------------
           Work table 2: payment behaviour on CREDIT/LOAN accounts, 24 months.
           A payment is late when it posts after
           ADD_MONTHS(OPEN_DATE, TRUNC(MONTHS_BETWEEN(txn_date, OPEN_DATE)) + 1).
           ----------------------------------------------------------------- */
        IF OBJECT_ID('stg.WRK_PAYMENT_HISTORY') IS NOT NULL
            DROP TABLE stg.WRK_PAYMENT_HISTORY;

        CREATE TABLE stg.WRK_PAYMENT_HISTORY
        WITH (DISTRIBUTION = HASH(CUSTOMER_ID), HEAP)
        AS
        SELECT
            agg.CUSTOMER_ID,
            agg.ACCOUNT_ID,
            agg.TOTAL_PAYMENTS,
            agg.ONTIME_PAYMENTS,
            agg.LATE_PAYMENTS,
            CAST(
                  (YEAR(@AsOfDate) - YEAR(agg.LAST_LATE_OR_OPEN)) * 12
                + (MONTH(@AsOfDate) - MONTH(agg.LAST_LATE_OR_OPEN))
                + CASE
                      WHEN DAY(@AsOfDate) = DAY(agg.LAST_LATE_OR_OPEN)
                        OR (@AsOfDate = EOMONTH(@AsOfDate)
                            AND agg.LAST_LATE_OR_OPEN = EOMONTH(agg.LAST_LATE_OR_OPEN))
                      THEN 0.0
                      ELSE (DAY(@AsOfDate) - DAY(agg.LAST_LATE_OR_OPEN)) / 31.0
                  END
            AS INT) AS MONTHS_SINCE_LAST_LATE
        FROM (
            SELECT
                pay.CUSTOMER_ID,
                pay.ACCOUNT_ID,
                CAST(COUNT(*) AS INT)                                  AS TOTAL_PAYMENTS,
                CAST(SUM(1 - pay.IS_LATE) AS INT)                      AS ONTIME_PAYMENTS,
                CAST(SUM(pay.IS_LATE) AS INT)                          AS LATE_PAYMENTS,
                COALESCE(MAX(CASE WHEN pay.IS_LATE = 1 THEN pay.TRANSACTION_DATE END),
                         MAX(pay.OPEN_DATE))                           AS LAST_LATE_OR_OPEN
            FROM (
                SELECT
                    mb.CUSTOMER_ID,
                    mb.ACCOUNT_ID,
                    mb.OPEN_DATE,
                    mb.TRANSACTION_DATE,
                    CASE
                        WHEN mb.TRANSACTION_DATE >
                             DATEADD(MONTH, CAST(mb.MONTHS_BETWEEN_OPEN AS INT) + 1, mb.OPEN_DATE)
                        THEN 1 ELSE 0
                    END AS IS_LATE
                FROM (
                    SELECT
                        acct.CUSTOMER_ID,
                        acct.ACCOUNT_ID,
                        acct.OPEN_DATE,
                        t.TRANSACTION_DATE,
                          (YEAR(t.TRANSACTION_DATE) - YEAR(acct.OPEN_DATE)) * 12
                        + (MONTH(t.TRANSACTION_DATE) - MONTH(acct.OPEN_DATE))
                        + CASE
                              WHEN DAY(t.TRANSACTION_DATE) = DAY(acct.OPEN_DATE)
                                OR (t.TRANSACTION_DATE = EOMONTH(t.TRANSACTION_DATE)
                                    AND acct.OPEN_DATE = EOMONTH(acct.OPEN_DATE))
                              THEN 0.0
                              ELSE (DAY(t.TRANSACTION_DATE) - DAY(acct.OPEN_DATE)) / 31.0
                          END AS MONTHS_BETWEEN_OPEN
                    FROM txn.TRANSACTIONS t
                    INNER JOIN core.ACCOUNTS acct
                        ON t.ACCOUNT_ID = acct.ACCOUNT_ID
                    INNER JOIN txn.TRANSACTION_TYPES tt
                        ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
                    WHERE acct.ACCOUNT_TYPE IN ('CREDIT', 'LOAN')
                      AND tt.CATEGORY = 'CREDIT'
                      AND t.STATUS_CODE = 'P'
                      AND t.TRANSACTION_DATE >= @Minus24M
                ) mb
            ) pay
            GROUP BY
                pay.CUSTOMER_ID,
                pay.ACCOUNT_ID
        ) agg;

        CREATE STATISTICS st_WRK_PAYMENT_HISTORY_CUSTOMER_ID
            ON stg.WRK_PAYMENT_HISTORY (CUSTOMER_ID);

        /* -----------------------------------------------------------------
           Final table
           ----------------------------------------------------------------- */
        IF OBJECT_ID('stg.STG_RISK_FACTORS') IS NOT NULL
            DROP TABLE stg.STG_RISK_FACTORS;

        CREATE TABLE stg.STG_RISK_FACTORS
        WITH (DISTRIBUTION = HASH(CUSTOMER_ID), CLUSTERED COLUMNSTORE INDEX)
        AS
        SELECT
            CAST(c.CUSTOMER_ID AS BIGINT)                                   AS CUSTOMER_ID,
            CAST(COALESCE(overdraft.OVERDRAFT_COUNT, 0) AS INT)             AS ACCOUNT_OVERDRAFT_CNT,
            CAST(COALESCE(overdraft.NSF_TOTAL, 0.00) AS DECIMAL(15,2))      AS NSF_FEE_TOTAL,
            CAST(COALESCE(lg_wd.LARGE_WD_CNT, 0) AS INT)                    AS LARGE_WITHDRAWAL_CNT,
            CAST(COALESCE(lg_wd.LARGE_WD_AMT, 0.00) AS DECIMAL(18,2))       AS LARGE_WITHDRAWAL_AMT,
            CAST(COALESCE(bal.AVG_BAL_30D, 0.00) AS DECIMAL(15,2))          AS AVG_DAILY_BALANCE_30D,
            CAST(COALESCE(bal.AVG_BAL_90D, 0.00) AS DECIMAL(15,2))          AS AVG_DAILY_BALANCE_90D,
            CAST(COALESCE(bal.BAL_STDDEV, 0.0000) AS DECIMAL(10,4))         AS BALANCE_VOLATILITY,
            CAST(CASE
                     WHEN credit.TOTAL_CREDIT_LIMIT > 0
                     THEN credit.TOTAL_CREDIT_BAL / credit.TOTAL_CREDIT_LIMIT
                     ELSE 0.0000
                 END AS DECIMAL(5,4))                                       AS CREDIT_UTIL_RATIO,
            CAST(CASE
                     WHEN pmh.TOTAL_PAYMENTS > 0
                     THEN pmh.ONTIME_PAYMENTS * 100.0 / pmh.TOTAL_PAYMENTS
                     ELSE 100.00
                 END AS DECIMAL(5,2))                                       AS PAYMENT_ONTIME_PCT,
            CAST(COALESCE(pmh.LATE_PAYMENTS, 0) AS INT)                     AS PAYMENT_LATE_CNT,
            CAST(COALESCE(pmh.MONTHS_SINCE_LAST_LATE, 999) AS INT)          AS MONTHS_SINCE_LAST_LATE,
            CAST(COALESCE(bureau.CREDIT_SCORE, 0) AS INT)                   AS EXTERNAL_CREDIT_SCORE,
            CAST(COALESCE(vel.DEBIT_7D, 0.00) AS DECIMAL(15,2))             AS DEBIT_VELOCITY_7D,
            CAST(COALESCE(vel.DEBIT_30D, 0.00) AS DECIMAL(15,2))            AS DEBIT_VELOCITY_30D,
            CAST(COALESCE(merch.NEW_MERCH_30D, 0) AS INT)                   AS NEW_MERCHANT_CNT_30D,
            CAST(COALESCE(merch.INTL_TXN_CNT, 0) AS INT)                    AS INTERNATIONAL_TXN_CNT,
            CAST(COALESCE(merch.HIGH_RISK_CNT, 0) AS INT)                   AS HIGH_RISK_MERCHANT_CNT,
            CAST(SYSUTCDATETIME() AS DATETIME2(6))                          AS LOAD_TS
        FROM core.CUSTOMERS c

        /* Overdraft / NSF fees, last 12 months */
        LEFT JOIN (
            SELECT
                acct.CUSTOMER_ID,
                SUM(CASE WHEN t.RUNNING_BALANCE < 0 THEN 1 ELSE 0 END) AS OVERDRAFT_COUNT,
                SUM(CASE WHEN tt.CATEGORY = 'FEE' AND tt.DESCRIPTION LIKE '%NSF%'
                         THEN ABS(t.AMOUNT) ELSE 0 END)                AS NSF_TOTAL
            FROM txn.TRANSACTIONS t
            INNER JOIN core.ACCOUNTS acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            INNER JOIN txn.TRANSACTION_TYPES tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
            WHERE t.TRANSACTION_DATE >= @Minus12M
              AND t.STATUS_CODE = 'P'
            GROUP BY acct.CUSTOMER_ID
        ) overdraft
            ON c.CUSTOMER_ID = overdraft.CUSTOMER_ID

        /* Large withdrawals (>= 5,000 single debit), last 12 months */
        LEFT JOIN (
            SELECT
                acct.CUSTOMER_ID,
                COUNT(*)           AS LARGE_WD_CNT,
                SUM(ABS(t.AMOUNT)) AS LARGE_WD_AMT
            FROM txn.TRANSACTIONS t
            INNER JOIN core.ACCOUNTS acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            INNER JOIN txn.TRANSACTION_TYPES tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
            WHERE tt.CATEGORY = 'DEBIT'
              AND ABS(t.AMOUNT) >= 5000
              AND t.TRANSACTION_DATE >= @Minus12M
              AND t.STATUS_CODE = 'P'
            GROUP BY acct.CUSTOMER_ID
        ) lg_wd
            ON c.CUSTOMER_ID = lg_wd.CUSTOMER_ID

        /* Average daily balance (30d / 90d) and volatility (STDDEV_POP -> STDEVP) */
        LEFT JOIN (
            SELECT
                CUSTOMER_ID,
                AVG(CASE WHEN TRANSACTION_DATE >= @Minus30D THEN EOD_BALANCE END) AS AVG_BAL_30D,
                AVG(CASE WHEN TRANSACTION_DATE >= @Minus90D THEN EOD_BALANCE END) AS AVG_BAL_90D,
                STDEVP(EOD_BALANCE)                                              AS BAL_STDDEV
            FROM stg.WRK_DAILY_BALANCE
            GROUP BY CUSTOMER_ID
        ) bal
            ON c.CUSTOMER_ID = bal.CUSTOMER_ID

        /* Credit utilisation, open CREDIT accounts */
        LEFT JOIN (
            SELECT
                CUSTOMER_ID,
                SUM(COALESCE(CURRENT_BALANCE, 0)) AS TOTAL_CREDIT_BAL,
                SUM(COALESCE(CREDIT_LIMIT, 0))    AS TOTAL_CREDIT_LIMIT
            FROM core.ACCOUNTS
            WHERE ACCOUNT_TYPE = 'CREDIT'
              AND ACCOUNT_STATUS = 'O'
            GROUP BY CUSTOMER_ID
        ) credit
            ON c.CUSTOMER_ID = credit.CUSTOMER_ID

        /* Payment history summary */
        LEFT JOIN (
            SELECT
                CUSTOMER_ID,
                SUM(TOTAL_PAYMENTS)         AS TOTAL_PAYMENTS,
                SUM(ONTIME_PAYMENTS)        AS ONTIME_PAYMENTS,
                SUM(LATE_PAYMENTS)          AS LATE_PAYMENTS,
                MIN(MONTHS_SINCE_LAST_LATE) AS MONTHS_SINCE_LAST_LATE
            FROM stg.WRK_PAYMENT_HISTORY
            GROUP BY CUSTOMER_ID
        ) pmh
            ON c.CUSTOMER_ID = pmh.CUSTOMER_ID

        /* Latest bureau score (QUALIFY -> ROW_NUMBER in derived table) */
        LEFT JOIN (
            SELECT CUSTOMER_ID, CREDIT_SCORE
            FROM (
                SELECT
                    CUSTOMER_ID,
                    EXTERNAL_CREDIT_SCORE AS CREDIT_SCORE,
                    ROW_NUMBER() OVER (
                        PARTITION BY CUSTOMER_ID ORDER BY REPORT_DATE DESC
                    ) AS RN
                FROM core.CUSTOMER_BUREAU_SCORES
            ) b
            WHERE RN = 1
        ) bureau
            ON c.CUSTOMER_ID = bureau.CUSTOMER_ID

        /* Debit velocity (7d / 30d) */
        LEFT JOIN (
            SELECT
                acct.CUSTOMER_ID,
                SUM(CASE WHEN t.TRANSACTION_DATE >= @Minus7D
                         THEN ABS(t.AMOUNT) ELSE 0 END)  AS DEBIT_7D,
                SUM(CASE WHEN t.TRANSACTION_DATE >= @Minus30D
                         THEN ABS(t.AMOUNT) ELSE 0 END)  AS DEBIT_30D
            FROM txn.TRANSACTIONS t
            INNER JOIN core.ACCOUNTS acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            INNER JOIN txn.TRANSACTION_TYPES tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
            WHERE tt.CATEGORY = 'DEBIT'
              AND t.TRANSACTION_DATE >= @Minus30D
              AND t.STATUS_CODE = 'P'
            GROUP BY acct.CUSTOMER_ID
        ) vel
            ON c.CUSTOMER_ID = vel.CUSTOMER_ID

        /* Merchant risk indicators, last 6 months.
           The legacy correlated NOT IN on TRANSACTIONS t2 (any status, before
           as-of - 30 days, same account) is a LEFT JOIN anti-join to the
           distinct prior (ACCOUNT_ID, MERCHANT_NAME) set: aggregates cannot
           contain subqueries in T-SQL. */
        LEFT JOIN (
            SELECT
                acct.CUSTOMER_ID,
                COUNT(DISTINCT CASE
                    WHEN t.TRANSACTION_DATE >= @Minus30D
                     AND prior.MERCHANT_NAME IS NULL
                    THEN t.MERCHANT_NAME
                END)                                                        AS NEW_MERCH_30D,
                SUM(CASE WHEN t.CHANNEL_CODE = 'INTL' THEN 1 ELSE 0 END)   AS INTL_TXN_CNT,
                SUM(CASE WHEN t.MERCHANT_CATEGORY IN (
                        'GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP'
                    ) THEN 1 ELSE 0 END)                                    AS HIGH_RISK_CNT
            FROM txn.TRANSACTIONS t
            INNER JOIN core.ACCOUNTS acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            LEFT JOIN (
                SELECT DISTINCT t2.ACCOUNT_ID, t2.MERCHANT_NAME
                FROM txn.TRANSACTIONS t2
                WHERE t2.TRANSACTION_DATE < @Minus30D
                  AND t2.MERCHANT_NAME IS NOT NULL
            ) prior
                ON  prior.ACCOUNT_ID    = t.ACCOUNT_ID
                AND prior.MERCHANT_NAME = t.MERCHANT_NAME
            WHERE t.TRANSACTION_DATE >= @Minus6M
              AND t.STATUS_CODE = 'P'
            GROUP BY acct.CUSTOMER_ID
        ) merch
            ON c.CUSTOMER_ID = merch.CUSTOMER_ID

        WHERE c.CUSTOMER_STATUS IN ('A', 'I');

        CREATE STATISTICS st_STG_RISK_FACTORS_CUSTOMER_ID
            ON stg.STG_RISK_FACTORS (CUSTOMER_ID);

        SELECT @RowCount = COUNT_BIG(*) FROM stg.STG_RISK_FACTORS;

        IF @RowCount = 0
            THROW 50099, 'STG_RISK_FACTORS loaded 0 rows', 1;

        IF OBJECT_ID('stg.WRK_DAILY_BALANCE') IS NOT NULL
            DROP TABLE stg.WRK_DAILY_BALANCE;
        IF OBJECT_ID('stg.WRK_PAYMENT_HISTORY') IS NOT NULL
            DROP TABLE stg.WRK_PAYMENT_HISTORY;

        INSERT INTO stg.ETL_RUN_LOG
            (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
        VALUES
            ('03_stg_risk_factors', 'FULL_LOAD', 'SUCCESS', @RowCount,
             @StartTs, SYSUTCDATETIME(), NULL);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = LEFT(ERROR_MESSAGE(), 4000);

        IF OBJECT_ID('stg.WRK_DAILY_BALANCE') IS NOT NULL
            DROP TABLE stg.WRK_DAILY_BALANCE;
        IF OBJECT_ID('stg.WRK_PAYMENT_HISTORY') IS NOT NULL
            DROP TABLE stg.WRK_PAYMENT_HISTORY;

        INSERT INTO stg.ETL_RUN_LOG
            (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE)
        VALUES
            ('03_stg_risk_factors', 'FULL_LOAD', 'FAILED', NULL,
             @StartTs, SYSUTCDATETIME(), @ErrMsg);

        THROW;
    END CATCH
END;
