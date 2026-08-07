# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — `STG_RISK_FACTORS`
# MAGIC
# MAGIC Port of `bteq/03_stg_risk_factors.bteq`: overdraft/NSF, large withdrawals,
# MAGIC balance level and volatility, credit utilisation, payment history, bureau
# MAGIC score, debit velocity and merchant risk indicators — one row per customer.
# MAGIC
# MAGIC | Teradata construct | Databricks equivalent |
# MAGIC |---|---|
# MAGIC | `WRK_DAILY_BALANCE` / `WRK_PAYMENT_HISTORY` work tables | `daily_balance` / `payment_history` CTEs (no intermediate persistence) |
# MAGIC | `ADD_MONTHS(CURRENT_DATE, -3 / -6 / -12 / -24)` | `add_months(run_date, ...)` |
# MAGIC | `CURRENT_DATE - 30` | `date_sub(run_date, 30)` |
# MAGIC | `MONTHS_BETWEEN(x, y) (INTEGER)` | `CAST(months_between(x, y) AS INT)` |
# MAGIC | `STDDEV_POP(EOD_BALANCE)` | `stddev_pop(EOD_BALANCE)` |
# MAGIC | correlated `MERCHANT_NAME NOT IN (SELECT ... WHERE t2.ACCOUNT_ID = t.ACCOUNT_ID)` | `LEFT ANTI`-style join against `prior_merchants` (Spark cannot correlate a subquery inside an aggregate) |
# MAGIC
# MAGIC The rewritten new-merchant rule is logically identical: a merchant counts as
# MAGIC new when the account has no posted-or-otherwise transaction with that
# MAGIC merchant before `run_date - 30`.

# COMMAND ----------

from __future__ import annotations

import os
import sys


def _bootstrap() -> None:
    here = os.path.dirname(os.path.abspath(globals().get("__file__", os.path.join(os.getcwd(), "nb.py"))))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    if root not in sys.path:
        sys.path.insert(0, root)


_bootstrap()

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from shared import io, schemas  # noqa: E402
from shared.audit import ensure_run_log, step  # noqa: E402
from shared.config import PipelineConfig, exit_if_skipped  # noqa: E402
from shared.logging_utils import get_logger, log_event  # noqa: E402
from shared.validation import validate_and_log  # noqa: E402

JOB_NAME = "03_stg_risk_factors"
TARGET_TABLE = "STG_RISK_FACTORS"

HIGH_RISK_CATEGORIES = ("GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP")

# COMMAND ----------


def build_stg_risk_factors(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Assemble every risk factor for the customer base (status A or I)."""
    run_date = cfg.run_date_literal
    transactions = cfg.bronze("TRANSACTIONS")
    accounts = cfg.bronze("ACCOUNTS")
    txn_types = cfg.bronze("TRANSACTION_TYPES")
    high_risk = ", ".join(f"'{c}'" for c in HIGH_RISK_CATEGORIES)

    return spark.sql(
        f"""
        WITH daily_balance AS (
            /* Last posted transaction per account per day, 3-month window */
            SELECT
                acct.CUSTOMER_ID,
                t.ACCOUNT_ID,
                t.TRANSACTION_DATE,
                t.RUNNING_BALANCE AS EOD_BALANCE
            FROM {transactions} t
            INNER JOIN {accounts} acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            WHERE t.TRANSACTION_DATE >= add_months({run_date}, -3)
              AND t.STATUS_CODE = 'P'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.ACCOUNT_ID, t.TRANSACTION_DATE
                ORDER BY t.TRANSACTION_TS DESC
            ) = 1
        ),
        payment_history AS (
            /* Payment behaviour on credit and loan accounts, 24-month window */
            SELECT
                acct.CUSTOMER_ID,
                acct.ACCOUNT_ID,
                COUNT(*) AS TOTAL_PAYMENTS,
                SUM(CASE
                    WHEN t.TRANSACTION_DATE <= add_months(acct.OPEN_DATE,
                         CAST(months_between(t.TRANSACTION_DATE, acct.OPEN_DATE) AS INT) + 1)
                    THEN 1 ELSE 0
                END) AS ONTIME_PAYMENTS,
                SUM(CASE
                    WHEN t.TRANSACTION_DATE > add_months(acct.OPEN_DATE,
                         CAST(months_between(t.TRANSACTION_DATE, acct.OPEN_DATE) AS INT) + 1)
                    THEN 1 ELSE 0
                END) AS LATE_PAYMENTS,
                CAST(months_between(
                    {run_date},
                    COALESCE(MAX(CASE
                        WHEN t.TRANSACTION_DATE > add_months(acct.OPEN_DATE,
                             CAST(months_between(t.TRANSACTION_DATE, acct.OPEN_DATE) AS INT) + 1)
                        THEN t.TRANSACTION_DATE
                    END), acct.OPEN_DATE)
                ) AS INT) AS MONTHS_SINCE_LAST_LATE
            FROM {transactions} t
            INNER JOIN {accounts} acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            INNER JOIN {txn_types} tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
            WHERE acct.ACCOUNT_TYPE IN ('CREDIT', 'LOAN')
              AND tt.CATEGORY = 'CREDIT'          /* Payment transactions */
              AND t.STATUS_CODE = 'P'
              AND t.TRANSACTION_DATE >= add_months({run_date}, -24)
            GROUP BY acct.CUSTOMER_ID, acct.ACCOUNT_ID, acct.OPEN_DATE
        ),
        overdraft AS (
            SELECT
                acct.CUSTOMER_ID,
                SUM(CASE WHEN t.RUNNING_BALANCE < 0 THEN 1 ELSE 0 END) AS OVERDRAFT_COUNT,
                SUM(CASE WHEN tt.CATEGORY = 'FEE' AND tt.DESCRIPTION LIKE '%NSF%'
                         THEN ABS(t.AMOUNT) ELSE 0 END)                AS NSF_TOTAL
            FROM {transactions} t
            INNER JOIN {accounts} acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            INNER JOIN {txn_types} tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
            WHERE t.TRANSACTION_DATE >= add_months({run_date}, -12)
              AND t.STATUS_CODE = 'P'
            GROUP BY acct.CUSTOMER_ID
        ),
        large_withdrawals AS (
            SELECT
                acct.CUSTOMER_ID,
                COUNT(*)           AS LARGE_WD_CNT,
                SUM(ABS(t.AMOUNT)) AS LARGE_WD_AMT
            FROM {transactions} t
            INNER JOIN {accounts} acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            INNER JOIN {txn_types} tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
            WHERE tt.CATEGORY = 'DEBIT'
              AND ABS(t.AMOUNT) >= 5000
              AND t.TRANSACTION_DATE >= add_months({run_date}, -12)
              AND t.STATUS_CODE = 'P'
            GROUP BY acct.CUSTOMER_ID
        ),
        balances AS (
            SELECT
                CUSTOMER_ID,
                AVG(CASE WHEN TRANSACTION_DATE >= date_sub({run_date}, 30) THEN EOD_BALANCE END) AS AVG_BAL_30D,
                AVG(CASE WHEN TRANSACTION_DATE >= date_sub({run_date}, 90) THEN EOD_BALANCE END) AS AVG_BAL_90D,
                stddev_pop(EOD_BALANCE)                                                          AS BAL_STDDEV
            FROM daily_balance
            GROUP BY CUSTOMER_ID
        ),
        credit AS (
            SELECT
                CUSTOMER_ID,
                SUM(COALESCE(CURRENT_BALANCE, 0)) AS TOTAL_CREDIT_BAL,
                SUM(COALESCE(CREDIT_LIMIT, 0))    AS TOTAL_CREDIT_LIMIT
            FROM {accounts}
            WHERE ACCOUNT_TYPE = 'CREDIT'
              AND ACCOUNT_STATUS = 'O'
            GROUP BY CUSTOMER_ID
        ),
        payments AS (
            SELECT
                CUSTOMER_ID,
                SUM(TOTAL_PAYMENTS)         AS TOTAL_PAYMENTS,
                SUM(ONTIME_PAYMENTS)        AS ONTIME_PAYMENTS,
                SUM(LATE_PAYMENTS)          AS LATE_PAYMENTS,
                MIN(MONTHS_SINCE_LAST_LATE) AS MONTHS_SINCE_LAST_LATE
            FROM payment_history
            GROUP BY CUSTOMER_ID
        ),
        bureau AS (
            SELECT CUSTOMER_ID, EXTERNAL_CREDIT_SCORE AS CREDIT_SCORE
            FROM {cfg.bronze('CUSTOMER_BUREAU_SCORES')}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CUSTOMER_ID ORDER BY REPORT_DATE DESC
            ) = 1
        ),
        velocity AS (
            SELECT
                acct.CUSTOMER_ID,
                SUM(CASE WHEN t.TRANSACTION_DATE >= date_sub({run_date}, 7)
                         THEN ABS(t.AMOUNT) ELSE 0 END) AS DEBIT_7D,
                SUM(CASE WHEN t.TRANSACTION_DATE >= date_sub({run_date}, 30)
                         THEN ABS(t.AMOUNT) ELSE 0 END) AS DEBIT_30D
            FROM {transactions} t
            INNER JOIN {accounts} acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            INNER JOIN {txn_types} tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
            WHERE tt.CATEGORY = 'DEBIT'
              AND t.TRANSACTION_DATE >= date_sub({run_date}, 30)
              AND t.STATUS_CODE = 'P'
            GROUP BY acct.CUSTOMER_ID
        ),
        prior_merchants AS (
            /* Merchants an account transacted with before the 30-day window */
            SELECT DISTINCT ACCOUNT_ID, MERCHANT_NAME
            FROM {transactions}
            WHERE TRANSACTION_DATE < date_sub({run_date}, 30)
              AND MERCHANT_NAME IS NOT NULL
        ),
        merchant_activity AS (
            SELECT
                acct.CUSTOMER_ID,
                t.TRANSACTION_DATE,
                t.MERCHANT_NAME,
                t.MERCHANT_CATEGORY,
                t.CHANNEL_CODE,
                CASE WHEN pm.ACCOUNT_ID IS NULL THEN 1 ELSE 0 END AS IS_NEW_MERCHANT
            FROM {transactions} t
            INNER JOIN {accounts} acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            LEFT JOIN prior_merchants pm
                   ON t.ACCOUNT_ID = pm.ACCOUNT_ID
                  AND t.MERCHANT_NAME = pm.MERCHANT_NAME
            WHERE t.TRANSACTION_DATE >= add_months({run_date}, -6)
              AND t.STATUS_CODE = 'P'
        ),
        merchant_risk AS (
            SELECT
                CUSTOMER_ID,
                COUNT(DISTINCT CASE
                    WHEN TRANSACTION_DATE >= date_sub({run_date}, 30) AND IS_NEW_MERCHANT = 1
                    THEN MERCHANT_NAME
                END)                                                             AS NEW_MERCH_30D,
                SUM(CASE WHEN CHANNEL_CODE = 'INTL' THEN 1 ELSE 0 END)           AS INTL_TXN_CNT,
                SUM(CASE WHEN MERCHANT_CATEGORY IN ({high_risk})
                         THEN 1 ELSE 0 END)                                      AS HIGH_RISK_CNT
            FROM merchant_activity
            GROUP BY CUSTOMER_ID
        )
        SELECT
            c.CUSTOMER_ID,
            /* ---- Overdraft & NSF ---- */
            COALESCE(overdraft.OVERDRAFT_COUNT, 0)      AS ACCOUNT_OVERDRAFT_CNT,
            COALESCE(overdraft.NSF_TOTAL, 0.00)         AS NSF_FEE_TOTAL,
            /* ---- Large Withdrawal Detection ---- */
            COALESCE(lg_wd.LARGE_WD_CNT, 0)             AS LARGE_WITHDRAWAL_CNT,
            COALESCE(lg_wd.LARGE_WD_AMT, 0.00)          AS LARGE_WITHDRAWAL_AMT,
            /* ---- Balance Metrics ---- */
            COALESCE(bal.AVG_BAL_30D, 0.00)             AS AVG_DAILY_BALANCE_30D,
            COALESCE(bal.AVG_BAL_90D, 0.00)             AS AVG_DAILY_BALANCE_90D,
            COALESCE(bal.BAL_STDDEV, 0.0000)            AS BALANCE_VOLATILITY,
            /* ---- Credit Utilization ---- */
            CASE
                WHEN credit.TOTAL_CREDIT_LIMIT > 0
                THEN CAST(credit.TOTAL_CREDIT_BAL / credit.TOTAL_CREDIT_LIMIT AS DECIMAL(5,4))
                ELSE 0.0000
            END                                         AS CREDIT_UTIL_RATIO,
            /* ---- Payment History ---- */
            CASE
                WHEN pmh.TOTAL_PAYMENTS > 0
                THEN CAST(pmh.ONTIME_PAYMENTS * 100.0 / pmh.TOTAL_PAYMENTS AS DECIMAL(5,2))
                ELSE 100.00
            END                                         AS PAYMENT_ONTIME_PCT,
            COALESCE(pmh.LATE_PAYMENTS, 0)              AS PAYMENT_LATE_CNT,
            COALESCE(pmh.MONTHS_SINCE_LAST_LATE, 999)   AS MONTHS_SINCE_LAST_LATE,
            /* ---- External Bureau Score ---- */
            COALESCE(bureau.CREDIT_SCORE, 0)            AS EXTERNAL_CREDIT_SCORE,
            /* ---- Transaction Velocity ---- */
            COALESCE(vel.DEBIT_7D, 0.00)                AS DEBIT_VELOCITY_7D,
            COALESCE(vel.DEBIT_30D, 0.00)               AS DEBIT_VELOCITY_30D,
            /* ---- Merchant Risk Indicators ---- */
            COALESCE(merch.NEW_MERCH_30D, 0)            AS NEW_MERCHANT_CNT_30D,
            COALESCE(merch.INTL_TXN_CNT, 0)             AS INTERNATIONAL_TXN_CNT,
            COALESCE(merch.HIGH_RISK_CNT, 0)            AS HIGH_RISK_MERCHANT_CNT,
            current_timestamp()                         AS LOAD_TS
        FROM {cfg.bronze('CUSTOMERS')} c
        LEFT JOIN overdraft         ON c.CUSTOMER_ID = overdraft.CUSTOMER_ID
        LEFT JOIN large_withdrawals lg_wd ON c.CUSTOMER_ID = lg_wd.CUSTOMER_ID
        LEFT JOIN balances bal      ON c.CUSTOMER_ID = bal.CUSTOMER_ID
        LEFT JOIN credit            ON c.CUSTOMER_ID = credit.CUSTOMER_ID
        LEFT JOIN payments pmh      ON c.CUSTOMER_ID = pmh.CUSTOMER_ID
        LEFT JOIN bureau            ON c.CUSTOMER_ID = bureau.CUSTOMER_ID
        LEFT JOIN velocity vel      ON c.CUSTOMER_ID = vel.CUSTOMER_ID
        LEFT JOIN merchant_risk merch ON c.CUSTOMER_ID = merch.CUSTOMER_ID
        WHERE c.CUSTOMER_STATUS IN ('A', 'I')
        """
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    ensure_run_log(spark, cfg)
    target = cfg.silver(TARGET_TABLE)

    with step(spark, cfg, JOB_NAME, "FULL_LOAD") as ctx:
        df = schemas.conform(
            build_stg_risk_factors(spark, cfg), schemas.SILVER_SCHEMAS[TARGET_TABLE]
        )
        ctx["row_count"] = io.write_table(spark, cfg, df, target, merge_keys=["CUSTOMER_ID"])
        rows = ctx["row_count"]

    validate_and_log(
        spark,
        cfg,
        JOB_NAME,
        target,
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID", "PAYMENT_ONTIME_PCT", "CREDIT_UTIL_RATIO"],
    )
    return rows


# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = PipelineConfig.from_widgets(spark)
    logger = get_logger()
    log_event(logger, "job_start", run_id=cfg.run_id, job=JOB_NAME, config=cfg.describe())

    if not exit_if_skipped(cfg, "silver", spark):
        log_event(
            logger,
            "job_complete",
            run_id=cfg.run_id,
            job=JOB_NAME,
            table=TARGET_TABLE,
            row_count=run(spark, cfg),
        )
