# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — `STG_TXN_SUMMARY`
# MAGIC
# MAGIC Port of `bteq/02_stg_txn_summary.bteq`: per customer/account transaction
# MAGIC rollup over the trailing `lookback_months` window (posted transactions only).
# MAGIC
# MAGIC | Teradata construct | Databricks equivalent |
# MAGIC |---|---|
# MAGIC | `CREATE VOLATILE TABLE VT_RUN_PARAMS ... ON COMMIT PRESERVE ROWS` | `run_params` CTE built from the `run_date`/`lookback_months` job parameters |
# MAGIC | `ADD_MONTHS(CURRENT_DATE, -${LOOKBACK_MONTHS})` | `add_months(run_date, -lookback_months)` |
# MAGIC | `NULLIFZERO(COUNT(*))` | `nullif(COUNT(*), 0)` |
# MAGIC | `CURRENT_DATE - MAX(TRANSACTION_DATE)` | `datediff(run_date, MAX(TRANSACTION_DATE))` |
# MAGIC | `PRIMARY INDEX (CUSTOMER_ID, ACCOUNT_ID)` | liquid clustering on the same columns |
# MAGIC
# MAGIC The top-merchant-category subquery keeps the original
# MAGIC `QUALIFY ROW_NUMBER() OVER (... ORDER BY SUM(ABS(AMOUNT)) OVER (...) DESC)`
# MAGIC shape, which Databricks SQL supports natively.

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

JOB_NAME = "02_stg_txn_summary"
TARGET_TABLE = "STG_TXN_SUMMARY"

# COMMAND ----------


def build_stg_txn_summary(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Account-level transaction rollup for the trailing lookback window."""
    run_date = cfg.run_date_literal
    period_start = f"add_months({run_date}, -{cfg.lookback_months})"
    return spark.sql(
        f"""
        WITH run_params AS (
            SELECT {period_start} AS PERIOD_START, {run_date} AS PERIOD_END
        ),
        top_cat AS (
            /* Top merchant category per account by total absolute spend */
            SELECT
                t2.ACCOUNT_ID,
                t2.MERCHANT_CATEGORY
            FROM {cfg.bronze('TRANSACTIONS')} t2
            INNER JOIN run_params rp2
                ON t2.TRANSACTION_DATE BETWEEN rp2.PERIOD_START AND rp2.PERIOD_END
            WHERE t2.STATUS_CODE = 'P'
              AND t2.MERCHANT_CATEGORY IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t2.ACCOUNT_ID
                ORDER BY SUM(ABS(t2.AMOUNT)) OVER (
                    PARTITION BY t2.ACCOUNT_ID, t2.MERCHANT_CATEGORY
                ) DESC
            ) = 1
        )
        SELECT
            acct.CUSTOMER_ID,
            acct.ACCOUNT_ID,
            acct.ACCOUNT_TYPE,
            rp.PERIOD_START                                             AS SUMMARY_PERIOD_START,
            rp.PERIOD_END                                               AS SUMMARY_PERIOD_END,
            /* ---- Volume Counts ---- */
            COUNT(*)                                                    AS TXN_COUNT_TOTAL,
            SUM(CASE WHEN tt.CATEGORY = 'DEBIT'  THEN 1 ELSE 0 END)     AS TXN_COUNT_DEBIT,
            SUM(CASE WHEN tt.CATEGORY = 'CREDIT' THEN 1 ELSE 0 END)     AS TXN_COUNT_CREDIT,
            SUM(CASE WHEN tt.CATEGORY = 'FEE'    THEN 1 ELSE 0 END)     AS TXN_COUNT_FEE,
            /* ---- Dollar Amounts ---- */
            SUM(CASE WHEN tt.CATEGORY = 'DEBIT'
                     THEN ABS(t.AMOUNT) ELSE 0 END)                     AS AMT_TOTAL_DEBIT,
            SUM(CASE WHEN tt.CATEGORY = 'CREDIT'
                     THEN t.AMOUNT ELSE 0 END)                          AS AMT_TOTAL_CREDIT,
            SUM(CASE WHEN tt.CATEGORY = 'FEE'
                     THEN ABS(t.AMOUNT) ELSE 0 END)                     AS AMT_TOTAL_FEES,
            /* ---- Averages ---- */
            AVG(CASE WHEN tt.CATEGORY = 'DEBIT'
                     THEN ABS(t.AMOUNT) ELSE NULL END)                  AS AMT_AVG_DEBIT,
            AVG(CASE WHEN tt.CATEGORY = 'CREDIT'
                     THEN t.AMOUNT ELSE NULL END)                       AS AMT_AVG_CREDIT,
            /* ---- Maximums ---- */
            MAX(CASE WHEN tt.CATEGORY = 'DEBIT'
                     THEN ABS(t.AMOUNT) ELSE 0 END)                     AS AMT_MAX_SINGLE_DEBIT,
            MAX(CASE WHEN tt.CATEGORY = 'CREDIT'
                     THEN t.AMOUNT ELSE 0 END)                          AS AMT_MAX_SINGLE_CREDIT,
            /* ---- Merchant Diversity ---- */
            COUNT(DISTINCT t.MERCHANT_NAME)                             AS DISTINCT_MERCHANTS,
            /* ---- Top merchant category (by spend) ---- */
            MAX(top_cat.MERCHANT_CATEGORY)                              AS TOP_MERCHANT_CATEGORY,
            /* ---- Channel Mix ---- */
            CAST(SUM(CASE WHEN t.CHANNEL_CODE = 'ATM' THEN 1 ELSE 0 END) * 100.0
                 / nullif(COUNT(*), 0) AS DECIMAL(5,2))                 AS PCT_ATM,
            CAST(SUM(CASE WHEN t.CHANNEL_CODE = 'POS' THEN 1 ELSE 0 END) * 100.0
                 / nullif(COUNT(*), 0) AS DECIMAL(5,2))                 AS PCT_POS,
            CAST(SUM(CASE WHEN t.CHANNEL_CODE = 'WEB' THEN 1 ELSE 0 END) * 100.0
                 / nullif(COUNT(*), 0) AS DECIMAL(5,2))                 AS PCT_WEB,
            CAST(SUM(CASE WHEN t.CHANNEL_CODE IN ('MOB') THEN 1 ELSE 0 END) * 100.0
                 / nullif(COUNT(*), 0) AS DECIMAL(5,2))                 AS PCT_MOBILE,
            /* ---- Recency ---- */
            CAST(datediff({run_date}, MAX(t.TRANSACTION_DATE)) AS INT)  AS DAYS_SINCE_LAST_TXN,
            current_timestamp()                                         AS LOAD_TS
        FROM {cfg.bronze('TRANSACTIONS')} t
        INNER JOIN {cfg.bronze('ACCOUNTS')} acct
            ON t.ACCOUNT_ID = acct.ACCOUNT_ID
        INNER JOIN {cfg.bronze('TRANSACTION_TYPES')} tt
            ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
        CROSS JOIN run_params rp
        LEFT JOIN top_cat
            ON t.ACCOUNT_ID = top_cat.ACCOUNT_ID
        WHERE t.TRANSACTION_DATE BETWEEN rp.PERIOD_START AND rp.PERIOD_END
          AND t.STATUS_CODE = 'P'   /* Posted transactions only */
        GROUP BY
            acct.CUSTOMER_ID,
            acct.ACCOUNT_ID,
            acct.ACCOUNT_TYPE,
            rp.PERIOD_START,
            rp.PERIOD_END,
            top_cat.MERCHANT_CATEGORY
        """
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    ensure_run_log(spark, cfg)
    target = cfg.silver(TARGET_TABLE)

    with step(spark, cfg, JOB_NAME, "FULL_LOAD") as ctx:
        df = schemas.conform(
            build_stg_txn_summary(spark, cfg), schemas.SILVER_SCHEMAS[TARGET_TABLE]
        )
        ctx["row_count"] = io.write_table(
            spark, cfg, df, target, merge_keys=["CUSTOMER_ID", "ACCOUNT_ID"]
        )
        ctx["message"] = f"lookback_months={cfg.lookback_months}"
        rows = ctx["row_count"]

    validate_and_log(
        spark,
        cfg,
        JOB_NAME,
        target,
        key_cols=["CUSTOMER_ID", "ACCOUNT_ID"],
        not_null=["CUSTOMER_ID", "ACCOUNT_ID", "TXN_COUNT_TOTAL"],
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
