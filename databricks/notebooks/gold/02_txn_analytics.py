# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — `TRANSACTION_ANALYTICS`
# MAGIC
# MAGIC Port of `sas/02_sas_txn_analytics.sas` (model version `TXN_V2.1`).
# MAGIC
# MAGIC | SAS step | Databricks equivalent |
# MAGIC |---|---|
# MAGIC | `proc sql` account -> customer rollup | `aggregate_to_customer` |
# MAGIC | `data WORK.CUST_TXN_TREND` (trend, revenue) | same expressions in Spark SQL |
# MAGIC | `proc rank groups=100 ... ranks SPEND_PERCENTILE` | `cume_dist()` window (see note) |
# MAGIC | `proc means ... median= qrange=` + IQR rule | `percentile(...)` / `approxQuantile` |
# MAGIC | `proc sql delete` + `proc append force` | Delta overwrite/merge |
# MAGIC
# MAGIC **Percentile note.** `PROC RANK groups=100` buckets rows into hundredths.
# MAGIC The certified extract keeps two decimals, matching a cumulative
# MAGIC distribution rather than a bucket index, so this port uses `cume_dist() * 100`
# MAGIC (`percent_rank()` is the alternative when a 0-100 open interval is wanted —
# MAGIC it differs from the published values by one rank unit).

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
from pyspark.sql import functions as F  # noqa: E402

from shared import io, schemas  # noqa: E402
from shared.audit import ensure_run_log, step  # noqa: E402
from shared.config import PipelineConfig, exit_if_skipped  # noqa: E402
from shared.logging_utils import get_logger, log_event  # noqa: E402
from shared.validation import validate_and_log  # noqa: E402

JOB_NAME = "02_txn_analytics"
TARGET_TABLE = "TRANSACTION_ANALYTICS"
MODEL_VERSION = "TXN_V2.1"

# COMMAND ----------


def aggregate_to_customer(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """`WORK.CUST_TXN` + `WORK.CUST_TXN_TREND`: customer-level rollup."""
    return spark.sql(
        f"""
        WITH cust_txn AS (
            SELECT
                CUSTOMER_ID,
                COUNT(DISTINCT ACCOUNT_ID)                                   AS TOTAL_ACCOUNTS,
                SUM(CASE WHEN DAYS_SINCE_LAST_TXN <= 30 THEN 1 ELSE 0 END)   AS ACTIVE_ACCOUNTS,
                SUM(TXN_COUNT_TOTAL)                                         AS TOTAL_TRANSACTIONS,
                SUM(AMT_TOTAL_DEBIT)                                         AS TOTAL_DEBIT_AMT,
                SUM(AMT_TOTAL_CREDIT)                                        AS TOTAL_CREDIT_AMT,
                SUM(AMT_TOTAL_CREDIT) - SUM(AMT_TOTAL_DEBIT)                 AS NET_CASH_FLOW,
                CASE WHEN SUM(TXN_COUNT_TOTAL) > 0
                     THEN SUM(AMT_TOTAL_DEBIT + AMT_TOTAL_CREDIT) / SUM(TXN_COUNT_TOTAL)
                     ELSE 0 END                                              AS AVG_TRANSACTION_SIZE,
                SUM(AMT_TOTAL_FEES)                                          AS TOTAL_FEES,
                /* Top spending category across all accounts */
                MAX(TOP_MERCHANT_CATEGORY)                                   AS TOP_SPEND_CATEGORY,
                /* Digital transaction percentage, weighted by account volume */
                CASE WHEN SUM(TXN_COUNT_TOTAL) > 0
                     THEN SUM(TXN_COUNT_TOTAL * (COALESCE(PCT_WEB, 0) + COALESCE(PCT_MOBILE, 0)) / 100)
                          / SUM(TXN_COUNT_TOTAL) * 100
                     ELSE 0 END                                              AS DIGITAL_TXN_PCT
            FROM {cfg.silver('STG_TXN_SUMMARY')}
            GROUP BY CUSTOMER_ID
        )
        SELECT
            *,
            CASE
                WHEN NET_CASH_FLOW >  AVG_TRANSACTION_SIZE * 5 THEN 'UP'
                WHEN NET_CASH_FLOW < -AVG_TRANSACTION_SIZE * 5 THEN 'DOWN'
                ELSE 'STABLE'
            END                                                              AS MONTHLY_SPEND_TREND,
            TOTAL_FEES                                                       AS FEE_INCOME,
            TOTAL_DEBIT_AMT * 0.02                                           AS INTEREST_INCOME,
            TOTAL_FEES + TOTAL_DEBIT_AMT * 0.02                              AS REVENUE_CONTRIBUTION
        FROM cust_txn
        """
    )


def spend_iqr_bounds(df: DataFrame) -> tuple[float, float]:
    """`PROC MEANS median= qrange=` on `TOTAL_DEBIT_AMT`.

    ``percentile`` interpolates exactly like SAS. Swap in
    ``df.approxQuantile("TOTAL_DEBIT_AMT", [0.25, 0.5, 0.75], 0.01)`` when the
    customer base grows large enough for an exact sort to hurt.
    """
    q1, median, q3 = df.select(
        F.expr("percentile(TOTAL_DEBIT_AMT, array(0.25, 0.5, 0.75))")
    ).collect()[0][0]
    return float(median), float(q3) - float(q1)


def build_txn_analytics(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Customer-level analytics with percentile ranking and IQR anomaly flag."""
    cust = aggregate_to_customer(spark, cfg).cache()
    median, iqr = spend_iqr_bounds(cust)

    return cust.select(
        F.col("CUSTOMER_ID"),
        F.lit(cfg.reporting_period).alias("REPORTING_PERIOD"),
        F.col("TOTAL_ACCOUNTS"),
        F.col("ACTIVE_ACCOUNTS"),
        F.col("TOTAL_TRANSACTIONS"),
        F.col("TOTAL_DEBIT_AMT"),
        F.col("TOTAL_CREDIT_AMT"),
        F.col("NET_CASH_FLOW"),
        F.col("AVG_TRANSACTION_SIZE"),
        F.col("MONTHLY_SPEND_TREND"),
        F.round(F.expr("cume_dist() OVER (ORDER BY TOTAL_DEBIT_AMT)") * 100, 2).alias(
            "SPEND_PERCENTILE"
        ),
        F.col("TOP_SPEND_CATEGORY"),
        F.col("DIGITAL_TXN_PCT"),
        F.col("FEE_INCOME"),
        F.col("INTEREST_INCOME"),
        F.col("REVENUE_CONTRIBUTION"),
        # Flag customers whose spend exceeds median + 3*IQR
        F.when(
            (F.col("TOTAL_DEBIT_AMT") > F.lit(median + 3 * iqr)) & F.lit(iqr > 0), "Y"
        )
        .otherwise("N")
        .alias("ANOMALY_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.lit(cfg.run_date).cast("date").alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    ensure_run_log(spark, cfg)
    target = cfg.gold(TARGET_TABLE)

    with step(spark, cfg, JOB_NAME, "ANALYTICS_AND_LOAD") as ctx:
        df = schemas.conform(build_txn_analytics(spark, cfg), schemas.GOLD_SCHEMAS[TARGET_TABLE])
        ctx["row_count"] = io.write_table(spark, cfg, df, target, merge_keys=["CUSTOMER_ID"])
        ctx["message"] = f"reporting_period={cfg.reporting_period}"
        rows = ctx["row_count"]

    validate_and_log(
        spark,
        cfg,
        JOB_NAME,
        target,
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS"],
    )
    return rows


# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = PipelineConfig.from_widgets(spark)
    logger = get_logger()
    log_event(logger, "job_start", run_id=cfg.run_id, job=JOB_NAME, config=cfg.describe())

    if not exit_if_skipped(cfg, "gold", spark):
        log_event(
            logger,
            "job_complete",
            run_id=cfg.run_id,
            job=JOB_NAME,
            table=TARGET_TABLE,
            row_count=run(spark, cfg),
        )
