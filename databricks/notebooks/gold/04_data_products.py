# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — `CUSTOMER_MASTER_PROFILE`
# MAGIC
# MAGIC Port of `sas/04_sas_data_products.sas` (model version `MASTER_V1.5`):
# MAGIC the golden record assembled from the customer base plus the three
# MAGIC upstream data products.
# MAGIC
# MAGIC | SAS step | Databricks equivalent |
# MAGIC |---|---|
# MAGIC | `proc sql` extracts of `BASE`/`SEGMENTS`/`TXN`/`RISK` | four CTEs over silver + gold |
# MAGIC | `proc sort` + `data ... merge ... by CUSTOMER_ID; if _base;` | three `LEFT JOIN`s onto the base (Spark joins do not need pre-sorting) |
# MAGIC | `if not _seg / _txn / _risk then do; ... end;` | `COALESCE` defaults, identical values |
# MAGIC | `proc sql` data-quality report titles | `quality_report` (logged, not printed to an ODS destination) |
# MAGIC | `proc sql delete` + `proc append force` | Delta overwrite/merge |
# MAGIC
# MAGIC The report intentionally aggregates only — no customer-identifying values
# MAGIC are written to the driver log.

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
from shared.audit import ensure_run_log, log_step, step  # noqa: E402
from shared.config import PipelineConfig, exit_if_skipped  # noqa: E402
from shared.logging_utils import get_logger, log_event  # noqa: E402
from shared.validation import validate_and_log  # noqa: E402

JOB_NAME = "04_master_profile"
TARGET_TABLE = "CUSTOMER_MASTER_PROFILE"
MODEL_VERSION = "MASTER_V1.5"

# COMMAND ----------


def build_master_profile(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Four-way golden-record assembly, keeping only active base customers."""
    run_date = cfg.run_date_literal
    return spark.sql(
        f"""
        WITH base AS (
            SELECT
                CUSTOMER_ID,
                TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME) AS FULL_NAME,
                AGE,
                STATE_CODE,
                CUSTOMER_SINCE,
                TENURE_MONTHS,
                CUSTOMER_STATUS,
                NUM_ACCOUNTS        AS TOTAL_ACCOUNTS,
                NUM_ACTIVE_ACCOUNTS AS ACTIVE_ACCOUNTS,
                TOTAL_BALANCE,
                TOTAL_CREDIT_LIMIT,
                CREDIT_UTILIZATION_PCT
            FROM {cfg.silver('STG_CUSTOMER_360')}
            WHERE CUSTOMER_STATUS = 'A'
        ),
        segments AS (
            SELECT
                CUSTOMER_ID,
                SEGMENT_NAME,
                LIFETIME_VALUE_SCORE,
                ENGAGEMENT_SCORE,
                CROSS_SELL_FLAG,
                UPSELL_FLAG,
                RETENTION_RISK_FLAG
            FROM {cfg.gold('CUSTOMER_SEGMENTS')}
        ),
        txn AS (
            /* Current reporting period only */
            SELECT
                CUSTOMER_ID,
                TOTAL_TRANSACTIONS AS MONTHLY_TRANSACTIONS,
                TOTAL_DEBIT_AMT    AS MONTHLY_SPEND,
                NET_CASH_FLOW,
                TOP_SPEND_CATEGORY,
                DIGITAL_TXN_PCT
            FROM {cfg.gold('TRANSACTION_ANALYTICS')}
            WHERE EFFECTIVE_DATE = {run_date}
        ),
        risk AS (
            SELECT
                CUSTOMER_ID,
                COMPOSITE_RISK_SCORE,
                RISK_TIER,
                PROBABILITY_OF_DEFAULT,
                WATCH_LIST_FLAG
            FROM {cfg.gold('CUSTOMER_RISK_SCORES')}
        )
        SELECT
            b.CUSTOMER_ID,
            b.FULL_NAME,
            b.AGE,
            b.STATE_CODE,
            b.CUSTOMER_SINCE,
            b.TENURE_MONTHS,
            b.CUSTOMER_STATUS,
            /* Segment attributes, defaulted when the customer is unsegmented */
            COALESCE(s.SEGMENT_NAME, 'UNCLASSIFIED')    AS SEGMENT_NAME,
            COALESCE(s.LIFETIME_VALUE_SCORE, 0)         AS LIFETIME_VALUE_SCORE,
            COALESCE(s.ENGAGEMENT_SCORE, 0)             AS ENGAGEMENT_SCORE,
            b.TOTAL_ACCOUNTS,
            b.ACTIVE_ACCOUNTS,
            b.TOTAL_BALANCE,
            b.TOTAL_CREDIT_LIMIT,
            b.CREDIT_UTILIZATION_PCT,
            /* Transaction attributes, defaulted when there is no activity */
            COALESCE(t.MONTHLY_TRANSACTIONS, 0)         AS MONTHLY_TRANSACTIONS,
            COALESCE(t.MONTHLY_SPEND, 0)                AS MONTHLY_SPEND,
            COALESCE(t.NET_CASH_FLOW, 0)                AS NET_CASH_FLOW,
            COALESCE(t.TOP_SPEND_CATEGORY, '')          AS TOP_SPEND_CATEGORY,
            COALESCE(t.DIGITAL_TXN_PCT, 0)              AS DIGITAL_TXN_PCT,
            /* Risk attributes; score and probability stay NULL when unscored */
            r.COMPOSITE_RISK_SCORE,
            COALESCE(r.RISK_TIER, 'UNKNOWN')            AS RISK_TIER,
            r.PROBABILITY_OF_DEFAULT,
            COALESCE(r.WATCH_LIST_FLAG, 'N')            AS WATCH_LIST_FLAG,
            COALESCE(s.CROSS_SELL_FLAG, 'N')            AS CROSS_SELL_FLAG,
            COALESCE(s.UPSELL_FLAG, 'N')                AS UPSELL_FLAG,
            COALESCE(s.RETENTION_RISK_FLAG, 'N')        AS RETENTION_RISK_FLAG,
            '{MODEL_VERSION}'                           AS MODEL_VERSION,
            {run_date}                                  AS EFFECTIVE_DATE,
            current_timestamp()                         AS LOAD_TS
        FROM base b
        LEFT JOIN segments s ON b.CUSTOMER_ID = s.CUSTOMER_ID
        LEFT JOIN txn      t ON b.CUSTOMER_ID = t.CUSTOMER_ID
        LEFT JOIN risk     r ON b.CUSTOMER_ID = r.CUSTOMER_ID
        """
    )


def quality_report(spark: SparkSession, cfg: PipelineConfig) -> dict[str, int]:
    """`proc sql` completeness check — aggregate counts only, never row data."""
    row = spark.sql(
        f"""
        SELECT
            COUNT(*)                                                            AS TOTAL,
            SUM(CASE WHEN SEGMENT_NAME <> 'UNCLASSIFIED' THEN 1 ELSE 0 END)     AS HAS_SEGMENT,
            SUM(CASE WHEN MONTHLY_TRANSACTIONS > 0 THEN 1 ELSE 0 END)           AS HAS_TXN,
            SUM(CASE WHEN RISK_TIER <> 'UNKNOWN' THEN 1 ELSE 0 END)             AS HAS_RISK_SCORE,
            SUM(CASE WHEN CROSS_SELL_FLAG = 'Y' THEN 1 ELSE 0 END)              AS CROSS_SELL_ELIGIBLE,
            SUM(CASE WHEN UPSELL_FLAG = 'Y' THEN 1 ELSE 0 END)                  AS UPSELL_ELIGIBLE,
            SUM(CASE WHEN RETENTION_RISK_FLAG = 'Y' THEN 1 ELSE 0 END)          AS RETENTION_AT_RISK,
            SUM(CASE WHEN WATCH_LIST_FLAG = 'Y' THEN 1 ELSE 0 END)              AS ON_WATCH_LIST
        FROM {cfg.gold(TARGET_TABLE)}
        """
    ).collect()[0]
    counts = {k: int(v or 0) for k, v in row.asDict().items()}
    log_step(
        spark,
        cfg,
        JOB_NAME,
        "COMPLETENESS_CHECK",
        "SUCCESS",
        message=" ".join(f"{k}={v}" for k, v in counts.items()),
        row_count=counts["TOTAL"],
    )
    return counts


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    ensure_run_log(spark, cfg)
    target = cfg.gold(TARGET_TABLE)

    with step(spark, cfg, JOB_NAME, "MERGE_AND_LOAD") as ctx:
        df = schemas.conform(build_master_profile(spark, cfg), schemas.GOLD_SCHEMAS[TARGET_TABLE])
        ctx["row_count"] = io.write_table(spark, cfg, df, target, merge_keys=["CUSTOMER_ID"])
        ctx["message"] = f"model_version={MODEL_VERSION}"
        rows = ctx["row_count"]

    validate_and_log(
        spark,
        cfg,
        JOB_NAME,
        target,
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID", "FULL_NAME", "SEGMENT_NAME"],
    )
    quality_report(spark, cfg)
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
