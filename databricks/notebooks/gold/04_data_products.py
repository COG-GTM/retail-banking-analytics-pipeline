# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — `CUSTOMER_MASTER_PROFILE` (golden record)
# MAGIC
# MAGIC Port of `sas/04_sas_data_products.sas`.
# MAGIC
# MAGIC | SAS step | PySpark equivalent |
# MAGIC |---|---|
# MAGIC | `PROC SQL` extracts of the three data products | `spark.table(...).select(...)` |
# MAGIC | `PROC SORT` + `DATA ... MERGE BY CUSTOMER_ID; if _base;` | three `left` joins onto the base |
# MAGIC | `if not _seg / _txn / _risk then ...` defaults | `coalesce` per column group |
# MAGIC | `PROC SQL` distribution / completeness report | `quality_report()` |
# MAGIC | `DELETE` + `PROC APPEND` | `write.mode("overwrite")` |
# MAGIC | `COLLECT STATISTICS` | `OPTIMIZE ... ZORDER BY (CUSTOMER_ID)` |

# COMMAND ----------

import os
import sys
from pathlib import Path

for _p in [os.getcwd(), *[str(p) for p in Path(os.getcwd()).parents]]:
    if os.path.isdir(os.path.join(_p, "shared")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shared.audit import AUDIT_TABLE, AuditLogger
from shared.dq import validate_dataframe
from shared.runtime import (
    PipelineConfig,
    exit_if_skipped,
    get_int_param,
    get_param,
    in_databricks,
)
from shared.schemas import CUSTOMER_MASTER_PROFILE, conform

JOB_NAME = "04_data_products"
TARGET_TABLE = "CUSTOMER_MASTER_PROFILE"
MODEL_VERSION = "MASTER_V1.5"

# COMMAND ----------


def build_master_profile(
    cust_360: DataFrame,
    segments: DataFrame,
    txn_analytics: DataFrame,
    risk_scores: DataFrame,
) -> DataFrame:
    base = cust_360.where(F.col("CUSTOMER_STATUS") == "A").select(
        "CUSTOMER_ID",
        F.concat(F.trim("FIRST_NAME"), F.lit(" "), F.trim("LAST_NAME")).alias("FULL_NAME"),
        "AGE",
        "STATE_CODE",
        "CUSTOMER_SINCE",
        "TENURE_MONTHS",
        "CUSTOMER_STATUS",
        F.col("NUM_ACCOUNTS").alias("TOTAL_ACCOUNTS"),
        F.col("NUM_ACTIVE_ACCOUNTS").alias("ACTIVE_ACCOUNTS"),
        "TOTAL_BALANCE",
        "TOTAL_CREDIT_LIMIT",
        "CREDIT_UTILIZATION_PCT",
    )

    seg = segments.select(
        "CUSTOMER_ID",
        "SEGMENT_NAME",
        "LIFETIME_VALUE_SCORE",
        "ENGAGEMENT_SCORE",
        "CROSS_SELL_FLAG",
        "UPSELL_FLAG",
        "RETENTION_RISK_FLAG",
    )

    txn = txn_analytics.where(F.col("EFFECTIVE_DATE") == F.current_date()).select(
        "CUSTOMER_ID",
        F.col("TOTAL_TRANSACTIONS").alias("MONTHLY_TRANSACTIONS"),
        F.col("TOTAL_DEBIT_AMT").alias("MONTHLY_SPEND"),
        "NET_CASH_FLOW",
        "TOP_SPEND_CATEGORY",
        "DIGITAL_TXN_PCT",
    )

    risk = risk_scores.select(
        "CUSTOMER_ID",
        "COMPOSITE_RISK_SCORE",
        "RISK_TIER",
        "PROBABILITY_OF_DEFAULT",
        "WATCH_LIST_FLAG",
    )

    joined = (
        base.join(seg, ["CUSTOMER_ID"], "left")
        .join(txn, ["CUSTOMER_ID"], "left")
        .join(risk, ["CUSTOMER_ID"], "left")
    )

    result = joined.select(
        F.col("CUSTOMER_ID"),
        F.col("FULL_NAME"),
        F.col("AGE"),
        F.col("STATE_CODE"),
        F.col("CUSTOMER_SINCE"),
        F.col("TENURE_MONTHS"),
        F.col("CUSTOMER_STATUS"),
        F.coalesce(F.col("SEGMENT_NAME"), F.lit("UNCLASSIFIED")).alias("SEGMENT_NAME"),
        F.coalesce(F.col("LIFETIME_VALUE_SCORE"), F.lit(0)).alias("LIFETIME_VALUE_SCORE"),
        F.coalesce(F.col("ENGAGEMENT_SCORE"), F.lit(0)).alias("ENGAGEMENT_SCORE"),
        F.col("TOTAL_ACCOUNTS"),
        F.col("ACTIVE_ACCOUNTS"),
        F.col("TOTAL_BALANCE"),
        F.col("TOTAL_CREDIT_LIMIT"),
        F.col("CREDIT_UTILIZATION_PCT"),
        F.coalesce(F.col("MONTHLY_TRANSACTIONS"), F.lit(0)).alias("MONTHLY_TRANSACTIONS"),
        F.coalesce(F.col("MONTHLY_SPEND"), F.lit(0)).alias("MONTHLY_SPEND"),
        F.coalesce(F.col("NET_CASH_FLOW"), F.lit(0)).alias("NET_CASH_FLOW"),
        F.coalesce(F.col("TOP_SPEND_CATEGORY"), F.lit("")).alias("TOP_SPEND_CATEGORY"),
        F.coalesce(F.col("DIGITAL_TXN_PCT"), F.lit(0)).alias("DIGITAL_TXN_PCT"),
        F.col("COMPOSITE_RISK_SCORE"),
        F.coalesce(F.col("RISK_TIER"), F.lit("UNKNOWN")).alias("RISK_TIER"),
        F.col("PROBABILITY_OF_DEFAULT"),
        F.coalesce(F.col("WATCH_LIST_FLAG"), F.lit("N")).alias("WATCH_LIST_FLAG"),
        F.coalesce(F.col("CROSS_SELL_FLAG"), F.lit("N")).alias("CROSS_SELL_FLAG"),
        F.coalesce(F.col("UPSELL_FLAG"), F.lit("N")).alias("UPSELL_FLAG"),
        F.coalesce(F.col("RETENTION_RISK_FLAG"), F.lit("N")).alias("RETENTION_RISK_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.current_date().alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return conform(result, CUSTOMER_MASTER_PROFILE)


def quality_report(master: DataFrame) -> None:
    """SAS STEP 3 — segment / risk distributions and the completeness check."""
    master.groupBy("SEGMENT_NAME").agg(
        F.count(F.lit(1)).alias("N"), F.round(F.avg("LIFETIME_VALUE_SCORE"), 2).alias("AVG_LTV")
    ).orderBy(F.col("N").desc()).show(truncate=False)

    master.groupBy("RISK_TIER").agg(
        F.count(F.lit(1)).alias("N"), F.round(F.avg("COMPOSITE_RISK_SCORE"), 2).alias("AVG_SCORE")
    ).orderBy(F.col("AVG_SCORE").desc()).show(truncate=False)

    def flag_count(column: str, value: str) -> F.Column:
        return F.sum(F.when(F.col(column) == value, 1).otherwise(0))

    master.agg(
        F.count(F.lit(1)).alias("TOTAL"),
        F.sum(F.when(F.col("SEGMENT_NAME") != "UNCLASSIFIED", 1).otherwise(0)).alias("HAS_SEGMENT"),
        F.sum(F.when(F.col("MONTHLY_TRANSACTIONS") > 0, 1).otherwise(0)).alias("HAS_TXN"),
        F.sum(F.when(F.col("RISK_TIER") != "UNKNOWN", 1).otherwise(0)).alias("HAS_RISK_SCORE"),
        flag_count("CROSS_SELL_FLAG", "Y").alias("CROSS_SELL_ELIGIBLE"),
        flag_count("UPSELL_FLAG", "Y").alias("UPSELL_ELIGIBLE"),
        flag_count("RETENTION_RISK_FLAG", "Y").alias("RETENTION_AT_RISK"),
        flag_count("WATCH_LIST_FLAG", "Y").alias("ON_WATCH_LIST"),
    ).show(truncate=False)


# COMMAND ----------

if in_databricks() and not exit_if_skipped("skip_gold", JOB_NAME):
    cfg = PipelineConfig.from_widgets()
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))
    target = cfg.gold(TARGET_TABLE)

    with audit.step("MASTER_PROFILE", f"-> {target}") as ctx:
        df = build_master_profile(
            spark.table(cfg.silver("STG_CUSTOMER_360")),
            spark.table(cfg.gold("CUSTOMER_SEGMENTS")),
            spark.table(cfg.gold("TRANSACTION_ANALYTICS")),
            spark.table(cfg.gold("CUSTOMER_RISK_SCORES")),
        )
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)
        spark.sql(f"OPTIMIZE {target} ZORDER BY (CUSTOMER_ID)")

        quality_report(spark.table(target))
        ctx.row_count = validate_dataframe(
            spark.table(target),
            target,
            key_cols=["CUSTOMER_ID"],
            not_null=["CUSTOMER_ID", "FULL_NAME", "CUSTOMER_STATUS"],
            min_rows=get_int_param("min_gold_rows", "1000"),
        )
