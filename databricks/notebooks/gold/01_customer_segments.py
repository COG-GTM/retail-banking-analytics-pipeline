# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — `CUSTOMER_SEGMENTS`
# MAGIC
# MAGIC Port of `sas/01_sas_customer_segments.sas` (model version `SEG_V3.2`).
# MAGIC
# MAGIC | SAS step | Databricks equivalent |
# MAGIC |---|---|
# MAGIC | `proc sql` extract from `STGDB.STG_CUSTOMER_360` | `spark.sql` on the silver table |
# MAGIC | `data WORK.CUST_FEATURES` (breadth, tenure/age/balance groups, `log`, ratio) | `build_features` |
# MAGIC | `proc stdize method=std` | `pyspark.ml.feature.StandardScaler(withMean, withStd)` |
# MAGIC | `proc fastclus maxclusters=5 maxiter=50 converge=0.001` | `pyspark.ml.clustering.KMeans(k=5, maxIter=50, tol=0.001)` |
# MAGIC | cluster profile ordered by `avg(LOG_BALANCE) desc` -> labels | `label_clusters` (same ordering, same five labels) |
# MAGIC | `proc sql delete` + `proc append force` into `PRODDB.CUSTOMER_SEGMENTS` | Delta overwrite/merge |
# MAGIC
# MAGIC **Fidelity note.** `PROC STDIZE` overwrites its input columns, so the SAS
# MAGIC scores technically read standardised values while the certified outputs (and
# MAGIC the DuckDB reference in `local/duckdb/run_demo.py`) are computed from the raw
# MAGIC feature values. This port keeps the raw values — standardisation is applied
# MAGIC only to the clustering input — which reproduces the published data product.

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

from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from shared import io, schemas  # noqa: E402
from shared.audit import ensure_run_log, step  # noqa: E402
from shared.config import PipelineConfig, exit_if_skipped  # noqa: E402
from shared.logging_utils import get_logger, log_event  # noqa: E402
from shared.modeling import kmeans_cluster, standardize  # noqa: E402
from shared.validation import validate_and_log  # noqa: E402

JOB_NAME = "01_customer_segments"
TARGET_TABLE = "CUSTOMER_SEGMENTS"
MODEL_VERSION = "SEG_V3.2"

CLUSTER_FEATURES = [
    "LOG_BALANCE",
    "TENURE_MONTHS",
    "CREDIT_UTILIZATION_PCT",
    "PRODUCT_BREADTH",
    "ACCT_RATIO",
    "AGE",
]

# Labels applied to clusters ordered by descending average balance.
SEGMENT_LABELS = [
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
]

# COMMAND ----------


def build_features(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """`data WORK.CUST_FEATURES`: derived attributes for active customers."""
    return spark.sql(
        f"""
        SELECT
            CUSTOMER_ID,
            AGE,
            TENURE_MONTHS,
            CUSTOMER_STATUS,
            SEGMENT_CODE,
            STATE_CODE,
            NUM_ACCOUNTS,
            NUM_ACTIVE_ACCOUNTS,
            TOTAL_BALANCE,
            TOTAL_CREDIT_LIMIT,
            CREDIT_UTILIZATION_PCT,
            /* Product breadth index: proportion of product types held */
            (CASE WHEN HAS_CHECKING = 'Y' THEN 1 ELSE 0 END
             + CASE WHEN HAS_SAVINGS = 'Y' THEN 1 ELSE 0 END
             + CASE WHEN HAS_CREDIT  = 'Y' THEN 1 ELSE 0 END
             + CASE WHEN HAS_LOAN    = 'Y' THEN 1 ELSE 0 END) / 4.0          AS PRODUCT_BREADTH,
            CASE
                WHEN TENURE_MONTHS < 12 THEN 'NEW (<1yr)'
                WHEN TENURE_MONTHS < 36 THEN 'DEVELOPING (1-3yr)'
                WHEN TENURE_MONTHS < 84 THEN 'ESTABLISHED (3-7yr)'
                ELSE 'LOYAL (7yr+)'
            END                                                              AS TENURE_GROUP,
            CASE
                WHEN AGE < 25 THEN 'GEN_Z'
                WHEN AGE < 41 THEN 'MILLENNIAL'
                WHEN AGE < 57 THEN 'GEN_X'
                WHEN AGE < 76 THEN 'BOOMER'
                ELSE 'SILENT'
            END                                                              AS AGE_GROUP,
            CASE
                WHEN TOTAL_BALANCE < 1000   THEN 'LOW'
                WHEN TOTAL_BALANCE < 10000  THEN 'MODERATE'
                WHEN TOTAL_BALANCE < 100000 THEN 'AFFLUENT'
                ELSE 'HIGH_NET_WORTH'
            END                                                              AS BALANCE_TIER,
            /* Digital adoption proxy (placeholder in the SAS model) */
            CAST(0 AS DOUBLE)                                                AS DIGITAL_ADOPTION_SCORE,
            ln(greatest(COALESCE(TOTAL_BALANCE, 0), 1))                      AS LOG_BALANCE,
            COALESCE(NUM_ACTIVE_ACCOUNTS, 0) / greatest(COALESCE(NUM_ACCOUNTS, 1), 1) AS ACCT_RATIO
        FROM {cfg.silver('STG_CUSTOMER_360')}
        WHERE CUSTOMER_STATUS = 'A'
        """
    )


def label_clusters(clustered: DataFrame) -> DataFrame:
    """Name clusters by descending average balance (SAS `WORK.SEGMENT_LABELS`)."""
    ordering = Window.orderBy(F.col("AVG_BALANCE").desc())
    labels = (
        clustered.groupBy("CLUSTER")
        .agg(F.avg("LOG_BALANCE").alias("AVG_BALANCE"))
        .withColumn("_rank", F.row_number().over(ordering))
        .withColumn(
            "SEGMENT_NAME",
            F.element_at(F.array(*[F.lit(label) for label in SEGMENT_LABELS]), F.col("_rank")),
        )
        .withColumn("SUBSEGMENT_ID", F.lit(0))
        .select("CLUSTER", "SEGMENT_NAME", "SUBSEGMENT_ID")
    )
    return clustered.join(labels, on="CLUSTER", how="inner")


def build_customer_segments(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Full segmentation model: features -> standardise -> k-means -> scores."""
    features = build_features(spark, cfg).cache()
    clustered = kmeans_cluster(standardize(features, CLUSTER_FEATURES), k=5, max_iter=50, tol=0.001)
    labelled = label_clusters(clustered)

    return labelled.select(
        F.col("CUSTOMER_ID"),
        F.col("SEGMENT_NAME"),
        F.col("CLUSTER").alias("SEGMENT_ID"),
        F.col("SUBSEGMENT_ID"),
        # Lifetime value heuristic: balance * tenure * breadth
        F.round(F.col("LOG_BALANCE") * F.col("TENURE_MONTHS") * F.col("PRODUCT_BREADTH") * 10, 2).alias(
            "LIFETIME_VALUE_SCORE"
        ),
        F.round(F.col("ACCT_RATIO") * 100, 2).alias("ENGAGEMENT_SCORE"),
        F.col("DIGITAL_ADOPTION_SCORE"),
        F.round(F.col("PRODUCT_BREADTH") * 100, 2).alias("PRODUCT_BREADTH_INDEX"),
        F.col("TENURE_GROUP"),
        F.col("AGE_GROUP"),
        F.col("BALANCE_TIER"),
        # Channel preference (placeholder; enriched later by txn analytics)
        F.lit("").alias("CHANNEL_PREFERENCE"),
        # Cross-sell if low product breadth but good engagement
        F.when((F.col("PRODUCT_BREADTH") < 0.50) & (F.col("ACCT_RATIO") >= 0.75), "Y")
        .otherwise("N")
        .alias("CROSS_SELL_FLAG"),
        # Upsell if moderate balance with room to grow
        F.when(
            (F.col("BALANCE_TIER") == "MODERATE") & (F.col("TENURE_GROUP") != "NEW (<1yr)"), "Y"
        )
        .otherwise("N")
        .alias("UPSELL_FLAG"),
        # Retention risk if low engagement and long tenure
        F.when((F.col("ACCT_RATIO") < 0.50) & (F.col("TENURE_MONTHS") >= 60), "Y")
        .otherwise("N")
        .alias("RETENTION_RISK_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.lit(cfg.run_date).cast("date").alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    ensure_run_log(spark, cfg)
    target = cfg.gold(TARGET_TABLE)

    with step(spark, cfg, JOB_NAME, "SEGMENT_AND_LOAD") as ctx:
        df = schemas.conform(
            build_customer_segments(spark, cfg), schemas.GOLD_SCHEMAS[TARGET_TABLE]
        )
        ctx["row_count"] = io.write_table(spark, cfg, df, target, merge_keys=["CUSTOMER_ID"])
        ctx["message"] = f"model_version={MODEL_VERSION}"
        rows = ctx["row_count"]

    validate_and_log(
        spark,
        cfg,
        JOB_NAME,
        target,
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID", "SEGMENT_NAME", "SEGMENT_ID"],
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
