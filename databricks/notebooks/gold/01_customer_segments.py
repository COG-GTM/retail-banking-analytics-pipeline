# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — `CUSTOMER_SEGMENTS`
# MAGIC
# MAGIC Port of `sas/01_sas_customer_segments.sas`.
# MAGIC
# MAGIC | SAS step | PySpark equivalent |
# MAGIC |---|---|
# MAGIC | `PROC SQL` extract of `STGDB.STG_CUSTOMER_360` where `CUSTOMER_STATUS='A'` | `spark.table(...).where(...)` |
# MAGIC | `DATA` step feature engineering | `feature_engineering()` |
# MAGIC | `PROC STDIZE method=std` | `VectorAssembler` + `StandardScaler(withMean=True, withStd=True)` |
# MAGIC | `PROC FASTCLUS maxclusters=5 maxiter=50 converge=0.001` | `pyspark.ml.clustering.KMeans(k=5, maxIter=50, tol=0.001, seed=42)` |
# MAGIC | cluster profile ordered by `avg(LOG_BALANCE) desc` + `_N_` labels | `label_clusters()` |
# MAGIC | `PROC SQL` scores/flags | `score_segments()` |
# MAGIC | `DELETE` + `PROC APPEND` truncate-and-load | `write.mode("overwrite")` |
# MAGIC
# MAGIC **Deviations from the legacy code** (both documented in `databricks/README.md`):
# MAGIC 1. `PROC FASTCLUS` seeds clusters with `replace=full`; Spark uses `k-means||`.
# MAGIC    With the same k / iterations / tolerance the partitions are equivalent in
# MAGIC    shape but individual borderline members can differ between engines.
# MAGIC 2. The SAS program reads the scores and flags off `WORK.CUST_CLUSTERED`, i.e.
# MAGIC    the **standardised** copies of `LOG_BALANCE`, `TENURE_MONTHS`, `ACCT_RATIO`.
# MAGIC    That is a bug in the legacy job (z-scores make `LIFETIME_VALUE_SCORE`
# MAGIC    negative for half the book); the raw features are used here, matching the
# MAGIC    DuckDB reference implementation and the documented business definition.

# COMMAND ----------

import os
import sys
from pathlib import Path

for _p in [os.getcwd(), *[str(p) for p in Path(os.getcwd()).parents]]:
    if os.path.isdir(os.path.join(_p, "shared")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
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
from shared.schemas import CUSTOMER_SEGMENTS, conform

JOB_NAME = "01_customer_segments"
TARGET_TABLE = "CUSTOMER_SEGMENTS"
MODEL_VERSION = "SEG_V3.2"

FEATURE_COLS = [
    "LOG_BALANCE",
    "TENURE_MONTHS",
    "CREDIT_UTILIZATION_PCT",
    "PRODUCT_BREADTH",
    "ACCT_RATIO",
    "AGE",
]

SEGMENT_LABELS = [
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
]

# COMMAND ----------


def feature_engineering(cust_360: DataFrame) -> DataFrame:
    """SAS STEP 2 — `WORK.CUST_FEATURES`."""
    product_breadth = sum(
        F.when(F.col(c) == "Y", F.lit(1.0)).otherwise(F.lit(0.0))
        for c in ("HAS_CHECKING", "HAS_SAVINGS", "HAS_CREDIT", "HAS_LOAN")
    ) / F.lit(4.0)

    tenure_group = (
        F.when(F.col("TENURE_MONTHS") < 12, "NEW (<1yr)")
        .when(F.col("TENURE_MONTHS") < 36, "DEVELOPING (1-3yr)")
        .when(F.col("TENURE_MONTHS") < 84, "ESTABLISHED (3-7yr)")
        .otherwise("LOYAL (7yr+)")
    )
    age_group = (
        F.when(F.col("AGE") < 25, "GEN_Z")
        .when(F.col("AGE") < 41, "MILLENNIAL")
        .when(F.col("AGE") < 57, "GEN_X")
        .when(F.col("AGE") < 76, "BOOMER")
        .otherwise("SILENT")
    )
    balance_tier = (
        F.when(F.col("TOTAL_BALANCE") < 1000, "LOW")
        .when(F.col("TOTAL_BALANCE") < 10000, "MODERATE")
        .when(F.col("TOTAL_BALANCE") < 100000, "AFFLUENT")
        .otherwise("HIGH_NET_WORTH")
    )

    return (
        cust_360.where(F.col("CUSTOMER_STATUS") == "A")
        .withColumn("PRODUCT_BREADTH", product_breadth)
        .withColumn("TENURE_GROUP", tenure_group)
        .withColumn("AGE_GROUP", age_group)
        .withColumn("BALANCE_TIER", balance_tier)
        .withColumn("DIGITAL_ADOPTION_SCORE", F.lit(0.0))
        .withColumn(
            "LOG_BALANCE",
            F.log(
                F.greatest(
                    F.coalesce(F.col("TOTAL_BALANCE").cast("double"), F.lit(0.0)), F.lit(1.0)
                )
            ),
        )
        .withColumn(
            "ACCT_RATIO",
            F.coalesce(F.col("NUM_ACTIVE_ACCOUNTS").cast("double"), F.lit(0.0))
            / F.greatest(F.coalesce(F.col("NUM_ACCOUNTS").cast("double"), F.lit(1.0)), F.lit(1.0)),
        )
    )


def cluster_customers(features: DataFrame, seed: int = 42) -> DataFrame:
    """SAS STEPS 3-4 — standardise the six features and fit k-means with k=5."""
    prepared = features
    for col in FEATURE_COLS:
        prepared = prepared.withColumn(
            f"_f_{col}", F.coalesce(F.col(col).cast("double"), F.lit(0.0))
        )

    assembled = VectorAssembler(
        inputCols=[f"_f_{c}" for c in FEATURE_COLS], outputCol="_features"
    ).transform(prepared)

    scaler = StandardScaler(
        inputCol="_features", outputCol="_scaled", withMean=True, withStd=True
    ).fit(assembled)
    scaled = scaler.transform(assembled)

    kmeans = KMeans(
        featuresCol="_scaled",
        predictionCol="CLUSTER",
        k=5,
        maxIter=50,
        tol=0.001,
        seed=seed,
    ).fit(scaled)

    clustered = kmeans.transform(scaled)
    return clustered.drop("_features", "_scaled", *[f"_f_{c}" for c in FEATURE_COLS])


def label_clusters(clustered: DataFrame) -> DataFrame:
    """SAS STEP 5 — rank clusters by average `LOG_BALANCE` descending and label them."""
    profiles = (
        clustered.groupBy("CLUSTER")
        .agg(F.avg("LOG_BALANCE").alias("AVG_BALANCE"))
        .orderBy(F.col("AVG_BALANCE").desc())
        .collect()
    )
    mapping = {int(row["CLUSTER"]): SEGMENT_LABELS[i] for i, row in enumerate(profiles)}

    segment_name = F.lit(None).cast("string")
    for cluster_id, label in mapping.items():
        segment_name = F.when(F.col("CLUSTER") == cluster_id, F.lit(label)).otherwise(segment_name)

    return clustered.withColumn("SEGMENT_NAME", segment_name).withColumn(
        "SUBSEGMENT_ID", F.lit(0)
    )


def score_segments(labelled: DataFrame) -> DataFrame:
    """SAS STEP 6 — value / engagement scores and next-best-action flags."""
    result = labelled.select(
        F.col("CUSTOMER_ID"),
        F.col("SEGMENT_NAME"),
        F.col("CLUSTER").alias("SEGMENT_ID"),
        F.col("SUBSEGMENT_ID"),
        F.round(
            F.col("LOG_BALANCE") * F.col("TENURE_MONTHS") * F.col("PRODUCT_BREADTH") * 10, 2
        ).alias("LIFETIME_VALUE_SCORE"),
        F.round(F.col("ACCT_RATIO") * 100, 2).alias("ENGAGEMENT_SCORE"),
        F.col("DIGITAL_ADOPTION_SCORE"),
        F.round(F.col("PRODUCT_BREADTH") * 100, 2).alias("PRODUCT_BREADTH_INDEX"),
        F.col("TENURE_GROUP"),
        F.col("AGE_GROUP"),
        F.col("BALANCE_TIER"),
        F.lit("").alias("CHANNEL_PREFERENCE"),
        F.when(
            (F.col("PRODUCT_BREADTH") < 0.50) & (F.col("ACCT_RATIO") >= 0.75), "Y"
        ).otherwise("N").alias("CROSS_SELL_FLAG"),
        F.when(
            (F.col("BALANCE_TIER") == "MODERATE") & (F.col("TENURE_GROUP") != "NEW (<1yr)"), "Y"
        ).otherwise("N").alias("UPSELL_FLAG"),
        F.when((F.col("ACCT_RATIO") < 0.50) & (F.col("TENURE_MONTHS") >= 60), "Y")
        .otherwise("N")
        .alias("RETENTION_RISK_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.current_date().alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return conform(result, CUSTOMER_SEGMENTS)


def build_customer_segments(cust_360: DataFrame, seed: int = 42) -> DataFrame:
    return score_segments(label_clusters(cluster_customers(feature_engineering(cust_360), seed)))


# COMMAND ----------

if in_databricks() and not exit_if_skipped("skip_gold", JOB_NAME):
    cfg = PipelineConfig.from_widgets()
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))
    target = cfg.gold(TARGET_TABLE)

    with audit.step("SEGMENTATION", f"-> {target}") as ctx:
        df = build_customer_segments(
            spark.table(cfg.silver("STG_CUSTOMER_360")), seed=get_int_param("model_seed", "42")
        )
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)
        ctx.row_count = validate_dataframe(
            spark.table(target),
            target,
            key_cols=["CUSTOMER_ID"],
            not_null=["CUSTOMER_ID", "SEGMENT_NAME", "SEGMENT_ID"],
            min_rows=get_int_param("min_gold_rows", "1000"),
        )
