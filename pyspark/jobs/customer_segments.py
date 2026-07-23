"""Customer behavioural segmentation (data product).

Port of ``sas/01_sas_customer_segments.sas``.

Reads   : Delta ``etl_staging.stg_customer_360``
Writes  : Delta ``data_products.customer_segments``

Legacy construct -> PySpark mapping
-----------------------------------
* ``PROC STDIZE method=std``           -> ``StandardScaler(withMean, withStd)``
* ``PROC FASTCLUS maxclusters=5``      -> ``KMeans(k=5, seed=SEED)`` in a
                                          Spark ML ``Pipeline``
* SAS ``DATA`` step feature build      -> Spark column expressions
* cluster-profile ordering + labels    -> rank clusters by avg log-balance desc,
                                          then map rank -> business segment name
* ``%log_step`` / ``%validate_table``  -> ``log_step`` / ``validate_table``
* ``DELETE`` + ``PROC APPEND``         -> idempotent Delta ``overwrite``
"""
from __future__ import annotations

import uuid

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    ShortType,
)

from common.audit import init_audit, log_step
from common.config import Config
from common.validation import validate_table

JOB_NAME = "01_customer_segments"
MODEL_VERSION = "SEG_V3.2"
NUM_SEGMENTS = 5
SEED = 42

# Numeric features fed to standardisation + k-means (order preserved from SAS).
CLUSTER_FEATURES = [
    "log_balance",
    "tenure_months",
    "credit_utilization_pct",
    "product_breadth",
    "acct_ratio",
    "age",
]

# rank (1-based, by descending avg log-balance) -> business segment name.
SEGMENT_LABELS = {
    1: "PREMIUM_WEALTH",
    2: "ENGAGED_MAINSTREAM",
    3: "GROWING_DIGITAL",
    4: "CREDIT_DEPENDENT",
    5: "VALUE_BASIC",
}


def _engineer_features(df: DataFrame) -> DataFrame:
    """Reproduce the SAS ``WORK.CUST_FEATURES`` data step."""
    product_breadth = (
        (F.col("has_checking") == F.lit("Y")).cast("double")
        + (F.col("has_savings") == F.lit("Y")).cast("double")
        + (F.col("has_credit") == F.lit("Y")).cast("double")
        + (F.col("has_loan") == F.lit("Y")).cast("double")
    ) / F.lit(4.0)

    tenure_group = (
        F.when(F.col("tenure_months") < 12, F.lit("NEW (<1yr)"))
        .when(F.col("tenure_months") < 36, F.lit("DEVELOPING (1-3yr)"))
        .when(F.col("tenure_months") < 84, F.lit("ESTABLISHED (3-7yr)"))
        .otherwise(F.lit("LOYAL (7yr+)"))
    )

    age_group = (
        F.when(F.col("age") < 25, F.lit("GEN_Z"))
        .when(F.col("age") < 41, F.lit("MILLENNIAL"))
        .when(F.col("age") < 57, F.lit("GEN_X"))
        .when(F.col("age") < 76, F.lit("BOOMER"))
        .otherwise(F.lit("SILENT"))
    )

    balance_tier = (
        F.when(F.col("total_balance") < 1000, F.lit("LOW"))
        .when(F.col("total_balance") < 10000, F.lit("MODERATE"))
        .when(F.col("total_balance") < 100000, F.lit("AFFLUENT"))
        .otherwise(F.lit("HIGH_NET_WORTH"))
    )

    return (
        df.withColumn("product_breadth", product_breadth)
        .withColumn("tenure_group", tenure_group)
        .withColumn("age_group", age_group)
        .withColumn("balance_tier", balance_tier)
        .withColumn("digital_adoption_score", F.lit(0.0))
        .withColumn(
            "log_balance",
            F.log(F.greatest(F.col("total_balance"), F.lit(1.0))),
        )
        .withColumn(
            "acct_ratio",
            F.col("num_active_accounts")
            / F.greatest(F.col("num_accounts"), F.lit(1)),
        )
    )


def _fit_clusters(features_df: DataFrame) -> DataFrame:
    """Standardise features and assign a k-means cluster to each customer."""
    assembler = VectorAssembler(
        inputCols=CLUSTER_FEATURES,
        outputCol="_raw_features",
    )
    scaler = StandardScaler(
        inputCol="_raw_features",
        outputCol="_scaled_features",
        withMean=True,
        withStd=True,
    )
    kmeans = KMeans(
        featuresCol="_scaled_features",
        predictionCol="cluster",
        k=NUM_SEGMENTS,
        maxIter=50,
        tol=1e-3,
        seed=SEED,
    )
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(features_df)
    return model.transform(features_df)


def _label_segments(clustered_df: DataFrame) -> DataFrame:
    """Rank clusters by avg log-balance (desc) and map rank -> segment name.

    Mirrors the SAS ``CLUSTER_PROFILES`` ordering: cluster with the highest
    average balance becomes ``PREMIUM_WEALTH`` (rank 1), etc. ``segment_id`` is
    the deterministic 1-based rank so it is stable across runs regardless of the
    arbitrary raw cluster index emitted by k-means.
    """
    profiles = (
        clustered_df.groupBy("cluster")
        .agg(F.avg("log_balance").alias("avg_balance"))
        .orderBy(F.col("avg_balance").desc(), F.col("cluster").asc())
        .collect()
    )

    mapping = [
        (int(row["cluster"]), rank, SEGMENT_LABELS[rank])
        for rank, row in enumerate(profiles, start=1)
    ]
    map_df = clustered_df.sparkSession.createDataFrame(
        mapping, schema=["cluster", "segment_id", "segment_name"]
    )
    return clustered_df.join(F.broadcast(map_df), on="cluster", how="inner")


def _build_output(labelled_df: DataFrame, cfg: Config) -> DataFrame:
    """Assemble the CUSTOMER_SEGMENTS data-product columns (matches DDL)."""
    cross_sell = F.when(
        (F.col("product_breadth") < 0.50) & (F.col("acct_ratio") >= 0.75),
        F.lit("Y"),
    ).otherwise(F.lit("N"))

    upsell = F.when(
        (F.col("balance_tier") == "MODERATE")
        & (F.col("tenure_group") != "NEW (<1yr)"),
        F.lit("Y"),
    ).otherwise(F.lit("N"))

    retention_risk = F.when(
        (F.col("acct_ratio") < 0.50) & (F.col("tenure_months") >= 60),
        F.lit("Y"),
    ).otherwise(F.lit("N"))

    lifetime_value = (
        F.col("log_balance") * F.col("tenure_months") * F.col("product_breadth")
        * F.lit(10.0)
    )

    return (
        labelled_df.select(
            F.col("customer_id").cast(LongType()).alias("customer_id"),
            F.col("segment_name").cast("string").alias("segment_name"),
            F.col("segment_id").cast(ShortType()).alias("segment_id"),
            F.lit(0).cast(ShortType()).alias("subsegment_id"),
            F.round(lifetime_value, 2)
            .cast(DecimalType(10, 2))
            .alias("lifetime_value_score"),
            F.round(F.col("acct_ratio") * 100, 2)
            .cast(DecimalType(5, 2))
            .alias("engagement_score"),
            F.round(F.col("digital_adoption_score"), 2)
            .cast(DecimalType(5, 2))
            .alias("digital_adoption_score"),
            F.round(F.col("product_breadth") * 100, 2)
            .cast(DecimalType(5, 2))
            .alias("product_breadth_index"),
            F.col("tenure_group").cast("string").alias("tenure_group"),
            F.col("age_group").cast("string").alias("age_group"),
            F.col("balance_tier").cast("string").alias("balance_tier"),
            F.lit("").cast("string").alias("channel_preference"),
            cross_sell.alias("cross_sell_flag"),
            upsell.alias("upsell_flag"),
            retention_risk.alias("retention_risk_flag"),
            F.lit(MODEL_VERSION).cast("string").alias("model_version"),
            F.to_date(F.lit(cfg.run_date)).alias("effective_date"),
            F.current_timestamp().alias("load_ts"),
        )
    )


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    """Build and persist the ``data_products.customer_segments`` data product."""
    run_id = str(uuid.uuid4())
    init_audit(spark, cfg)
    log_step(spark, cfg, run_id, JOB_NAME, "extract", "START",
             message="Beginning customer segmentation pipeline")

    source_table = cfg.table(cfg.schema_stg, "stg_customer_360")
    cust_360 = (
        spark.read.table(source_table)
        .filter(F.col("customer_status") == F.lit("A"))
    )

    features = _engineer_features(cust_360)
    features = validate_table(
        features, min_rows=1, not_null_cols=["customer_id"]
    )
    log_step(spark, cfg, run_id, JOB_NAME, "features", "SUCCESS",
             row_count=features.count(), message="Engineered features")

    clustered = _fit_clusters(features)
    labelled = _label_segments(clustered)
    output = _build_output(labelled, cfg)

    output = validate_table(
        output,
        min_rows=1,
        not_null_cols=["customer_id", "segment_name", "segment_id"],
        unique_keys=["customer_id"],
    )

    target_table = cfg.table(cfg.schema_dp, "customer_segments")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_dp}")
    (
        output.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    written = spark.read.table(target_table)
    log_step(spark, cfg, run_id, JOB_NAME, "load", "SUCCESS",
             row_count=written.count(),
             message=f"Loaded {target_table}")
    return written


__all__ = ["run", "JOB_NAME", "MODEL_VERSION", "NUM_SEGMENTS", "SEGMENT_LABELS"]
