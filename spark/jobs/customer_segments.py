"""01 - Customer segmentation (PySpark port of ``01_sas_customer_segments.sas``).

SAS -> PySpark mapping
    PROC STDIZE method=std      -> pyspark.ml.feature.StandardScaler(withMean, withStd)
    PROC FASTCLUS maxclusters=5 -> pyspark.ml.clustering.KMeans(k=5)
    PROC SQL feature/label work -> DataFrame API

Reads  ETL_STAGING_DB.STG_CUSTOMER_360  (customer_status = 'A')
Writes DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS
"""
from __future__ import annotations

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from ..config import PipelineConfig
from ..logging_utils import PipelineAudit
from ..session import DataLayer, get_spark
from ..validation import enforce, validate_table

STEP = "01_CUSTOMER_SEG"

# Clustering features, matching the SAS PROC FASTCLUS VAR statement.
CLUSTER_FEATURES = [
    "log_balance",
    "tenure_months",
    "credit_utilization_pct",
    "product_breadth",
    "acct_ratio",
    "age",
]

# Cluster labels assigned to profiles ordered by average balance (desc),
# matching the SAS SEGMENT_LABELS data step.
SEGMENT_NAMES = [
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
]

OUTPUT_COLUMNS = [
    "customer_id", "segment_name", "segment_id", "subsegment_id",
    "lifetime_value_score", "engagement_score", "digital_adoption_score",
    "product_breadth_index", "tenure_group", "age_group", "balance_tier",
    "channel_preference", "cross_sell_flag", "upsell_flag", "retention_risk_flag",
    "model_version", "effective_date", "load_ts",
]


def _flag(condition) -> "F.Column":
    return F.when(condition, F.lit("Y")).otherwise(F.lit("N"))


def engineer_features(cust360: DataFrame) -> DataFrame:
    """Replicate the STEP 2 feature-engineering data step (un-standardized)."""
    active = cust360.where(F.col("customer_status") == "A")

    def is_y(col: str):
        return (F.col(col) == "Y").cast("double")

    product_breadth = (
        is_y("has_checking") + is_y("has_savings")
        + is_y("has_credit") + is_y("has_loan")
    ) / F.lit(4.0)

    tenure_group = (
        F.when(F.col("tenure_months") < 12, "NEW (<1yr)")
        .when(F.col("tenure_months") < 36, "DEVELOPING (1-3yr)")
        .when(F.col("tenure_months") < 84, "ESTABLISHED (3-7yr)")
        .otherwise("LOYAL (7yr+)")
    )
    age_group = (
        F.when(F.col("age") < 25, "GEN_Z")
        .when(F.col("age") < 41, "MILLENNIAL")
        .when(F.col("age") < 57, "GEN_X")
        .when(F.col("age") < 76, "BOOMER")
        .otherwise("SILENT")
    )
    balance_tier = (
        F.when(F.col("total_balance") < 1000, "LOW")
        .when(F.col("total_balance") < 10000, "MODERATE")
        .when(F.col("total_balance") < 100000, "AFFLUENT")
        .otherwise("HIGH_NET_WORTH")
    )

    return (
        active.withColumn("product_breadth", product_breadth)
        .withColumn("tenure_group", tenure_group)
        .withColumn("age_group", age_group)
        .withColumn("balance_tier", balance_tier)
        .withColumn("digital_adoption_score", F.lit(0.0))
        .withColumn("log_balance", F.log(F.greatest(F.col("total_balance"), F.lit(1.0))))
        .withColumn(
            "acct_ratio",
            F.col("num_active_accounts") / F.greatest(F.col("num_accounts"), F.lit(1.0)),
        )
    )


def cluster(features: DataFrame, config: PipelineConfig) -> DataFrame:
    """Standardize features and run k-means (PROC STDIZE + PROC FASTCLUS)."""
    assembler = VectorAssembler(
        inputCols=CLUSTER_FEATURES, outputCol="_features_raw", handleInvalid="skip"
    )
    scaler = StandardScaler(
        inputCol="_features_raw", outputCol="_features",
        withMean=True, withStd=True,
    )
    kmeans = KMeans(
        featuresCol="_features", predictionCol="_cluster",
        k=config.n_clusters, maxIter=config.kmeans_max_iter,
        tol=config.kmeans_tol, seed=config.kmeans_seed,
        distanceMeasure="euclidean",
    )
    assembled = assembler.transform(features)
    scaled_model = scaler.fit(assembled)
    scaled = scaled_model.transform(assembled)
    km_model = kmeans.fit(scaled)
    return km_model.transform(scaled)


def label_segments(clustered: DataFrame) -> DataFrame:
    """Assign business names to clusters ordered by avg balance (desc).

    Ordering by average *raw* ``log_balance`` is equivalent to ordering by the
    average *standardized* value used in the SAS CLUSTER_PROFILES step, since
    standardization is a monotonic linear transform.
    """
    profiles = clustered.groupBy("_cluster").agg(
        F.avg("log_balance").alias("_avg_balance")
    )
    ranked = profiles.withColumn(
        "_rank",
        F.row_number().over(Window.orderBy(F.col("_avg_balance").desc())),
    )
    name_col = None
    for idx, name in enumerate(SEGMENT_NAMES, start=1):
        cond = F.col("_rank") == idx
        name_col = F.when(cond, name) if name_col is None else name_col.when(cond, name)
    # Any additional clusters beyond the named list fall back to the last name.
    name_col = name_col.otherwise(SEGMENT_NAMES[-1])
    return ranked.withColumn("segment_name", name_col).select("_cluster", "segment_name")


def build_customer_segments(cust360: DataFrame, config: PipelineConfig) -> DataFrame:
    features = engineer_features(cust360)
    clustered = cluster(features, config)
    labels = label_segments(clustered)

    joined = clustered.join(labels, on="_cluster", how="inner")

    return joined.select(
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("segment_name"),
        # SAS SEGMENT_ID = FASTCLUS cluster id (1-based); Spark predictions are
        # 0-based, so shift by 1 to preserve the 1..k cluster-number semantics.
        (F.col("_cluster") + F.lit(1)).cast("int").alias("segment_id"),
        F.lit(0).cast("int").alias("subsegment_id"),
        F.round(
            F.col("log_balance") * F.col("tenure_months") * F.col("product_breadth") * 10, 2
        ).alias("lifetime_value_score"),
        F.round(F.col("acct_ratio") * 100, 2).alias("engagement_score"),
        F.col("digital_adoption_score"),
        F.round(F.col("product_breadth") * 100, 2).alias("product_breadth_index"),
        F.col("tenure_group"),
        F.col("age_group"),
        F.col("balance_tier"),
        F.lit("").alias("channel_preference"),
        _flag((F.col("product_breadth") < 0.50) & (F.col("acct_ratio") >= 0.75)).alias("cross_sell_flag"),
        _flag((F.col("balance_tier") == "MODERATE") & (F.col("tenure_group") != "NEW (<1yr)")).alias("upsell_flag"),
        _flag((F.col("acct_ratio") < 0.50) & (F.col("tenure_months") >= 60)).alias("retention_risk_flag"),
        F.lit(config.seg_model_version).alias("model_version"),
        F.lit(config.effective_date).alias("effective_date"),
        F.lit(config.run_ts).alias("load_ts"),
    ).select(*OUTPUT_COLUMNS)


def run(config: PipelineConfig) -> str:
    audit = PipelineAudit(run_id=config.run_id)
    spark = get_spark(config)
    data = DataLayer(spark, config)

    audit.log_step(step=STEP, status="START", msg="Beginning customer segmentation pipeline")
    cust360 = data.read_staging("stg_customer_360")

    result_df = build_customer_segments(cust360, config).cache()
    row_count = result_df.count()
    audit.log_step(step=STEP, status="SUCCESS", msg="Segment table built", rowcount=row_count)

    validation = validate_table(
        result_df, table="CUSTOMER_SEGMENTS",
        key_cols=["customer_id"],
        not_null=["customer_id", "segment_name", "segment_id"],
        min_rows=config.min_rows, audit=audit,
    )
    enforce(validation, "CUSTOMER_SEGMENTS", audit)

    audit.log_step(step=STEP, status="START", msg="Loading DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS")
    path = data.write_product(result_df, "customer_segments")
    audit.log_step(step=STEP, status="SUCCESS", msg=f"Pipeline complete -> {path}", rowcount=row_count)
    result_df.unpersist()
    return path


if __name__ == "__main__":
    run(PipelineConfig.from_env())
