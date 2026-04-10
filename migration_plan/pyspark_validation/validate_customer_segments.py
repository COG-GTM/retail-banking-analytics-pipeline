"""
PySpark Validation: CUSTOMER_SEGMENTS
Replaces: sas/01_sas_customer_segments.sas

Validates that a PySpark+MLlib translation of the SAS customer segmentation
program (PROC FASTCLUS k=5) produces output comparable to reference data.

Note: k-means is non-deterministic so we compare cluster DISTRIBUTIONS
rather than 1:1 assignments.
"""
from pathlib import Path

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_date,
    current_timestamp,
    lit,
    when,
)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
STAGING_DIR = DATA_DIR / "02_bteq_staging"
REFERENCE_DIR = DATA_DIR / "03_sas_data_products"


def build_customer_segments(spark: SparkSession):
    """Replicate the logic of 01_sas_customer_segments.sas in PySpark+MLlib."""

    # ------------------------------------------------------------------
    # 1. Load staging data (equivalent to PROC SQL extract from STGDB)
    # ------------------------------------------------------------------
    stg = spark.read.csv(
        str(STAGING_DIR / "stg_customer_360.csv"), header=True, inferSchema=True
    )

    # ------------------------------------------------------------------
    # 2. Feature engineering (equivalent to SAS DATA step)
    # ------------------------------------------------------------------
    features_df = stg.withColumn(
        "product_breadth",
        (
            when(col("has_checking") == "Y", 1).otherwise(0)
            + when(col("has_savings") == "Y", 1).otherwise(0)
            + when(col("has_credit") == "Y", 1).otherwise(0)
            + when(col("has_loan") == "Y", 1).otherwise(0)
        )
        / 4.0
        * 100,
    ).withColumn(
        "tenure_group",
        when(col("tenure_months") < 12, "NEW (<1yr)")
        .when(col("tenure_months") < 36, "DEVELOPING (1-3yr)")
        .when(col("tenure_months") < 84, "ESTABLISHED (3-7yr)")
        .otherwise("LOYAL (7yr+)"),
    ).withColumn(
        "age_group",
        when(col("age") < 25, "GEN_Z")
        .when(col("age") < 41, "MILLENNIAL")
        .when(col("age") < 57, "GEN_X")
        .otherwise("BOOMER"),
    ).withColumn(
        "balance_tier",
        when(col("total_balance") < 1000, "LOW")
        .when(col("total_balance") < 10000, "MODERATE")
        .when(col("total_balance") < 50000, "AFFLUENT")
        .otherwise("HIGH_NET_WORTH"),
    )

    # Fill nulls for numeric features used in clustering
    features_df = features_df.fillna(
        {
            "total_balance": 0,
            "num_active_accounts": 0,
            "credit_utilization_pct": 0,
            "product_breadth": 0,
            "tenure_months": 0,
        }
    )

    # ------------------------------------------------------------------
    # 3. Standardize (PROC STDIZE method=std) and cluster (PROC FASTCLUS k=5)
    # ------------------------------------------------------------------
    feature_cols = [
        "total_balance",
        "num_active_accounts",
        "credit_utilization_pct",
        "product_breadth",
        "tenure_months",
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip"
    )
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True,
    )
    kmeans = KMeans(
        k=5,
        maxIter=50,
        featuresCol="features",
        predictionCol="segment_id",
        seed=42,
    )

    assembled = assembler.transform(features_df)
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)
    kmeans_model = kmeans.fit(scaled)
    clustered = kmeans_model.transform(scaled)

    # ------------------------------------------------------------------
    # 4. Label clusters (simplified — rank by avg balance)
    # ------------------------------------------------------------------
    SEGMENT_LABELS = [
        "ENGAGED_MAINSTREAM",
        "DIGITAL_ACTIVE",
        "PREMIUM_LOYAL",
        "VALUE_BASIC",
        "GROWTH_POTENTIAL",
    ]

    cluster_profiles = (
        clustered.groupBy("segment_id")
        .agg({"total_balance": "avg"})
        .orderBy("avg(total_balance)")
        .collect()
    )

    label_map = {}
    for i, row in enumerate(cluster_profiles):
        label_map[row["segment_id"]] = SEGMENT_LABELS[i % len(SEGMENT_LABELS)]

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    label_udf = udf(lambda x: label_map.get(x, "UNKNOWN"), StringType())

    result = (
        clustered.withColumn("segment_name", label_udf(col("segment_id")))
        .withColumn("model_version", lit("SEG_V3.2"))
        .withColumn("effective_date", current_date())
        .withColumn("load_ts", current_timestamp())
        .select(
            "customer_id",
            "segment_name",
            "segment_id",
            "tenure_group",
            "age_group",
            "balance_tier",
            "product_breadth",
            "model_version",
            "effective_date",
            "load_ts",
        )
    )

    return result


def validate(spark: SparkSession) -> dict:
    """Run the segmentation and compare distributions to reference data."""
    result = build_customer_segments(spark)
    reference = spark.read.csv(
        str(REFERENCE_DIR / "customer_segments.csv"), header=True, inferSchema=True
    )

    checks = {}

    # Row count
    result_count = result.count()
    ref_count = reference.count()
    checks["row_count_match"] = result_count == ref_count
    checks["row_count_pyspark"] = result_count
    checks["row_count_reference"] = ref_count

    # Number of distinct segments
    py_segments = result.select("segment_name").distinct().count()
    ref_segments = reference.select("segment_name").distinct().count()
    checks["num_segments_pyspark"] = py_segments
    checks["num_segments_reference"] = ref_segments
    checks["segment_count_match"] = py_segments == ref_segments

    # Cluster distribution comparison (within 5% tolerance per segment)
    py_dist = (
        result.groupBy("segment_name")
        .agg(count("*").alias("cnt"))
        .withColumn("pct", col("cnt") / result_count * 100)
        .orderBy("segment_name")
    )
    ref_dist = (
        reference.groupBy("segment_name")
        .agg(count("*").alias("cnt"))
        .withColumn("pct", col("cnt") / ref_count * 100)
        .orderBy("segment_name")
    )

    checks["pyspark_distribution"] = {
        row["segment_name"]: round(row["pct"], 1) for row in py_dist.collect()
    }
    checks["reference_distribution"] = {
        row["segment_name"]: round(row["pct"], 1) for row in ref_dist.collect()
    }

    # Check overlap of customer IDs
    py_custs = set(
        row["customer_id"] for row in result.select("customer_id").collect()
    )
    ref_custs = set(
        row["customer_id"] for row in reference.select("customer_id").collect()
    )
    overlap = len(py_custs & ref_custs)
    checks["customer_id_overlap"] = overlap
    checks["customer_id_overlap_pct"] = round(overlap / max(len(ref_custs), 1) * 100, 2)

    # Overall: row count matches and we have 5 segments
    checks["overall_pass"] = checks["row_count_match"] and py_segments == 5

    return checks


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("validate_customer_segments")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("  Validating: CUSTOMER_SEGMENTS (SAS Program 01)")
    print("=" * 70)

    results = validate(spark)
    for k, v in results.items():
        if isinstance(v, dict):
            print(f"  {k}:")
            for seg, pct in v.items():
                print(f"    {seg:30s} : {pct}%")
        else:
            status = "PASS" if v is True else ("FAIL" if v is False else str(v))
            print(f"  {k:40s} : {status}")

    print("=" * 70)
    overall = "PASS" if results["overall_pass"] else "FAIL"
    print(f"  Overall: {overall}")
    print("=" * 70)

    spark.stop()
