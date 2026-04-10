"""
PySpark Validation: CUSTOMER_MASTER_PROFILE
Replaces: sas/04_sas_data_products.sas

Validates that a PySpark translation of the SAS golden-record assembly
program (DATA step MERGE with IN= variables) produces output comparable
to reference data.
"""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_date,
    current_timestamp,
    lit,
    sum as spark_sum,
    when,
)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
STAGING_DIR = DATA_DIR / "02_bteq_staging"
PRODUCT_DIR = DATA_DIR / "03_sas_data_products"
REFERENCE_DIR = DATA_DIR / "03_sas_data_products"


def build_data_products(spark: SparkSession):
    """Replicate the logic of 04_sas_data_products.sas in PySpark.

    This is the 4-way MERGE that creates CUSTOMER_MASTER_PROFILE.
    """

    # ------------------------------------------------------------------
    # 1. Load upstream data products
    # ------------------------------------------------------------------
    stg_360 = spark.read.csv(
        str(STAGING_DIR / "stg_customer_360.csv"), header=True, inferSchema=True
    )
    segments = spark.read.csv(
        str(PRODUCT_DIR / "customer_segments.csv"), header=True, inferSchema=True
    )
    txn_analytics = spark.read.csv(
        str(PRODUCT_DIR / "transaction_analytics.csv"), header=True, inferSchema=True
    )
    risk_scores = spark.read.csv(
        str(PRODUCT_DIR / "customer_risk_scores.csv"), header=True, inferSchema=True
    )

    # ------------------------------------------------------------------
    # 2. SAS MERGE with IN= variables → PySpark left joins + coalesce
    # ------------------------------------------------------------------
    # Base: stg_360 is the driver (if _b)
    master = stg_360.select(
        "customer_id",
        "first_name",
        "last_name",
        "customer_status",
        "segment_code",
        "total_balance",
        "num_accounts",
        "tenure_months",
        "age",
    )

    # Join segments
    master = master.join(
        segments.select(
            col("customer_id").alias("seg_cust_id"),
            "segment_name",
            "segment_id",
            "balance_tier",
        ),
        col("customer_id") == col("seg_cust_id"),
        "left",
    ).drop("seg_cust_id")

    # Default: if not joined from segments
    master = master.withColumn(
        "segment_name", coalesce(col("segment_name"), lit("UNCLASSIFIED"))
    ).withColumn("segment_id", coalesce(col("segment_id"), lit(-1)))

    # Join txn analytics
    # Reference transaction_analytics.csv columns:
    #   customer_id, total_accounts, active_accounts, total_transactions,
    #   total_debit_amt, total_credit_amt, total_fees, top_spend_category,
    #   net_cash_flow, avg_transaction_size, digital_txn_pct,
    #   monthly_spend_trend, fee_income, interest_income, revenue_contribution,
    #   spend_percentile, anomaly_flag
    master = master.join(
        txn_analytics.select(
            col("customer_id").alias("txn_cust_id"),
            "total_transactions",
            "spend_percentile",
            "anomaly_flag",
            "monthly_spend_trend",
            "digital_txn_pct",
            "net_cash_flow",
            "top_spend_category",
        ),
        col("customer_id") == col("txn_cust_id"),
        "left",
    ).drop("txn_cust_id")

    # Default: if not joined from txn_analytics
    master = master.withColumn(
        "anomaly_flag", coalesce(col("anomaly_flag"), lit("N"))
    ).withColumn(
        "monthly_spend_trend", coalesce(col("monthly_spend_trend"), lit("UNKNOWN"))
    )

    # Join risk scores
    master = master.join(
        risk_scores.select(
            col("customer_id").alias("risk_cust_id"),
            "composite_risk_score",
            "risk_tier",
        ),
        col("customer_id") == col("risk_cust_id"),
        "left",
    ).drop("risk_cust_id")

    # Default: if not joined from risk_scores
    master = master.withColumn(
        "risk_tier", coalesce(col("risk_tier"), lit("UNSCORED"))
    ).withColumn(
        "composite_risk_score", coalesce(col("composite_risk_score"), lit(0.0))
    )

    # ------------------------------------------------------------------
    # 3. Finalize
    # ------------------------------------------------------------------
    result = master.withColumn("profile_date", current_date()).withColumn(
        "load_ts", current_timestamp()
    )

    return result


def validate(spark: SparkSession) -> dict:
    """Run the merge and compare to reference data."""
    result = build_data_products(spark)
    reference = spark.read.csv(
        str(REFERENCE_DIR / "customer_master_profile.csv"),
        header=True,
        inferSchema=True,
    )

    checks = {}

    # Row count
    result_count = result.count()
    ref_count = reference.count()
    checks["row_count_match"] = result_count == ref_count
    checks["row_count_pyspark"] = result_count
    checks["row_count_reference"] = ref_count

    # Column count
    checks["columns_pyspark"] = len(result.columns)
    checks["columns_reference"] = len(reference.columns)

    # Customer overlap
    py_custs = set(row["customer_id"] for row in result.select("customer_id").collect())
    ref_custs = set(
        row["customer_id"] for row in reference.select("customer_id").collect()
    )
    overlap = len(py_custs & ref_custs)
    checks["customer_id_overlap"] = overlap
    checks["customer_id_overlap_pct"] = round(overlap / max(len(ref_custs), 1) * 100, 2)

    # Completeness check (no NULLs in key columns)
    null_checks = {}
    for c in ["customer_id", "segment_name", "risk_tier"]:
        if c in result.columns:
            null_count = result.filter(col(c).isNull()).count()
            null_checks[c] = null_count
    checks["null_counts_key_columns"] = null_checks
    checks["no_nulls_in_keys"] = all(v == 0 for v in null_checks.values())

    checks["overall_pass"] = checks["row_count_match"] and checks["no_nulls_in_keys"]

    return checks


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("validate_data_products")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("  Validating: CUSTOMER_MASTER_PROFILE (SAS Program 04)")
    print("=" * 70)

    results = validate(spark)
    for k, v in results.items():
        if isinstance(v, dict):
            print(f"  {k}:")
            for col_name, cnt in v.items():
                print(f"    {col_name:30s} : {cnt}")
        else:
            status = "PASS" if v is True else ("FAIL" if v is False else str(v))
            print(f"  {k:40s} : {status}")

    print("=" * 70)
    overall = "PASS" if results["overall_pass"] else "FAIL"
    print(f"  Overall: {overall}")
    print("=" * 70)

    spark.stop()
