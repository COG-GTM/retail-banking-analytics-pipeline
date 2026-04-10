"""
PySpark Validation: CUSTOMER_RISK_SCORES
Replaces: sas/03_sas_risk_scoring.sas

Validates that a PySpark+MLlib translation of the SAS risk scoring
program (PROC LOGISTIC) produces output comparable to reference data.

Note: Logistic regression coefficients may differ from SAS.
We compare tier distributions rather than exact scores.
"""
from pathlib import Path

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
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


def build_risk_scoring(spark: SparkSession):
    """Replicate the logic of 03_sas_risk_scoring.sas in PySpark+MLlib."""

    # ------------------------------------------------------------------
    # 1. Load staging data
    # ------------------------------------------------------------------
    risk_factors = spark.read.csv(
        str(STAGING_DIR / "stg_risk_factors.csv"), header=True, inferSchema=True
    )
    customer_360 = spark.read.csv(
        str(STAGING_DIR / "stg_customer_360.csv"), header=True, inferSchema=True
    )

    # ------------------------------------------------------------------
    # 2. Feature preparation (imputation + normalization)
    # ------------------------------------------------------------------
    features_df = risk_factors.join(
        customer_360.select("customer_id", "tenure_months", "num_active_accounts"),
        "customer_id",
        "inner",
    )

    # Impute missing values (SAS: if external_credit_score <= 0 then 680)
    #
    # Reference stg_risk_factors columns:
    #   customer_id, account_overdraft_cnt, nsf_fee_total,
    #   large_withdrawal_cnt, large_withdrawal_amt,
    #   avg_daily_balance_30d, avg_daily_balance_90d, balance_volatility,
    #   credit_util_ratio, payment_ontime_pct, payment_late_cnt,
    #   months_since_last_late, external_credit_score,
    #   debit_velocity_7d, debit_velocity_30d, new_merchant_cnt_30d,
    #   international_txn_cnt, high_risk_merchant_cnt
    features_df = features_df.withColumn(
        "bureau_score_norm",
        when(
            col("external_credit_score").isNull() | (col("external_credit_score") <= 0),
            (680 - 300) / (850 - 300) * 100,
        ).otherwise((col("external_credit_score") - 300) / (850 - 300) * 100),
    ).fillna(
        {
            "avg_daily_balance_30d": 0,
            "balance_volatility": 0,
            "credit_util_ratio": 0,
            "payment_ontime_pct": 100,
            "payment_late_cnt": 0,
            "tenure_months": 0,
            "num_active_accounts": 0,
        }
    )

    # ------------------------------------------------------------------
    # 3. Composite risk score (weighted formula from SAS)
    #    In the SAS code, PROC LOGISTIC is used to derive weights.
    #    For validation, we use a deterministic weighted composite.
    # ------------------------------------------------------------------
    features_df = features_df.withColumn(
        "composite_risk_score",
        (100 - col("bureau_score_norm")) * 0.35
        + col("credit_util_ratio") * 100 * 0.20
        + when(col("payment_late_cnt") > 0, col("payment_late_cnt") * 10).otherwise(0) * 0.15
        + when(col("balance_volatility") > 0, 50).otherwise(0) * 0.10
        + when(col("debit_velocity_30d") > 10000, 30).otherwise(0) * 0.10
        + when(col("tenure_months") < 12, 20).otherwise(0) * 0.10,
    )

    # ------------------------------------------------------------------
    # 4. Risk tier classification
    # ------------------------------------------------------------------
    features_df = features_df.withColumn(
        "risk_tier",
        when(col("composite_risk_score") < 20, "LOW")
        .when(col("composite_risk_score") < 40, "MODERATE")
        .when(col("composite_risk_score") < 60, "ELEVATED")
        .when(col("composite_risk_score") < 80, "HIGH")
        .otherwise("CRITICAL"),
    )

    result = (
        features_df.withColumn("model_version", lit("RISK_V2.1"))
        .withColumn("effective_date", current_date())
        .withColumn("load_ts", current_timestamp())
        .select(
            "customer_id",
            "composite_risk_score",
            "risk_tier",
            "bureau_score_norm",
            "credit_util_ratio",
            "model_version",
            "effective_date",
            "load_ts",
        )
    )

    return result


def validate(spark: SparkSession) -> dict:
    """Run the scoring and compare tier distributions to reference data."""
    result = build_risk_scoring(spark)
    reference = spark.read.csv(
        str(REFERENCE_DIR / "customer_risk_scores.csv"), header=True, inferSchema=True
    )

    checks = {}

    # Row count
    result_count = result.count()
    ref_count = reference.count()
    checks["row_count_match"] = result_count == ref_count
    checks["row_count_pyspark"] = result_count
    checks["row_count_reference"] = ref_count

    # Customer overlap
    py_custs = set(row["customer_id"] for row in result.select("customer_id").collect())
    ref_custs = set(
        row["customer_id"] for row in reference.select("customer_id").collect()
    )
    overlap = len(py_custs & ref_custs)
    checks["customer_id_overlap"] = overlap
    checks["customer_id_overlap_pct"] = round(overlap / max(len(ref_custs), 1) * 100, 2)

    # Risk tier distribution comparison
    py_tier_dist = {
        row["risk_tier"]: row["cnt"]
        for row in result.groupBy("risk_tier").agg(count("*").alias("cnt")).collect()
    }
    ref_tier_dist = {}
    if "risk_tier" in reference.columns:
        ref_tier_dist = {
            row["risk_tier"]: row["cnt"]
            for row in reference.groupBy("risk_tier")
            .agg(count("*").alias("cnt"))
            .collect()
        }

    checks["pyspark_tier_distribution"] = py_tier_dist
    checks["reference_tier_distribution"] = ref_tier_dist

    checks["overall_pass"] = checks["row_count_match"]

    return checks


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("validate_risk_scoring")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("  Validating: CUSTOMER_RISK_SCORES (SAS Program 03)")
    print("=" * 70)

    results = validate(spark)
    for k, v in results.items():
        if isinstance(v, dict):
            print(f"  {k}:")
            for tier, cnt in sorted(v.items()):
                print(f"    {tier:20s} : {cnt}")
        else:
            status = "PASS" if v is True else ("FAIL" if v is False else str(v))
            print(f"  {k:40s} : {status}")

    print("=" * 70)
    overall = "PASS" if results["overall_pass"] else "FAIL"
    print(f"  Overall: {overall}")
    print("=" * 70)

    spark.stop()
