"""
PySpark Validation: TRANSACTION_ANALYTICS
Replaces: sas/02_sas_txn_analytics.sas

Validates that a PySpark translation of the SAS transaction analytics
program (PROC RANK, PROC MEANS) produces output comparable to reference data.
"""
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    coalesce,
    col,
    count,
    current_date,
    current_timestamp,
    floor,
    lit,
    percent_rank,
    sum as spark_sum,
    when,
)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
STAGING_DIR = DATA_DIR / "02_bteq_staging"
REFERENCE_DIR = DATA_DIR / "03_sas_data_products"


def build_txn_analytics(spark: SparkSession):
    """Replicate the logic of 02_sas_txn_analytics.sas in PySpark."""

    # ------------------------------------------------------------------
    # 1. Load staging data
    # ------------------------------------------------------------------
    txn_summary = spark.read.csv(
        str(STAGING_DIR / "stg_txn_summary.csv"), header=True, inferSchema=True
    )

    # ------------------------------------------------------------------
    # 2. Aggregate to customer level (SAS: PROC SQL with GROUP BY)
    #    Reference stg_txn_summary columns: txn_count_total, amt_total_debit,
    #    amt_total_credit, amt_total_fees, txn_count_debit, txn_count_credit
    # ------------------------------------------------------------------
    cust_agg = txn_summary.groupBy("customer_id").agg(
        spark_sum("amt_total_debit").alias("total_debit_amt"),
        spark_sum("amt_total_credit").alias("total_credit_amt"),
        spark_sum("amt_total_fees").alias("total_fees"),
        spark_sum("txn_count_total").alias("total_transactions"),
        count("account_id").alias("total_accounts"),
        count(when(col("txn_count_total") > 0, True)).alias("active_accounts"),
    )

    # Net cash flow
    cust_agg = cust_agg.withColumn(
        "net_cash_flow",
        coalesce(col("total_credit_amt"), lit(0))
        + coalesce(col("total_debit_amt"), lit(0)),
    ).withColumn(
        "avg_transaction_size",
        when(
            col("total_transactions") > 0,
            (spark_abs(col("total_debit_amt")) + col("total_credit_amt")) / col("total_transactions"),
        ).otherwise(0),
    )

    # ------------------------------------------------------------------
    # 3. PROC RANK equivalent: percentile rankings
    # ------------------------------------------------------------------
    spend_window = Window.orderBy(spark_abs(col("total_debit_amt")))
    txn_window = Window.orderBy(col("total_transactions"))

    cust_agg = cust_agg.withColumn(
        "spend_percentile",
        floor(percent_rank().over(spend_window) * 100).cast("int"),
    ).withColumn(
        "activity_percentile",
        floor(percent_rank().over(txn_window) * 100).cast("int"),
    )

    # ------------------------------------------------------------------
    # 4. PROC MEANS equivalent: IQR anomaly detection
    # ------------------------------------------------------------------
    debit_abs = cust_agg.withColumn(
        "abs_debit", spark_abs(col("total_debit_amt"))
    )
    quantiles = debit_abs.approxQuantile("abs_debit", [0.25, 0.5, 0.75], 0.01)

    if len(quantiles) == 3:
        q1, median, q3 = quantiles
        iqr = q3 - q1
        upper_fence = median + 3 * iqr
    else:
        upper_fence = float("inf")

    cust_agg = cust_agg.withColumn(
        "anomaly_flag",
        when(spark_abs(col("total_debit_amt")) > upper_fence, "Y").otherwise("N"),
    )

    # ------------------------------------------------------------------
    # 5. Spend trend classification
    # ------------------------------------------------------------------
    cust_agg = cust_agg.withColumn(
        "spend_trend",
        when(col("net_cash_flow") > 1000, "GROWING")
        .when(col("net_cash_flow") > 0, "STABLE")
        .when(col("net_cash_flow") > -1000, "DECLINING")
        .otherwise("AT_RISK"),
    )

    result = (
        cust_agg.withColumn("model_version", lit("TXN_V2.0"))
        .withColumn("effective_date", current_date())
        .withColumn("load_ts", current_timestamp())
        .select(
            "customer_id",
            "total_accounts",
            "active_accounts",
            "total_transactions",
            "total_debit_amt",
            "total_credit_amt",
            "total_fees",
            "net_cash_flow",
            "avg_transaction_size",
            "spend_percentile",
            "anomaly_flag",
            "spend_trend",
            "model_version",
            "effective_date",
            "load_ts",
        )
    )

    return result


def validate(spark: SparkSession) -> dict:
    """Run the transformation and compare to reference data."""
    result = build_txn_analytics(spark)
    reference = spark.read.csv(
        str(REFERENCE_DIR / "transaction_analytics.csv"), header=True, inferSchema=True
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

    # Anomaly flag distribution
    py_anomalies = result.filter(col("anomaly_flag") == "Y").count()
    checks["pyspark_anomaly_count"] = py_anomalies

    checks["overall_pass"] = checks["row_count_match"]

    return checks


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("validate_txn_analytics")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("  Validating: TRANSACTION_ANALYTICS (SAS Program 02)")
    print("=" * 70)

    results = validate(spark)
    for k, v in results.items():
        status = "PASS" if v is True else ("FAIL" if v is False else str(v))
        print(f"  {k:40s} : {status}")

    print("=" * 70)
    overall = "PASS" if results["overall_pass"] else "FAIL"
    print(f"  Overall: {overall}")
    print("=" * 70)

    spark.stop()
