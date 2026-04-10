"""
PySpark Validation: STG_TXN_SUMMARY
Replaces: bteq/02_stg_txn_summary.bteq

Validates that the PySpark translation of the BTEQ transaction summary
staging script produces output matching the reference CSV data.
"""
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    coalesce,
    col,
    count,
    countDistinct,
    current_timestamp,
    desc,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    sum as spark_sum,
    when,
)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
SOURCE_DIR = DATA_DIR / "01_source_tables"
REFERENCE_DIR = DATA_DIR / "02_bteq_staging"


def build_stg_txn_summary(spark: SparkSession):
    """Replicate the logic of 02_stg_txn_summary.bteq in PySpark."""

    # ------------------------------------------------------------------
    # 1. Load source tables
    # ------------------------------------------------------------------
    transactions = spark.read.csv(
        str(SOURCE_DIR / "transactions.csv"), header=True, inferSchema=True
    )
    txn_types = spark.read.csv(
        str(SOURCE_DIR / "transaction_types.csv"), header=True, inferSchema=True
    )
    accounts = spark.read.csv(
        str(SOURCE_DIR / "accounts.csv"), header=True, inferSchema=True
    )

    # ------------------------------------------------------------------
    # 2. Join transactions with types and accounts
    # ------------------------------------------------------------------
    txn_enriched = (
        transactions.filter(col("status_code") == "P")
        .join(txn_types, "transaction_type_cd", "left")
        .join(
            accounts.select("account_id", "customer_id", "account_type"),
            "account_id",
            "inner",
        )
    )

    # ------------------------------------------------------------------
    # 3. Per-account aggregations
    # Reference CSV columns:
    #   txn_count_total, txn_count_debit, txn_count_credit, txn_count_fee,
    #   amt_total_debit, amt_total_credit, amt_total_fees,
    #   amt_avg_debit, amt_avg_credit, amt_max_single_debit, amt_max_single_credit,
    #   distinct_merchants, top_merchant_category,
    #   pct_atm, pct_pos, pct_web, pct_mobile, days_since_last_txn
    # ------------------------------------------------------------------
    acct_agg = txn_enriched.groupBy("account_id", "customer_id", "account_type").agg(
        count("*").alias("txn_count_total"),
        count(when(col("category") == "DEBIT", True)).alias("txn_count_debit"),
        count(when(col("category") == "CREDIT", True)).alias("txn_count_credit"),
        count(when(col("category") == "FEE", True)).alias("txn_count_fee"),
        spark_sum(when(col("category") == "DEBIT", spark_abs(col("amount"))).otherwise(0)).alias(
            "amt_total_debit"
        ),
        spark_sum(when(col("category") == "CREDIT", col("amount")).otherwise(0)).alias(
            "amt_total_credit"
        ),
        spark_sum(when(col("category") == "FEE", spark_abs(col("amount"))).otherwise(0)).alias(
            "amt_total_fees"
        ),
        avg(when(col("category") == "DEBIT", spark_abs(col("amount")))).alias("amt_avg_debit"),
        avg(when(col("category") == "CREDIT", col("amount"))).alias("amt_avg_credit"),
        spark_max(when(col("category") == "DEBIT", spark_abs(col("amount")))).alias("amt_max_single_debit"),
        spark_max(when(col("category") == "CREDIT", col("amount"))).alias("amt_max_single_credit"),
        countDistinct("merchant_name").alias("distinct_merchants"),
        # Channel mix
        count(when(col("channel_code") == "ATM", True)).alias("atm_count"),
        count(when(col("channel_code") == "POS", True)).alias("pos_count"),
        count(when(col("channel_code").isin("ONL", "WEB"), True)).alias("web_count"),
        count(when(col("channel_code") == "MOB", True)).alias("mob_count"),
    )

    # Channel mix percentages
    acct_agg = acct_agg.withColumn(
        "pct_atm",
        when(col("txn_count_total") > 0, col("atm_count") / col("txn_count_total") * 100).otherwise(0),
    ).withColumn(
        "pct_pos",
        when(col("txn_count_total") > 0, col("pos_count") / col("txn_count_total") * 100).otherwise(0),
    ).withColumn(
        "pct_web",
        when(col("txn_count_total") > 0, col("web_count") / col("txn_count_total") * 100).otherwise(0),
    ).withColumn(
        "pct_mobile",
        when(col("txn_count_total") > 0, col("mob_count") / col("txn_count_total") * 100).otherwise(0),
    )

    # ------------------------------------------------------------------
    # 4. Top merchant category per account (QUALIFY ROW_NUMBER pattern)
    # ------------------------------------------------------------------
    merch_counts = (
        txn_enriched.filter(col("merchant_category").isNotNull())
        .groupBy("account_id", "merchant_category")
        .agg(count("*").alias("merch_cnt"))
    )
    merch_window = Window.partitionBy("account_id").orderBy(desc("merch_cnt"))
    top_merchant = (
        merch_counts.withColumn("rn", row_number().over(merch_window))
        .filter(col("rn") == 1)
        .select(
            col("account_id").alias("tm_account_id"),
            col("merchant_category").alias("top_merchant_category"),
        )
    )

    # ------------------------------------------------------------------
    # 5. Join and finalize
    #    Match reference CSV schema:
    #    customer_id, account_id, account_type,
    #    summary_period_start, summary_period_end,
    #    txn_count_total, txn_count_debit, txn_count_credit, txn_count_fee,
    #    amt_total_debit, amt_total_credit, amt_total_fees,
    #    amt_avg_debit, amt_avg_credit, amt_max_single_debit, amt_max_single_credit,
    #    distinct_merchants, top_merchant_category,
    #    pct_atm, pct_pos, pct_web, pct_mobile,
    #    days_since_last_txn, load_ts
    # ------------------------------------------------------------------
    result = (
        acct_agg.join(
            top_merchant, acct_agg["account_id"] == top_merchant["tm_account_id"], "left"
        )
        .withColumn("load_ts", current_timestamp())
        .select(
            "customer_id",
            "account_id",
            "account_type",
            "txn_count_total",
            "txn_count_debit",
            "txn_count_credit",
            "txn_count_fee",
            "amt_total_debit",
            "amt_total_credit",
            "amt_total_fees",
            "amt_avg_debit",
            "amt_avg_credit",
            "amt_max_single_debit",
            "amt_max_single_credit",
            "distinct_merchants",
            "top_merchant_category",
            "pct_atm",
            "pct_pos",
            "pct_web",
            "pct_mobile",
            "load_ts",
        )
    )

    return result


def validate(spark: SparkSession) -> dict:
    """Run the transformation and compare to reference data."""
    result = build_stg_txn_summary(spark)
    reference = spark.read.csv(
        str(REFERENCE_DIR / "stg_txn_summary.csv"), header=True, inferSchema=True
    )

    checks = {}

    # Row count
    result_count = result.count()
    ref_count = reference.count()
    checks["row_count_match"] = result_count == ref_count
    checks["row_count_pyspark"] = result_count
    checks["row_count_reference"] = ref_count

    # Column count
    checks["column_count_match"] = len(result.columns) == len(reference.columns)

    # Join on account_id and compare key numeric columns
    joined = result.alias("py").join(
        reference.alias("ref"),
        col("py.account_id") == col("ref.account_id"),
        "inner",
    )
    join_count = joined.count()
    checks["join_count"] = join_count
    checks["account_id_overlap_pct"] = round(join_count / max(ref_count, 1) * 100, 2)

    if join_count > 0:
        # txn_count_total comparison
        txn_diff = joined.withColumn(
            "cnt_diff",
            spark_abs(
                coalesce(col("py.txn_count_total"), lit(0))
                - coalesce(col("ref.txn_count_total"), lit(0))
            ),
        )
        max_cnt_diff = txn_diff.agg({"cnt_diff": "max"}).collect()[0][0]
        checks["max_txn_count_diff"] = int(max_cnt_diff) if max_cnt_diff else 0
        checks["txn_count_exact_match"] = checks["max_txn_count_diff"] == 0

    checks["overall_pass"] = (
        checks["row_count_match"]
        and checks.get("txn_count_exact_match", False)
    )

    return checks


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("validate_stg_txn_summary")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("  Validating: STG_TXN_SUMMARY (BTEQ Script 02)")
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
