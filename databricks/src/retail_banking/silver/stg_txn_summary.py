from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..audit import assert_rows, step
from ..config import RunConfig
from ..dq import validate_table
from ..tables import write_overwrite


def build(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    transactions = spark.table(cfg.fqn(cfg.bronze_txn_schema, "transactions")).alias("t")
    types = spark.table(cfg.fqn(cfg.bronze_txn_schema, "transaction_types")).alias("tt")
    accounts = spark.table(cfg.fqn(cfg.bronze_core_schema, "accounts")).alias("acct")
    period_start = F.add_months(F.lit(cfg.run_date).cast("date"), -cfg.lookback_months)
    period_end = F.lit(cfg.run_date).cast("date")

    posted = transactions.join(types, "transaction_type_cd").where(
        (F.col("transaction_date").between(period_start, period_end))
        & (F.col("status_code") == "P")
    )
    merchant_totals = (
        posted.where(F.col("merchant_category").isNotNull())
        .groupBy("account_id", "merchant_category")
        .agg(F.sum(F.abs("amount")).alias("_spend"))
    )
    top_window = Window.partitionBy("account_id").orderBy(F.col("_spend").desc())
    top_categories = (
        merchant_totals.withColumn("_rn", F.row_number().over(top_window))
        .where(F.col("_rn") == 1)
        .select("account_id", F.col("merchant_category").alias("top_merchant_category"))
    )
    grouped = (
        posted.groupBy("account_id")
        .agg(
            F.count("*").alias("txn_count_total"),
            F.sum(F.when(F.col("category") == "DEBIT", 1).otherwise(0)).alias("txn_count_debit"),
            F.sum(F.when(F.col("category") == "CREDIT", 1).otherwise(0)).alias("txn_count_credit"),
            F.sum(F.when(F.col("category") == "FEE", 1).otherwise(0)).alias("txn_count_fee"),
            F.sum(F.when(F.col("category") == "DEBIT", F.abs("amount")).otherwise(0)).alias(
                "amt_total_debit"
            ),
            F.sum(F.when(F.col("category") == "CREDIT", F.col("amount")).otherwise(0)).alias(
                "amt_total_credit"
            ),
            F.sum(F.when(F.col("category") == "FEE", F.abs("amount")).otherwise(0)).alias(
                "amt_total_fees"
            ),
            F.avg(F.when(F.col("category") == "DEBIT", F.abs("amount"))).alias("amt_avg_debit"),
            F.avg(F.when(F.col("category") == "CREDIT", F.col("amount"))).alias("amt_avg_credit"),
            F.max(F.when(F.col("category") == "DEBIT", F.abs("amount")).otherwise(0)).alias(
                "amt_max_single_debit"
            ),
            F.max(F.when(F.col("category") == "CREDIT", F.col("amount")).otherwise(0)).alias(
                "amt_max_single_credit"
            ),
            F.countDistinct("merchant_name").alias("distinct_merchants"),
            F.sum(F.when(F.col("channel_code") == "ATM", 1).otherwise(0)).alias("_atm"),
            F.sum(F.when(F.col("channel_code") == "POS", 1).otherwise(0)).alias("_pos"),
            F.sum(F.when(F.col("channel_code") == "WEB", 1).otherwise(0)).alias("_web"),
            F.sum(F.when(F.col("channel_code") == "MOB", 1).otherwise(0)).alias("_mobile"),
            F.datediff(period_end, F.max("transaction_date")).alias("days_since_last_txn"),
        )
        .join(accounts.select("account_id", "customer_id", "account_type"), "account_id")
        .join(top_categories, "account_id", "left")
    )
    denominator = F.when(F.col("txn_count_total") != 0, F.col("txn_count_total"))
    return grouped.select(
        "customer_id",
        "account_id",
        "account_type",
        period_start.alias("summary_period_start"),
        period_end.alias("summary_period_end"),
        F.col("txn_count_total").cast("int").alias("txn_count_total"),
        F.col("txn_count_debit").cast("int").alias("txn_count_debit"),
        F.col("txn_count_credit").cast("int").alias("txn_count_credit"),
        F.col("txn_count_fee").cast("int").alias("txn_count_fee"),
        F.col("amt_total_debit").cast("decimal(18,2)").alias("amt_total_debit"),
        F.col("amt_total_credit").cast("decimal(18,2)").alias("amt_total_credit"),
        F.col("amt_total_fees").cast("decimal(18,2)").alias("amt_total_fees"),
        F.col("amt_avg_debit").cast("decimal(15,2)").alias("amt_avg_debit"),
        F.col("amt_avg_credit").cast("decimal(15,2)").alias("amt_avg_credit"),
        F.col("amt_max_single_debit").cast("decimal(15,2)").alias("amt_max_single_debit"),
        F.col("amt_max_single_credit").cast("decimal(15,2)").alias("amt_max_single_credit"),
        F.col("distinct_merchants").cast("int").alias("distinct_merchants"),
        "top_merchant_category",
        (F.col("_atm") * 100.0 / denominator).cast("decimal(5,2)").alias("pct_atm"),
        (F.col("_pos") * 100.0 / denominator).cast("decimal(5,2)").alias("pct_pos"),
        (F.col("_web") * 100.0 / denominator).cast("decimal(5,2)").alias("pct_web"),
        (F.col("_mobile") * 100.0 / denominator).cast("decimal(5,2)").alias("pct_mobile"),
        F.col("days_since_last_txn").cast("int").alias("days_since_last_txn"),
        F.current_timestamp().alias("load_ts"),
    )


def run(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    fqn = cfg.fqn(cfg.silver_schema, "stg_txn_summary")
    with step(spark, cfg, "02_stg_txn_summary", "FULL_LOAD") as state:
        result = build(spark, cfg)
        state["row_count"] = assert_rows(result, "stg_txn_summary")
        write_overwrite(result, fqn)
    validate_table(
        spark, fqn, ["customer_id", "account_id"], ["customer_id", "account_id"], cfg.dq_min_rows
    )
    return result
