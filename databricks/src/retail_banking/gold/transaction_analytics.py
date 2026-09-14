from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..audit import assert_rows, step
from ..config import RunConfig
from ..dq import validate_table
from ..tables import write_overwrite_partition

MODEL_VERSION = "TXN_V2.1"


def build(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    staging = spark.table(cfg.fqn(cfg.silver_schema, "stg_txn_summary"))
    customer = (
        staging.groupBy("customer_id")
        .agg(
            F.countDistinct("account_id").alias("total_accounts"),
            F.sum(F.when(F.col("days_since_last_txn") <= 30, 1).otherwise(0)).alias(
                "active_accounts"
            ),
            F.sum("txn_count_total").alias("total_transactions"),
            F.sum("amt_total_debit").alias("total_debit_amt"),
            F.sum("amt_total_credit").alias("total_credit_amt"),
            F.sum("amt_total_fees").alias("total_fees"),
            F.first("top_merchant_category", ignorenulls=True).alias("top_spend_category"),
            F.sum(F.col("txn_count_total") * (F.col("pct_web") + F.col("pct_mobile")) / 100).alias(
                "_digital_transactions"
            ),
        )
        .withColumn("net_cash_flow", F.col("total_credit_amt") - F.col("total_debit_amt"))
        .withColumn(
            "avg_transaction_size",
            (F.col("total_debit_amt") + F.col("total_credit_amt")) / F.col("total_transactions"),
        )
        .withColumn(
            "digital_txn_pct",
            F.col("_digital_transactions") / F.col("total_transactions") * 100,
        )
    )
    customer = customer.withColumn(
        "monthly_spend_trend",
        F.when(
            F.col("net_cash_flow") > F.col("avg_transaction_size") * 5,
            "UP",
        )
        .when(
            F.col("net_cash_flow") < -F.col("avg_transaction_size") * 5,
            "DOWN",
        )
        .otherwise("STABLE"),
    )
    customer = customer.withColumn("fee_income", F.col("total_fees")).withColumn(
        "interest_income", F.col("total_debit_amt") * F.lit(0.02)
    )
    customer = customer.withColumn(
        "revenue_contribution", F.col("fee_income") + F.col("interest_income")
    )
    stats = customer.agg(
        F.percentile_approx("total_debit_amt", 0.5, 10000).alias("_median"),
        (
            F.percentile_approx("total_debit_amt", 0.75, 10000)
            - F.percentile_approx("total_debit_amt", 0.25, 10000)
        ).alias("_iqr"),
    ).first()
    median = stats["_median"]
    iqr = stats["_iqr"]
    customer = customer.withColumn(
        "anomaly_flag",
        F.when(
            (F.col("total_debit_amt") > F.lit(median) + 3 * F.lit(iqr)) & (F.lit(iqr) > 0),
            "Y",
        ).otherwise("N"),
    )
    rank_window = Window.orderBy(F.col("total_debit_amt"))
    customer = customer.withColumn(
        "spend_percentile",
        (F.rank().over(rank_window) / F.count("*").over(Window.partitionBy()) * 100).cast(
            "decimal(5,2)"
        ),
    )
    return customer.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("total_accounts").cast("smallint").alias("total_accounts"),
        F.col("active_accounts").cast("smallint").alias("active_accounts"),
        F.col("total_transactions").cast("int").alias("total_transactions"),
        F.col("total_debit_amt").cast("decimal(18,2)").alias("total_debit_amt"),
        F.col("total_credit_amt").cast("decimal(18,2)").alias("total_credit_amt"),
        F.col("total_fees").cast("decimal(15,2)").alias("total_fees"),
        "top_spend_category",
        F.col("net_cash_flow").cast("decimal(18,2)").alias("net_cash_flow"),
        F.col("avg_transaction_size").cast("decimal(15,2)").alias("avg_transaction_size"),
        F.col("digital_txn_pct").cast("decimal(5,2)").alias("digital_txn_pct"),
        "monthly_spend_trend",
        F.col("fee_income").cast("decimal(15,2)").alias("fee_income"),
        F.col("interest_income").cast("decimal(15,2)").alias("interest_income"),
        F.col("revenue_contribution").cast("decimal(15,2)").alias("revenue_contribution"),
        "spend_percentile",
        "anomaly_flag",
        F.date_format(F.lit(cfg.run_date), "yyyy-MM").alias("reporting_period"),
        F.lit(MODEL_VERSION).alias("model_version"),
        F.lit(cfg.run_date).cast("date").alias("effective_date"),
        F.current_timestamp().alias("load_ts"),
    )


def run(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    fqn = cfg.fqn(cfg.gold_schema, "transaction_analytics")
    period = cfg.run_date.strftime("%Y-%m")
    with step(spark, cfg, "02_transaction_analytics", "FULL_LOAD") as state:
        result = build(spark, cfg)
        state["row_count"] = assert_rows(result, "transaction_analytics")
        write_overwrite_partition(result, fqn, "reporting_period", period)
    validate_table(
        spark,
        fqn,
        ["customer_id", "reporting_period"],
        ["customer_id", "reporting_period"],
        cfg.dq_min_rows,
    )
    return result
