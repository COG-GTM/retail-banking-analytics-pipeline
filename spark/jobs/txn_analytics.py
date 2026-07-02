"""02 - Transaction analytics (PySpark port of ``02_sas_txn_analytics.sas``).

SAS -> PySpark mapping
    PROC SQL customer aggregation -> groupBy().agg()
    PROC RANK groups=100          -> window rank/count (PROC RANK GROUPS formula)
    PROC MEANS median=/qrange=    -> exact percentile aggregation (IQR anomaly rule)

Reads  ETL_STAGING_DB.STG_TXN_SUMMARY
Writes DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from ..config import PipelineConfig
from ..logging_utils import PipelineAudit
from ..session import DataLayer, get_spark
from ..validation import enforce, validate_table

STEP = "02_TXN_ANALYTICS"

OUTPUT_COLUMNS = [
    "customer_id", "total_accounts", "active_accounts", "total_transactions",
    "total_debit_amt", "total_credit_amt", "total_fees", "top_spend_category",
    "net_cash_flow", "avg_transaction_size", "digital_txn_pct",
    "monthly_spend_trend", "fee_income", "interest_income", "revenue_contribution",
    "spend_percentile", "anomaly_flag", "reporting_period", "model_version",
    "effective_date", "load_ts",
]


def aggregate_to_customer(txn: DataFrame) -> DataFrame:
    """STEP 2: aggregate account-level rows to customer level."""
    grouped = txn.groupBy("customer_id").agg(
        F.countDistinct("account_id").alias("total_accounts"),
        F.sum(F.when(F.col("days_since_last_txn") <= 30, 1).otherwise(0)).alias("active_accounts"),
        F.sum("txn_count_total").alias("total_transactions"),
        F.sum("amt_total_debit").alias("total_debit_amt"),
        F.sum("amt_total_credit").alias("total_credit_amt"),
        F.sum("amt_total_fees").alias("total_fees"),
        F.max("top_merchant_category").alias("top_spend_category"),
        F.sum(F.col("amt_total_debit") + F.col("amt_total_credit")).alias("_sum_amt"),
        F.sum(F.col("txn_count_total") * (F.col("pct_web") + F.col("pct_mobile")) / 100.0).alias("_sum_digital"),
    )
    return (
        grouped
        .withColumn("net_cash_flow", F.col("total_credit_amt") - F.col("total_debit_amt"))
        .withColumn(
            "avg_transaction_size",
            F.when(F.col("total_transactions") > 0, F.col("_sum_amt") / F.col("total_transactions")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "digital_txn_pct",
            F.when(F.col("total_transactions") > 0, F.col("_sum_digital") / F.col("total_transactions") * 100).otherwise(F.lit(0.0)),
        )
        .drop("_sum_amt", "_sum_digital")
    )


def add_trend(df: DataFrame, config: PipelineConfig) -> DataFrame:
    """STEP 3: spend-trend classification and revenue components."""
    trend = (
        F.when(F.col("net_cash_flow") > F.col("avg_transaction_size") * 5, "UP")
        .when(F.col("net_cash_flow") < -F.col("avg_transaction_size") * 5, "DOWN")
        .otherwise("STABLE")
    )
    return (
        df.withColumn("monthly_spend_trend", trend)
        .withColumn("fee_income", F.col("total_fees"))
        .withColumn("interest_income", F.col("total_debit_amt") * F.lit(config.interest_income_rate))
        .withColumn("revenue_contribution", F.col("fee_income") + F.col("interest_income"))
    )


def add_spend_percentile(df: DataFrame, groups: int) -> DataFrame:
    """STEP 4: PROC RANK GROUPS=<groups> on total_debit_amt.

    Reproduces SAS PROC RANK with the default TIES=MEAN and the GROUPS formula
    ``FLOOR(rank * groups / (n + 1))`` (ascending), yielding integer groups
    ``0 .. groups-1``.
    """
    w_order = Window.orderBy(F.col("total_debit_amt").asc())
    w_val = Window.partitionBy("total_debit_amt")
    w_all = Window.partitionBy(F.lit(1))

    rank_min = F.rank().over(w_order)
    grp_cnt = F.count(F.lit(1)).over(w_val)
    n = F.count(F.lit(1)).over(w_all)
    mean_rank = rank_min + (grp_cnt - 1) / 2.0
    percentile = F.floor(mean_rank * F.lit(groups) / (n + 1))
    return df.withColumn("spend_percentile", percentile.cast("double"))


def add_anomaly_flag(df: DataFrame) -> DataFrame:
    """STEP 5: IQR anomaly rule (PROC MEANS median=/qrange=)."""
    stats = df.select(
        F.expr("percentile(total_debit_amt, 0.5)").alias("median"),
        F.expr("percentile(total_debit_amt, 0.25)").alias("q1"),
        F.expr("percentile(total_debit_amt, 0.75)").alias("q3"),
    ).first()
    median = stats["median"] if stats and stats["median"] is not None else 0.0
    iqr = (stats["q3"] - stats["q1"]) if stats and stats["q3"] is not None else 0.0

    flag = F.when(
        (F.col("total_debit_amt") > F.lit(median) + 3 * F.lit(iqr)) & (F.lit(iqr) > 0),
        F.lit("Y"),
    ).otherwise(F.lit("N"))
    return df.withColumn("anomaly_flag", flag)


def build_transaction_analytics(txn: DataFrame, config: PipelineConfig) -> DataFrame:
    aggregated = aggregate_to_customer(txn)
    with_trend = add_trend(aggregated, config)
    ranked = add_spend_percentile(with_trend, config.rank_groups)
    flagged = add_anomaly_flag(ranked)

    return flagged.select(
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("total_accounts").cast("int").alias("total_accounts"),
        F.col("active_accounts").cast("int").alias("active_accounts"),
        F.col("total_transactions").cast("int").alias("total_transactions"),
        F.col("total_debit_amt"),
        F.col("total_credit_amt"),
        F.col("total_fees"),
        F.col("top_spend_category"),
        F.col("net_cash_flow"),
        F.col("avg_transaction_size"),
        F.col("digital_txn_pct"),
        F.col("monthly_spend_trend"),
        F.col("fee_income"),
        F.col("interest_income"),
        F.col("revenue_contribution"),
        F.col("spend_percentile"),
        F.col("anomaly_flag"),
        F.lit(config.reporting_period).alias("reporting_period"),
        F.lit(config.txn_model_version).alias("model_version"),
        F.lit(config.effective_date).alias("effective_date"),
        F.lit(config.run_ts).alias("load_ts"),
    ).select(*OUTPUT_COLUMNS)


def run(config: PipelineConfig) -> str:
    audit = PipelineAudit(run_id=config.run_id)
    spark = get_spark(config)
    data = DataLayer(spark, config)

    audit.log_step(step=STEP, status="START", msg=f"Period: {config.reporting_period}")
    txn = data.read_staging("stg_txn_summary")

    result_df = build_transaction_analytics(txn, config).cache()
    row_count = result_df.count()
    audit.log_step(step=STEP, status="SUCCESS", msg="Analytics table built", rowcount=row_count)

    validation = validate_table(
        result_df, table="TRANSACTION_ANALYTICS",
        key_cols=["customer_id"],
        not_null=["customer_id", "reporting_period", "total_transactions"],
        min_rows=config.min_rows, audit=audit,
    )
    enforce(validation, "TRANSACTION_ANALYTICS", audit)

    audit.log_step(step=STEP, status="START", msg="Loading DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS")
    path = data.write_product(result_df, "transaction_analytics")
    audit.log_step(step=STEP, status="SUCCESS", msg=f"Pipeline complete -> {path}", rowcount=row_count)
    result_df.unpersist()
    return path


if __name__ == "__main__":
    run(PipelineConfig.from_env())
