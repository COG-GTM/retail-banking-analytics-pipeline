"""Transaction analytics data product.

PySpark port of ``sas/02_sas_txn_analytics.sas``.

Reads Delta ``etl_staging.stg_txn_summary`` (account-level transaction
summaries), aggregates to customer level, derives spend trend, spend percentile,
revenue components and IQR-based anomaly flags, then writes the Delta
``data_products.transaction_analytics`` data product (partitioned by
``reporting_period``).

Legacy construct -> PySpark mapping
-----------------------------------
* ``PROC SQL`` customer aggregation      -> ``groupBy("customer_id").agg(...)``
* ``PROC RANK groups=100`` spend rank    -> ``percent_rank()`` window ([0, 1])
* ``PROC MEANS`` median/IQR anomaly      -> ``approxQuantile`` (Q1/median/Q3 -> IQR)
* ``%log_step`` / ``%validate_table``    -> ``log_step`` / ``validate_table``
* ``DELETE`` + ``PROC APPEND``           -> idempotent Delta partition overwrite
"""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.audit import init_audit, log_step
from common.config import Config
from common.validation import validate_table

JOB_NAME = "02_txn_analytics"
MODEL_VERSION = "TXN_V2.1"

# Relative error for approxQuantile; 0.0 => exact quantiles (fine at demo scale).
_QUANTILE_RELATIVE_ERROR = 0.0
# Anomaly threshold multiplier: spend above median + N*IQR is flagged (from SAS).
_IQR_MULTIPLIER = 3.0
# Spend-trend sensitivity: |net cash flow| beyond N*avg txn size => UP/DOWN.
_TREND_MULTIPLIER = 5.0

OUTPUT_COLUMNS = [
    "customer_id",
    "reporting_period",
    "total_accounts",
    "active_accounts",
    "total_transactions",
    "total_debit_amt",
    "total_credit_amt",
    "net_cash_flow",
    "avg_transaction_size",
    "monthly_spend_trend",
    "spend_percentile",
    "top_spend_category",
    "digital_txn_pct",
    "fee_income",
    "interest_income",
    "revenue_contribution",
    "anomaly_flag",
    "model_version",
    "effective_date",
    "load_ts",
]


def _reporting_period(cfg: Config) -> str:
    """Current reporting period as ``YYYY-MM`` (SAS ``&REPORTING_PERIOD``)."""
    return cfg.run_date.strftime("%Y-%m")


def aggregate_to_customer(txn_stg: DataFrame) -> DataFrame:
    """Aggregate account-level summaries to one row per customer (SAS STEP 2)."""
    total_txn = F.sum("txn_count_total")
    return txn_stg.groupBy("customer_id").agg(
        F.countDistinct("account_id").alias("total_accounts"),
        F.sum((F.col("days_since_last_txn") <= 30).cast("int")).alias(
            "active_accounts"
        ),
        total_txn.alias("total_transactions"),
        F.sum("amt_total_debit").alias("total_debit_amt"),
        F.sum("amt_total_credit").alias("total_credit_amt"),
        (F.sum("amt_total_credit") - F.sum("amt_total_debit")).alias(
            "net_cash_flow"
        ),
        F.when(
            total_txn > 0,
            F.sum(F.col("amt_total_debit") + F.col("amt_total_credit")) / total_txn,
        )
        .otherwise(F.lit(0.0))
        .alias("avg_transaction_size"),
        F.sum("amt_total_fees").alias("total_fees"),
        F.max("top_merchant_category").alias("top_spend_category"),
        F.when(
            total_txn > 0,
            F.sum(
                F.col("txn_count_total")
                * (F.col("pct_web") + F.col("pct_mobile"))
                / F.lit(100.0)
            )
            / total_txn
            * F.lit(100.0),
        )
        .otherwise(F.lit(0.0))
        .alias("digital_txn_pct"),
    )


def add_trend_and_revenue(cust_txn: DataFrame) -> DataFrame:
    """Add spend trend, revenue components and initial anomaly flag (SAS STEP 3)."""
    band = F.col("avg_transaction_size") * F.lit(_TREND_MULTIPLIER)
    trend = (
        F.when(F.col("net_cash_flow") > band, F.lit("UP"))
        .when(F.col("net_cash_flow") < -band, F.lit("DOWN"))
        .otherwise(F.lit("STABLE"))
    )
    return (
        cust_txn.withColumn("monthly_spend_trend", trend)
        .withColumn("fee_income", F.col("total_fees"))
        .withColumn("interest_income", F.col("total_debit_amt") * F.lit(0.02))
        .withColumn(
            "revenue_contribution",
            F.col("total_fees") + F.col("total_debit_amt") * F.lit(0.02),
        )
        .withColumn("anomaly_flag", F.lit("N"))
    )


def add_spend_percentile(df: DataFrame) -> DataFrame:
    """Rank customers by total debit spend using ``percent_rank`` (SAS STEP 4).

    ``percent_rank`` yields values in ``[0, 1]``, replacing ``PROC RANK groups=100``.
    """
    window = Window.orderBy(F.col("total_debit_amt").asc())
    return df.withColumn("spend_percentile", F.percent_rank().over(window))


def flag_anomalies(df: DataFrame) -> DataFrame:
    """Flag spend anomalies via the IQR method (SAS STEP 5, ``PROC MEANS``).

    Computes Q1/median/Q3 of ``total_debit_amt`` with ``approxQuantile``; flags a
    customer when spend exceeds ``median + 3 * IQR`` and ``IQR > 0``.
    """
    q1, median, q3 = df.approxQuantile(
        "total_debit_amt", [0.25, 0.5, 0.75], _QUANTILE_RELATIVE_ERROR
    )
    iqr = q3 - q1
    if iqr > 0:
        threshold = median + _IQR_MULTIPLIER * iqr
        flag = F.when(
            F.col("total_debit_amt") > F.lit(threshold), F.lit("Y")
        ).otherwise(F.col("anomaly_flag"))
        df = df.withColumn("anomaly_flag", flag)
    return df


def build_analytics(txn_stg: DataFrame, cfg: Config) -> DataFrame:
    """Full transform: staging summaries -> transaction_analytics data product."""
    period = _reporting_period(cfg)
    aggregated = aggregate_to_customer(txn_stg)
    enriched = add_trend_and_revenue(aggregated)
    ranked = add_spend_percentile(enriched)
    flagged = flag_anomalies(ranked)
    return (
        flagged.withColumn("reporting_period", F.lit(period))
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("effective_date", F.lit(cfg.run_date))
        .withColumn("load_ts", F.current_timestamp())
        .select(*OUTPUT_COLUMNS)
    )


def _write(df: DataFrame, spark: SparkSession, cfg: Config, period: str) -> None:
    """Idempotent, partition-aware Delta write of the current reporting period.

    First run creates the partitioned Delta table; subsequent runs replace only
    the current ``reporting_period`` partition via dynamic partition overwrite,
    leaving other periods untouched (no blind appends).
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.catalog}.{cfg.schema_dp}")
    fq_table = cfg.table(cfg.schema_dp, "transaction_analytics")

    if not spark.catalog.tableExists(fq_table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("reporting_period")
            .saveAsTable(fq_table)
        )
        return

    previous_mode = spark.conf.get(
        "spark.sql.sources.partitionOverwriteMode", "static"
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        df.write.format("delta").mode("overwrite").insertInto(fq_table)
    finally:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", previous_mode)


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    """Run the transaction analytics job; returns the written table's DataFrame."""
    run_id = str(uuid.uuid4())
    period = _reporting_period(cfg)
    init_audit(spark, cfg)
    log_step(
        spark, cfg, run_id, JOB_NAME, "start", "START",
        message=f"reporting_period={period}",
    )

    source = cfg.table(cfg.schema_stg, "stg_txn_summary")
    txn_stg = spark.table(source)
    log_step(
        spark, cfg, run_id, JOB_NAME, "extract_stg_txn_summary", "SUCCESS",
        row_count=txn_stg.count(), message=source,
    )

    analytics = build_analytics(txn_stg, cfg).cache()
    validate_table(
        analytics,
        min_rows=1,
        not_null_cols=["customer_id", "reporting_period", "total_transactions"],
        unique_keys=["customer_id"],
    )

    _write(analytics, spark, cfg, period)
    fq_table = cfg.table(cfg.schema_dp, "transaction_analytics")
    log_step(
        spark, cfg, run_id, JOB_NAME, "write_transaction_analytics", "SUCCESS",
        row_count=analytics.count(), message=fq_table,
    )
    analytics.unpersist()
    return spark.table(fq_table).where(F.col("reporting_period") == period)


if __name__ == "__main__":
    from common.config import get_config
    from common.spark import get_spark

    _cfg = get_config()
    run(get_spark(), _cfg)
