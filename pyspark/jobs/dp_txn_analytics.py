"""Data-product job 02 -- TRANSACTION_ANALYTICS.

Faithful PySpark port of ``sas/02_sas_txn_analytics.sas``: read the BTEQ-produced
``STG_TXN_SUMMARY`` staging table, aggregate the account-level summary up to
customer level, derive spend trends, revenue proxies, a spend percentile and an
IQR anomaly flag, then emit the ``TRANSACTION_ANALYTICS`` data product
(partitioned by ``reporting_period`` per the DDL).

SAS -> PySpark mapping:
* ``PROC SQL ... GROUP BY CUSTOMER_ID`` (STEP 2) -> :func:`aggregate_customer`.
  ``count(distinct ACCOUNT_ID)``, the ``DAYS_SINCE_LAST_TXN <= 30`` active-account
  sum, the zero-guarded ``AVG_TRANSACTION_SIZE`` and ``DIGITAL_TXN_PCT`` CASE
  expressions are reproduced verbatim.  ``max(TOP_MERCHANT_CATEGORY)`` -> ``F.max``.
* STEP 3 ``DATA`` step trend / revenue proxies -> :func:`add_trend_and_revenue`.
* STEP 4 ``PROC RANK groups=100`` -> :func:`add_spend_percentile` via
  ``ntile(100)`` (ascending) minus 1 to yield 0..99.  ``ntile`` splits ties across
  adjacent groups whereas ``PROC RANK`` assigns tied values the same group -- a
  minor, documented fidelity deviation (see MIGRATION_NOTES / PR body).
* STEP 5 ``PROC MEANS median/qrange`` + the ``median + 3*IQR`` flag ->
  :func:`add_anomaly_flag` using ``percentile_approx`` for the population median
  and inter-quartile range, broadcast-joined back onto every row.

Follows the reference job (``staging_customer_360``): pure ``transform``-style
functions (unit-testable on in-memory DataFrames) + a thin :func:`run` that wires
I/O, validation, audit and the schema contract.
"""

from __future__ import annotations

import argparse
import datetime as _dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.dates import load_timestamp
from common.io import DataIO, LocalDataIO
from common.spark import build_spark
from common.validation import abort_on_failure, validate_table

JOB_NAME = "02_txn_analytics"
TARGET = "TRANSACTION_ANALYTICS"
MODEL_VERSION = "TXN_V2.1"

_ACTIVE_TXN_DAYS = 30


def aggregate_customer(txn: DataFrame) -> DataFrame:
    """Account-level ``STG_TXN_SUMMARY`` -> customer-level rollup (SAS STEP 2).

    Ratio/percent expressions are evaluated in double precision (final decimal
    cast is applied by :func:`schemas.enforce_schema`) to avoid decimal-overflow
    NULLs under ``spark.sql.decimalOperations.allowPrecisionLoss=false``.
    """
    # sum(TXN_COUNT_TOTAL * (PCT_WEB + PCT_MOBILE) / 100)
    digital_weighted = (
        F.col("txn_count_total")
        * (F.col("pct_web") + F.col("pct_mobile")).cast("double")
        / F.lit(100.0)
    )
    # sum(AMT_TOTAL_DEBIT + AMT_TOTAL_CREDIT) numerator for AVG_TRANSACTION_SIZE
    activity_amt = (F.col("amt_total_debit") + F.col("amt_total_credit")).cast("double")

    agg = txn.groupBy("customer_id").agg(
        F.countDistinct("account_id").alias("total_accounts"),
        F.sum(
            F.when(F.col("days_since_last_txn") <= _ACTIVE_TXN_DAYS, F.lit(1)).otherwise(F.lit(0))
        ).alias("active_accounts"),
        F.sum("txn_count_total").alias("total_transactions"),
        F.sum("amt_total_debit").alias("total_debit_amt"),
        F.sum("amt_total_credit").alias("total_credit_amt"),
        F.sum("amt_total_fees").alias("total_fees"),
        F.max("top_merchant_category").alias("top_spend_category"),
        F.sum(digital_weighted).alias("_digital_weighted"),
        F.sum(activity_amt).alias("_activity_amt"),
    )

    total_txn = F.col("total_transactions")
    avg_txn_size = F.when(total_txn > 0, F.col("_activity_amt") / total_txn).otherwise(F.lit(0.0))
    digital_pct = F.when(
        total_txn > 0, F.col("_digital_weighted") / total_txn * F.lit(100.0)
    ).otherwise(F.lit(0.0))

    return (
        agg
        .withColumn("net_cash_flow", F.col("total_credit_amt") - F.col("total_debit_amt"))
        .withColumn("avg_transaction_size", avg_txn_size)
        .withColumn("digital_txn_pct", digital_pct)
        .drop("_digital_weighted", "_activity_amt")
    )


def add_trend_and_revenue(df: DataFrame) -> DataFrame:
    """Spend trend + revenue proxies (SAS STEP 3 DATA step)."""
    band = F.col("avg_transaction_size") * F.lit(5.0)
    trend = (
        F.when(F.col("net_cash_flow") > band, F.lit("UP"))
        .when(F.col("net_cash_flow") < -band, F.lit("DOWN"))
        .otherwise(F.lit("STABLE"))
    )
    interest_income = F.col("total_debit_amt") * F.lit(0.02)
    return (
        df
        .withColumn("monthly_spend_trend", trend)
        .withColumn("fee_income", F.col("total_fees"))
        .withColumn("interest_income", interest_income)
        .withColumn("revenue_contribution", F.col("total_fees") + interest_income)
    )


def add_spend_percentile(df: DataFrame) -> DataFrame:
    """Spend percentile 0..99 (SAS STEP 4 ``PROC RANK groups=100``).

    Ordered ascending so the highest spenders land in the top group; ``ntile``
    tie handling differs slightly from ``PROC RANK`` (documented deviation).
    """
    win = Window.orderBy(F.col("total_debit_amt").asc())
    return df.withColumn("spend_percentile", F.ntile(100).over(win) - F.lit(1))


def add_anomaly_flag(df: DataFrame) -> DataFrame:
    """IQR anomaly flag (SAS STEP 5 ``PROC MEANS`` median + qrange)."""
    debit = F.col("total_debit_amt").cast("double")
    stats = (
        df.agg(
            F.percentile_approx(debit, 0.5).alias("_median"),
            F.percentile_approx(debit, 0.25).alias("_q1"),
            F.percentile_approx(debit, 0.75).alias("_q3"),
        )
        .withColumn("_iqr", F.col("_q3") - F.col("_q1"))
        .select("_median", "_iqr")
    )
    flag = F.when(
        (F.col("total_debit_amt").cast("double") > F.col("_median") + F.lit(3.0) * F.col("_iqr"))
        & (F.col("_iqr") > F.lit(0.0)),
        F.lit("Y"),
    ).otherwise(F.lit("N"))
    return (
        df.crossJoin(F.broadcast(stats))
        .withColumn("anomaly_flag", flag)
        .drop("_median", "_iqr")
    )


def transform(txn: DataFrame, config: PipelineConfig) -> DataFrame:
    """Build TRANSACTION_ANALYTICS (schema-enforced to the DDL)."""
    df = aggregate_customer(txn)
    df = add_trend_and_revenue(df)
    df = add_spend_percentile(df)
    df = add_anomaly_flag(df)
    df = (
        df
        .withColumn("reporting_period", F.lit(config.reporting_period))
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("effective_date", F.lit(config.run_date))
        .withColumn("load_ts", F.lit(load_timestamp()).cast("timestamp"))
    )
    return schemas.enforce_schema(df, schemas.TRANSACTION_ANALYTICS)


def run(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog | None = None,
) -> DataFrame:
    """Read STG_TXN_SUMMARY, transform, validate, and write TRANSACTION_ANALYTICS."""
    audit = audit or AuditLog(log_level=config.log_level)
    audit.log_step(JOB_NAME, "START", f"Period: {config.reporting_period}")

    txn = io.read_staging("STG_TXN_SUMMARY")

    out = transform(txn, config).cache()
    n = out.count()

    result = validate_table(
        out,
        TARGET,
        key_cols=["customer_id"],
        not_null=["customer_id", "reporting_period", "total_transactions"],
        min_rows=1,
        audit=audit,
    )
    abort_on_failure(result)

    io.write_data_product(out, TARGET)
    audit.run_log_row(JOB_NAME, n)
    audit.log_step(JOB_NAME, "SUCCESS", "Data product written", rowcount=n)
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build TRANSACTION_ANALYTICS")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--lake-dir", required=True)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args(argv)

    config = PipelineConfig.from_env().with_overrides(
        **({"run_date": _dt.date.fromisoformat(args.run_date)} if args.run_date else {})
    )
    spark = build_spark(JOB_NAME)
    io = LocalDataIO(spark, config, args.source_dir, args.lake_dir)
    run(spark, io, config)


if __name__ == "__main__":
    main()
