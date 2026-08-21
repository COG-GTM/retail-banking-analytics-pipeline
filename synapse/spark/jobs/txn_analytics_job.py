"""Synapse Spark job: TRANSACTION_ANALYTICS data product.

Migration of ``sas/02_sas_txn_analytics.sas`` (MBA-2208 / TICKET-07).

Reads ``STG_TXN_SUMMARY`` from Snowflake, aggregates account-level rows to
customer level, computes spend trend, SAS-compatible percentile buckets
(PROC RANK GROUPS=100, TIES=MEAN) and SAS-compatible quantiles / IQR based
anomaly bounds (PROC MEANS, QNTLDEF=5), validates the result and publishes
``TRANSACTION_ANALYTICS`` back to Snowflake.

Run on Synapse Spark:

    spark-submit txn_analytics_job.py --reporting-period 2026-08 \
        --sf-database ANALYTICS --sf-stg-schema ETL_STAGING --sf-dp-schema DATA_PRODUCTS
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, ShortType, StringType

JOB_NAME = "02_TXN_ANALYTICS"
MODEL_VERSION = "TXN_V2.1"
STG_TABLE = "STG_TXN_SUMMARY"
DP_TABLE = "TRANSACTION_ANALYTICS"
SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"

# Percentile groups produced by `proc rank groups=100`.
RANK_GROUPS = 100
# `%validate_table(..., min_rows=1000)` in the SAS program.
MIN_ROWS = 1000
NOT_NULL_COLUMNS = ("CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS")

OUTPUT_COLUMNS: Sequence[str] = (
    "CUSTOMER_ID",
    "REPORTING_PERIOD",
    "TOTAL_ACCOUNTS",
    "ACTIVE_ACCOUNTS",
    "TOTAL_TRANSACTIONS",
    "TOTAL_DEBIT_AMT",
    "TOTAL_CREDIT_AMT",
    "NET_CASH_FLOW",
    "AVG_TRANSACTION_SIZE",
    "MONTHLY_SPEND_TREND",
    "SPEND_PERCENTILE",
    "TOP_SPEND_CATEGORY",
    "DIGITAL_TXN_PCT",
    "FEE_INCOME",
    "INTEREST_INCOME",
    "REVENUE_CONTRIBUTION",
    "ANOMALY_FLAG",
    "MODEL_VERSION",
    "EFFECTIVE_DATE",
    "LOAD_TS",
)

OUTPUT_TYPES = {
    "CUSTOMER_ID": "bigint",
    "REPORTING_PERIOD": StringType(),
    "TOTAL_ACCOUNTS": ShortType(),
    "ACTIVE_ACCOUNTS": ShortType(),
    "TOTAL_TRANSACTIONS": IntegerType(),
    "TOTAL_DEBIT_AMT": DecimalType(18, 2),
    "TOTAL_CREDIT_AMT": DecimalType(18, 2),
    "NET_CASH_FLOW": DecimalType(18, 2),
    "AVG_TRANSACTION_SIZE": DecimalType(15, 2),
    "MONTHLY_SPEND_TREND": StringType(),
    "SPEND_PERCENTILE": DecimalType(5, 2),
    "TOP_SPEND_CATEGORY": StringType(),
    "DIGITAL_TXN_PCT": DecimalType(5, 2),
    "FEE_INCOME": DecimalType(15, 2),
    "INTEREST_INCOME": DecimalType(15, 2),
    "REVENUE_CONTRIBUTION": DecimalType(15, 2),
    "ANOMALY_FLAG": StringType(),
    "MODEL_VERSION": StringType(),
}

LOGGER = logging.getLogger(JOB_NAME)


class ValidationError(RuntimeError):
    """Raised when a pre-publish data quality check fails."""


@dataclass(frozen=True)
class SnowflakeConfig:
    """Connection settings for the Snowflake Spark connector."""

    url: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    stg_schema: str
    dp_schema: str

    def options(self, schema: str) -> Dict[str, str]:
        return {
            "sfUrl": self.url,
            "sfUser": self.user,
            "sfPassword": self.password,
            "sfRole": self.role,
            "sfWarehouse": self.warehouse,
            "sfDatabase": self.database,
            "sfSchema": schema,
        }


@dataclass(frozen=True)
class SasQuantiles:
    """Quantiles computed with the SAS default definition (QNTLDEF=5)."""

    q1: float
    median: float
    q3: float

    @property
    def iqr(self) -> float:
        return self.q3 - self.q1

    @property
    def tukey_lower(self) -> float:
        return self.q1 - 1.5 * self.iqr

    @property
    def tukey_upper(self) -> float:
        return self.q3 + 1.5 * self.iqr

    @property
    def sas_upper(self) -> float:
        """Upper bound used by the legacy SAS program: median + 3 * IQR."""
        return self.median + 3.0 * self.iqr


def log_step(status: str, msg: str = "", rowcount: Optional[int] = None) -> None:
    """Structured pipeline logging, mirroring the SAS %log_step macro."""
    parts = [f"[PIPELINE] {JOB_NAME} | {status}"]
    if msg:
        parts.append(msg)
    if rowcount is not None:
        parts.append(f"rows={rowcount}")
    LOGGER.info(" | ".join(parts))


def current_reporting_period(today: Optional[date] = None) -> str:
    """First day of the current month formatted as YYYY-MM (SAS yymmn7.)."""
    today = today or date.today()
    return f"{today.year:04d}-{today.month:02d}"


def aggregate_to_customer(txn_stg: DataFrame) -> DataFrame:
    """Aggregate account-level STG_TXN_SUMMARY rows to customer level.

    Equivalent of STEP 2 (`proc sql ... group by CUSTOMER_ID`) in the SAS
    program.
    """
    txn_count = F.sum("TXN_COUNT_TOTAL")
    debit = F.sum("AMT_TOTAL_DEBIT")
    credit = F.sum("AMT_TOTAL_CREDIT")
    digital_weighted = F.sum(
        F.col("TXN_COUNT_TOTAL")
        * (F.coalesce(F.col("PCT_WEB"), F.lit(0)) + F.coalesce(F.col("PCT_MOBILE"), F.lit(0)))
        / F.lit(100)
    )

    return txn_stg.groupBy("CUSTOMER_ID").agg(
        F.countDistinct("ACCOUNT_ID").alias("TOTAL_ACCOUNTS"),
        F.sum(F.when(F.col("DAYS_SINCE_LAST_TXN") <= 30, 1).otherwise(0)).alias("ACTIVE_ACCOUNTS"),
        txn_count.alias("TOTAL_TRANSACTIONS"),
        debit.alias("TOTAL_DEBIT_AMT"),
        credit.alias("TOTAL_CREDIT_AMT"),
        (credit - debit).alias("NET_CASH_FLOW"),
        F.when(txn_count > 0, F.sum(F.col("AMT_TOTAL_DEBIT") + F.col("AMT_TOTAL_CREDIT")) / txn_count)
        .otherwise(F.lit(0))
        .alias("AVG_TRANSACTION_SIZE"),
        F.sum("AMT_TOTAL_FEES").alias("TOTAL_FEES"),
        F.max("TOP_MERCHANT_CATEGORY").alias("TOP_SPEND_CATEGORY"),
        F.when(txn_count > 0, digital_weighted / txn_count * F.lit(100))
        .otherwise(F.lit(0))
        .alias("DIGITAL_TXN_PCT"),
    )


def add_spend_trend(cust_txn: DataFrame) -> DataFrame:
    """Derive spend trend and revenue components (SAS STEP 3)."""
    trend = (
        F.when(F.col("NET_CASH_FLOW") > F.col("AVG_TRANSACTION_SIZE") * 5, F.lit("UP"))
        .when(F.col("NET_CASH_FLOW") < -F.col("AVG_TRANSACTION_SIZE") * 5, F.lit("DOWN"))
        .otherwise(F.lit("STABLE"))
    )
    return (
        cust_txn.withColumn("MONTHLY_SPEND_TREND", trend)
        .withColumn("FEE_INCOME", F.col("TOTAL_FEES"))
        .withColumn("INTEREST_INCOME", F.col("TOTAL_DEBIT_AMT") * F.lit(0.02))
        .withColumn("REVENUE_CONTRIBUTION", F.col("FEE_INCOME") + F.col("TOTAL_DEBIT_AMT") * F.lit(0.02))
        .withColumn("ANOMALY_FLAG", F.lit("N"))
    )


def add_spend_percentile(
    df: DataFrame,
    value_col: str = "TOTAL_DEBIT_AMT",
    output_col: str = "SPEND_PERCENTILE",
    groups: int = RANK_GROUPS,
) -> DataFrame:
    """Reproduce `proc rank groups=<k>` with the SAS default TIES=MEAN.

    SAS assigns the group value ``floor(rank * k / (n + 1))`` where ``rank`` is
    the (tie-averaged) ordinary rank in ``1..n`` and ``n`` is the number of
    non-missing values. Tied values therefore share one averaged rank and always
    land in the same bucket. Rows with a missing value keep a NULL percentile,
    as SAS leaves them missing.
    """
    ordered = Window.orderBy(F.col(value_col).asc_nulls_last())
    ties = Window.partitionBy(value_col)
    non_null = Window.partitionBy(F.lit(1))

    min_rank = F.rank().over(ordered)
    tie_count = F.count(F.lit(1)).over(ties)
    mean_rank = min_rank + (tie_count - F.lit(1)) / F.lit(2)
    n_non_null = F.sum(F.when(F.col(value_col).isNotNull(), 1).otherwise(0)).over(non_null)

    bucket = F.floor(mean_rank * F.lit(groups) / (n_non_null + F.lit(1)))
    return df.withColumn(
        output_col, F.when(F.col(value_col).isNull(), F.lit(None).cast("double")).otherwise(bucket.cast("double"))
    )


def _values_at_positions(df: DataFrame, value_col: str, positions: Iterable[int]) -> Dict[int, float]:
    """Return the ordered (1-based) values of ``value_col`` at ``positions``."""
    wanted = sorted({p for p in positions})
    if not wanted:
        return {}
    ordered = Window.orderBy(F.col(value_col).asc_nulls_last())
    rows = (
        df.select(value_col)
        .where(F.col(value_col).isNotNull())
        .withColumn("_pos", F.row_number().over(ordered))
        .where(F.col("_pos").isin(wanted))
        .collect()
    )
    return {int(row["_pos"]): float(row[value_col]) for row in rows}


def sas_quantiles(df: DataFrame, value_col: str = "TOTAL_DEBIT_AMT") -> SasQuantiles:
    """Compute Q1 / median / Q3 using the SAS default definition QNTLDEF=5.

    QNTLDEF=5 is the empirical distribution function with averaging: with
    ``n`` non-missing values sorted ascending and ``j = n * p``, the quantile is
    ``(x[j] + x[j+1]) / 2`` when ``j`` is an integer and ``x[ceil(j)]``
    otherwise. This matches the MEDIAN and QRANGE statistics that
    ``proc means`` produced in the SAS program, and differs from Spark's
    ``percentile``/``approx_percentile`` (linear interpolation).
    """
    n = df.where(F.col(value_col).isNotNull()).count()
    if n == 0:
        raise ValidationError(f"cannot compute quantiles: {value_col} has no non-null values")

    probabilities = {"q1": 0.25, "median": 0.5, "q3": 0.75}
    needed: List[int] = []
    plans: Dict[str, Sequence[int]] = {}
    for name, p in probabilities.items():
        j = n * p
        if math.isclose(j, round(j), rel_tol=0.0, abs_tol=1e-9):
            lower = min(int(round(j)), n)
            plans[name] = (lower, min(lower + 1, n))
        else:
            plans[name] = (min(int(math.ceil(j)), n),)
        needed.extend(plans[name])

    values = _values_at_positions(df, value_col, needed)
    resolved = {name: sum(values[p] for p in pos) / len(pos) for name, pos in plans.items()}
    return SasQuantiles(q1=resolved["q1"], median=resolved["median"], q3=resolved["q3"])


def anomaly_bound(quantiles: SasQuantiles, method: str) -> float:
    """Upper anomaly threshold for the requested method."""
    if method == "sas_parity":
        return quantiles.sas_upper
    if method == "tukey":
        return quantiles.tukey_upper
    raise ValueError(f"unknown anomaly method: {method}")


def add_anomaly_flag(
    df: DataFrame,
    quantiles: SasQuantiles,
    method: str = "sas_parity",
    value_col: str = "TOTAL_DEBIT_AMT",
) -> DataFrame:
    """Flag outlying spend.

    ``sas_parity`` reproduces the legacy rule (``spend > median + 3 * IQR``);
    ``tukey`` applies the classic fences (``< Q1 - 1.5 * IQR`` or
    ``> Q3 + 1.5 * IQR``). Both are no-ops when the IQR is zero, matching the
    ``_IQR > 0`` guard in the SAS data step.
    """
    if quantiles.iqr <= 0:
        return df.withColumn("ANOMALY_FLAG", F.lit("N"))

    upper = F.col(value_col) > F.lit(anomaly_bound(quantiles, method))
    condition = upper
    if method == "tukey":
        condition = upper | (F.col(value_col) < F.lit(quantiles.tukey_lower))
    return df.withColumn("ANOMALY_FLAG", F.when(condition, F.lit("Y")).otherwise(F.lit("N")))


def finalize(df: DataFrame, reporting_period: str, run_ts: Optional[datetime] = None) -> DataFrame:
    """Add metadata columns and project to the TRANSACTION_ANALYTICS schema."""
    run_ts = run_ts or datetime.now()
    typed = (
        df.withColumn("REPORTING_PERIOD", F.lit(reporting_period))
        .withColumn("MODEL_VERSION", F.lit(MODEL_VERSION))
        .withColumn("EFFECTIVE_DATE", F.lit(run_ts.date()).cast("date"))
        .withColumn("LOAD_TS", F.lit(run_ts).cast("timestamp"))
    )
    for name, dtype in OUTPUT_TYPES.items():
        typed = typed.withColumn(name, F.col(name).cast(dtype))
    return typed.select(*OUTPUT_COLUMNS)


def validate(df: DataFrame, min_rows: int = MIN_ROWS, not_null: Sequence[str] = NOT_NULL_COLUMNS) -> int:
    """Row-count, key-uniqueness and null-rate checks (SAS %validate_table).

    Raises ``ValidationError`` so the Synapse activity fails loudly instead of
    publishing a partial data product.
    """
    row_count = df.count()
    if row_count < min_rows:
        raise ValidationError(f"{DP_TABLE} has {row_count} rows (minimum {min_rows})")

    duplicate_keys = df.groupBy("CUSTOMER_ID").count().where(F.col("count") > 1).count()
    if duplicate_keys > 0:
        raise ValidationError(f"{DP_TABLE} has {duplicate_keys} duplicate CUSTOMER_ID values")

    null_counts = df.select(
        [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in not_null]
    ).collect()[0]
    offenders = {c: int(null_counts[c]) for c in not_null if null_counts[c]}
    if offenders:
        raise ValidationError(f"{DP_TABLE} has NULLs in non-nullable columns: {offenders}")

    log_step("SUCCESS", "Validation passed", rowcount=row_count)
    return row_count


def build_transaction_analytics(
    txn_stg: DataFrame,
    reporting_period: str,
    anomaly_method: str = "sas_parity",
    run_ts: Optional[datetime] = None,
) -> DataFrame:
    """Full in-memory transformation, kept free of any Snowflake dependency."""
    customer_level = aggregate_to_customer(txn_stg)
    with_trend = add_spend_trend(customer_level)
    ranked = add_spend_percentile(with_trend).cache()
    quantiles = sas_quantiles(ranked)
    log_step(
        "SUCCESS",
        "Quantiles (QNTLDEF=5): "
        f"q1={quantiles.q1:.2f} median={quantiles.median:.2f} q3={quantiles.q3:.2f} "
        f"iqr={quantiles.iqr:.2f} bound={anomaly_bound(quantiles, anomaly_method):.2f} "
        f"method={anomaly_method}",
    )
    flagged = add_anomaly_flag(ranked, quantiles, anomaly_method)
    return finalize(flagged, reporting_period, run_ts)


def read_staging(spark: SparkSession, config: SnowflakeConfig) -> DataFrame:
    return (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**config.options(config.stg_schema))
        .option("dbtable", STG_TABLE)
        .load()
    )


def delete_reporting_period(spark: SparkSession, config: SnowflakeConfig, reporting_period: str) -> None:
    """Idempotent republish: clear the target period before appending."""
    utils = spark._jvm.net.snowflake.spark.snowflake.Utils
    options = spark._jvm.PythonUtils.toScalaMap(config.options(config.dp_schema))
    statement = (
        f"DELETE FROM {config.database}.{config.dp_schema}.{DP_TABLE} "
        f"WHERE REPORTING_PERIOD = '{reporting_period}'"
    )
    utils.runQuery(options, statement)


def write_data_product(df: DataFrame, config: SnowflakeConfig) -> None:
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**config.options(config.dp_schema))
        .option("dbtable", DP_TABLE)
        .mode("append")
        .save()
    )


def snowflake_config_from_env(args: argparse.Namespace) -> SnowflakeConfig:
    """Read connection settings from the environment (Key Vault backed)."""

    def required(name: str) -> str:
        value = os.environ.get(name)
        if not value:
            raise ValidationError(f"missing required environment variable {name}")
        return value

    return SnowflakeConfig(
        url=required("SF_URL"),
        user=required("SF_USER"),
        password=required("SF_PASSWORD"),
        role=os.environ.get("SF_ROLE", "ETL_PIPELINE_ROLE"),
        warehouse=os.environ.get("SF_WAREHOUSE", "ETL_WH"),
        database=args.sf_database,
        stg_schema=args.sf_stg_schema,
        dp_schema=args.sf_dp_schema,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the TRANSACTION_ANALYTICS data product")
    parser.add_argument("--reporting-period", default=None, help="YYYY-MM (defaults to current month)")
    parser.add_argument("--sf-database", default=os.environ.get("SF_DATABASE", "ANALYTICS"))
    parser.add_argument("--sf-stg-schema", default=os.environ.get("SF_STG_SCHEMA", "ETL_STAGING"))
    parser.add_argument("--sf-dp-schema", default=os.environ.get("SF_DP_SCHEMA", "DATA_PRODUCTS"))
    parser.add_argument("--anomaly-method", choices=("sas_parity", "tukey"), default="sas_parity")
    parser.add_argument("--min-rows", type=int, default=MIN_ROWS)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)
    reporting_period = args.reporting_period or current_reporting_period()
    config = snowflake_config_from_env(args)

    spark = SparkSession.builder.appName(f"{JOB_NAME}_{reporting_period}").getOrCreate()
    try:
        log_step("START", f"Period: {reporting_period}")
        txn_stg = read_staging(spark, config)
        analytics = build_transaction_analytics(
            txn_stg, reporting_period, anomaly_method=args.anomaly_method
        ).cache()
        row_count = validate(analytics, min_rows=args.min_rows)

        log_step("START", f"Loading {config.dp_schema}.{DP_TABLE}")
        delete_reporting_period(spark, config, reporting_period)
        write_data_product(analytics, config)
        log_step("SUCCESS", "Pipeline complete", rowcount=row_count)
    except ValidationError as exc:
        log_step("ERROR", str(exc))
        raise
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
