"""Azure Synapse Spark job: TRANSACTION_ANALYTICS data product.

Migrated from sas/02_sas_txn_analytics.sas (Base SAS + SAS/STAT).

Reads ETL_STAGING.STG_TXN_SUMMARY from Snowflake, aggregates account-level rows
to customer level, derives spend-trend and revenue metrics, reproduces the SAS
PROC RANK percentile buckets and the PROC MEANS IQR anomaly bounds, validates
the result and publishes DATA_PRODUCTS.TRANSACTION_ANALYTICS back to Snowflake.

SAS -> PySpark mapping notes live in
docs/modernization/TICKET-07_txn_analytics_synapse.md.

Run on Synapse Spark:

    spark-submit txn_analytics_job.py \
        --env DEV --snowflake-secret-scope retail-banking-kv

Local reconciliation run against the CSV extracts in data/:

    spark-submit txn_analytics_job.py --io local \
        --input data/02_bteq_staging/stg_txn_summary.csv \
        --output /tmp/transaction_analytics
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, StringType, TimestampType

LOGGER = logging.getLogger("txn_analytics")

MODEL_VERSION = "TXN_V2.1"
SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
STAGING_TABLE = "ETL_STAGING.STG_TXN_SUMMARY"
TARGET_TABLE = "DATA_PRODUCTS.TRANSACTION_ANALYTICS"

# Column order of DATA_PRODUCTS.TRANSACTION_ANALYTICS (ddl/snowflake/03_data_product_tables.sql,
# owned by TICKET-01; identical column list to the Teradata ddl/02_data_product_tables.sql).
TARGET_COLUMNS: Dict[str, object] = {
    "CUSTOMER_ID": DecimalType(19, 0),
    "REPORTING_PERIOD": StringType(),
    "TOTAL_ACCOUNTS": DecimalType(5, 0),
    "ACTIVE_ACCOUNTS": DecimalType(5, 0),
    "TOTAL_TRANSACTIONS": DecimalType(10, 0),
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
    "EFFECTIVE_DATE": DateType(),
    "LOAD_TS": TimestampType(),
}

INTEREST_RATE_PROXY = 0.02
TREND_SENSITIVITY = 5


class ValidationError(RuntimeError):
    """Raised when a pre-publish data quality gate fails."""


@dataclass(frozen=True)
class IqrStats:
    """Quantile statistics computed with the SAS QNTLDEF=5 definition."""

    n: int
    q1: Optional[float]
    median: Optional[float]
    q3: Optional[float]

    @property
    def iqr(self) -> Optional[float]:
        if self.q1 is None or self.q3 is None:
            return None
        return self.q3 - self.q1

    @property
    def lower_fence(self) -> Optional[float]:
        iqr = self.iqr
        return None if iqr is None else self.q1 - 1.5 * iqr

    @property
    def upper_fence(self) -> Optional[float]:
        iqr = self.iqr
        return None if iqr is None else self.q3 + 1.5 * iqr

    @property
    def sas_upper_bound(self) -> Optional[float]:
        """median + 3*IQR, the bound the SAS program flags on."""
        iqr = self.iqr
        return None if iqr is None or self.median is None else self.median + 3 * iqr


# --------------------------------------------------------------------------- #
# Transformations
# --------------------------------------------------------------------------- #
def aggregate_customer_txn(stg_txn: DataFrame) -> DataFrame:
    """STEP 2 of the SAS program: account-level staging -> customer level.

    Mirrors the PROC SQL GROUP BY, including its quirks: TOP_SPEND_CATEGORY is
    the lexical MAX of the per-account categories, and DIGITAL_TXN_PCT is a
    transaction-count weighted mean of PCT_WEB + PCT_MOBILE.
    """
    txn_count = F.sum("TXN_COUNT_TOTAL")
    debit = F.sum("AMT_TOTAL_DEBIT")
    credit = F.sum("AMT_TOTAL_CREDIT")
    weighted_digital = F.sum(F.col("TXN_COUNT_TOTAL") * (F.col("PCT_WEB") + F.col("PCT_MOBILE")) / F.lit(100))

    return stg_txn.groupBy("CUSTOMER_ID").agg(
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
        F.when(txn_count > 0, weighted_digital / txn_count * F.lit(100))
        .otherwise(F.lit(0))
        .alias("DIGITAL_TXN_PCT"),
    )


def add_spend_trend(cust_txn: DataFrame) -> DataFrame:
    """STEP 3: spend-trend classification and the revenue components."""
    threshold = F.col("AVG_TRANSACTION_SIZE") * F.lit(TREND_SENSITIVITY)
    fee_income = F.col("TOTAL_FEES")
    interest_income = F.col("TOTAL_DEBIT_AMT") * F.lit(INTEREST_RATE_PROXY)

    return (
        cust_txn.withColumn(
            "MONTHLY_SPEND_TREND",
            F.when(F.col("NET_CASH_FLOW") > threshold, F.lit("UP"))
            .when(F.col("NET_CASH_FLOW") < -threshold, F.lit("DOWN"))
            .otherwise(F.lit("STABLE")),
        )
        .withColumn("FEE_INCOME", fee_income)
        .withColumn("INTEREST_INCOME", interest_income)
        .withColumn("REVENUE_CONTRIBUTION", fee_income + interest_income)
        .withColumn("ANOMALY_FLAG", F.lit("N"))
    )


def add_proc_rank_groups(df: DataFrame, value_col: str, out_col: str, groups: int = 100) -> DataFrame:
    """STEP 4: PROC RANK GROUPS=<groups> equivalent.

    SAS computes ``FLOOR(rank * groups / (n + 1))`` where ``rank`` is the
    ordinary rank of the value and ties share their mean rank (TIES=MEAN, the
    PROC RANK default). Missing values keep a missing rank. The result is a
    0-based bucket, i.e. 0..groups-1.
    """
    ordered = Window.orderBy(F.col(value_col).asc_nulls_last())
    tie_group = Window.partitionBy(value_col)
    non_null = Window.partitionBy(F.lit(1))

    min_rank = F.rank().over(ordered)
    tie_count = F.count(F.lit(1)).over(tie_group)
    mean_rank = min_rank + (tie_count - F.lit(1)) / F.lit(2)
    n_non_null = F.count(value_col).over(non_null)

    return df.withColumn(
        out_col,
        F.when(
            F.col(value_col).isNotNull(),
            F.floor(mean_rank * F.lit(groups) / (n_non_null + F.lit(1))),
        ),
    )


def sas_quantiles(df: DataFrame, value_col: str, probs: Sequence[float]) -> Dict[float, Optional[float]]:
    """Exact quantiles using the SAS default definition QNTLDEF=5.

    Definition 5 (empirical distribution with averaging): with ``np = n * p``
    over the ``n`` non-missing values sorted ascending,

    * ``np`` integral  -> ``(x[np] + x[np + 1]) / 2`` (1-based order statistics),
      falling back to ``x[n]`` when ``np == n``;
    * otherwise        -> ``x[ceil(np)]``.

    PROC MEANS MEDIAN/QRANGE use the same definition, so Q1/Q3/median and the
    resulting IQR reproduce the SAS numbers exactly rather than approximately
    (Spark's ``percentile_approx`` and ``percentile`` both use other
    definitions).
    """
    values = df.select(F.col(value_col).cast("double").alias("v")).where(F.col("v").isNotNull())
    n = values.count()
    if n == 0:
        return {p: None for p in probs}

    wanted: Dict[float, List[int]] = {}
    needed: set[int] = set()
    for p in probs:
        np_ = n * p
        if abs(np_ - round(np_)) < 1e-9:
            k = int(round(np_))
            idx = [k, k + 1] if k < n else [n]
        else:
            idx = [int(math.ceil(np_))]
        wanted[p] = idx
        needed.update(idx)

    ranked = values.withColumn("rn", F.row_number().over(Window.orderBy(F.col("v").asc())))
    rows = ranked.where(F.col("rn").isin(sorted(needed))).collect()
    by_index = {row["rn"]: float(row["v"]) for row in rows}

    return {p: sum(by_index[i] for i in idx) / len(idx) for p, idx in wanted.items()}


def compute_iqr_stats(df: DataFrame, value_col: str) -> IqrStats:
    """STEP 5a: the PROC MEANS ``median=`` / ``qrange=`` output."""
    q = sas_quantiles(df, value_col, (0.25, 0.5, 0.75))
    return IqrStats(
        n=df.where(F.col(value_col).isNotNull()).count(),
        q1=q[0.25],
        median=q[0.5],
        q3=q[0.75],
    )


def flag_anomalies(
    df: DataFrame,
    stats: IqrStats,
    value_col: str = "TOTAL_DEBIT_AMT",
    rule: str = "sas_median_3iqr",
) -> DataFrame:
    """STEP 5b: set ANOMALY_FLAG from the IQR bounds.

    ``sas_median_3iqr`` (default) reproduces the legacy SAS rule
    ``value > median + 3 * IQR``. ``tukey_fences`` uses the classic
    ``Q1 - 1.5*IQR`` / ``Q3 + 1.5*IQR`` outlier fences; both are computed from
    the same QNTLDEF=5 quantiles, so switching rules cannot change the
    quantiles themselves.
    """
    iqr = stats.iqr
    if iqr is None or iqr <= 0:
        LOGGER.warning("IQR is %s for %s - no anomalies flagged", iqr, value_col)
        return df.withColumn("ANOMALY_FLAG", F.lit("N"))

    if rule == "sas_median_3iqr":
        condition: Column = F.col(value_col) > F.lit(stats.sas_upper_bound)
    elif rule == "tukey_fences":
        condition = (F.col(value_col) > F.lit(stats.upper_fence)) | (
            F.col(value_col) < F.lit(stats.lower_fence)
        )
    else:
        raise ValueError(f"Unknown anomaly rule: {rule}")

    return df.withColumn("ANOMALY_FLAG", F.when(condition, F.lit("Y")).otherwise(F.lit("N")))


def add_metadata(df: DataFrame, reporting_period: str, effective_date: date, load_ts: datetime) -> DataFrame:
    """STEP 5c: reporting period / model version / audit timestamps."""
    return (
        df.withColumn("REPORTING_PERIOD", F.lit(reporting_period))
        .withColumn("MODEL_VERSION", F.lit(MODEL_VERSION))
        .withColumn("EFFECTIVE_DATE", F.lit(effective_date))
        .withColumn("LOAD_TS", F.lit(load_ts))
    )


def conform_to_target(df: DataFrame) -> DataFrame:
    """Project to the published TRANSACTION_ANALYTICS column list and types."""
    return df.select(*[F.col(name).cast(dtype).alias(name) for name, dtype in TARGET_COLUMNS.items()])


def build_transaction_analytics(
    stg_txn: DataFrame,
    reporting_period: str,
    effective_date: date,
    load_ts: datetime,
    anomaly_rule: str = "sas_median_3iqr",
) -> tuple[DataFrame, IqrStats]:
    """Full SAS STEP 2-5 chain."""
    cust_txn = add_spend_trend(aggregate_customer_txn(stg_txn))
    ranked = add_proc_rank_groups(cust_txn, "TOTAL_DEBIT_AMT", "SPEND_PERCENTILE").cache()
    stats = compute_iqr_stats(ranked, "TOTAL_DEBIT_AMT")
    LOGGER.info(
        "TOTAL_DEBIT_AMT stats (QNTLDEF=5): n=%s q1=%s median=%s q3=%s iqr=%s " "tukey=[%s, %s] sas_bound=%s",
        stats.n,
        stats.q1,
        stats.median,
        stats.q3,
        stats.iqr,
        stats.lower_fence,
        stats.upper_fence,
        stats.sas_upper_bound,
    )
    flagged = flag_anomalies(ranked, stats, rule=anomaly_rule)
    final = conform_to_target(add_metadata(flagged, reporting_period, effective_date, load_ts))
    return final, stats


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def validate(
    df: DataFrame,
    min_rows: int = 1000,
    key_col: str = "CUSTOMER_ID",
    not_null: Iterable[str] = ("CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS"),
    max_null_rate: float = 0.0,
) -> int:
    """Port of %validate_table: row count, key uniqueness and null rates.

    Raises ValidationError so the Synapse activity fails loudly instead of
    publishing a bad table (the SAS program used %ABORT CANCEL).
    """
    row_count = df.count()
    LOGGER.info("Validation: row_count=%s (min_rows=%s)", row_count, min_rows)
    failures: List[str] = []

    if row_count < min_rows:
        failures.append(f"row count {row_count} below minimum {min_rows}")

    duplicate_keys = df.groupBy(key_col).count().where(F.col("count") > 1).count() if row_count else 0
    if duplicate_keys:
        failures.append(f"{duplicate_keys} duplicate {key_col} values")

    if row_count:
        null_counts = df.select(
            *[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in not_null]
        ).collect()[0]
        for column in not_null:
            null_rate = (null_counts[column] or 0) / row_count
            LOGGER.info("Validation: null_rate(%s)=%.6f", column, null_rate)
            if null_rate > max_null_rate:
                failures.append(f"null rate {null_rate:.6f} on {column} exceeds {max_null_rate}")

    if failures:
        raise ValidationError("TRANSACTION_ANALYTICS validation failed: " + "; ".join(failures))

    LOGGER.info("Validation passed for %s rows", row_count)
    return row_count


# --------------------------------------------------------------------------- #
# Snowflake / local IO
# --------------------------------------------------------------------------- #
def snowflake_options(args: argparse.Namespace) -> Dict[str, str]:
    """Snowflake Spark connector options.

    Credentials come from Azure Key Vault through the Synapse token library
    (``mssparkutils``); nothing secret is ever baked into the job or the
    pipeline JSON.
    """
    from notebookutils import mssparkutils  # noqa: PLC0415  (Synapse runtime only)

    scope = args.snowflake_secret_scope
    return {
        "sfURL": mssparkutils.credentials.getSecret(scope, "snowflake-url"),
        "sfUser": mssparkutils.credentials.getSecret(scope, "snowflake-user"),
        "pem_private_key": mssparkutils.credentials.getSecret(scope, "snowflake-private-key"),
        "sfDatabase": f"RETAIL_BANKING_{args.env}",
        "sfWarehouse": args.snowflake_warehouse,
        "sfRole": args.snowflake_role,
        "sfSchema": "ETL_STAGING",
    }


def read_staging(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.io == "local":
        csv = spark.read.option("header", True).option("inferSchema", True).csv(args.input)
        return csv.toDF(*[c.upper() for c in csv.columns])
    options = snowflake_options(args)
    LOGGER.info("Reading %s.%s", options["sfDatabase"], STAGING_TABLE)
    return spark.read.format(SNOWFLAKE_SOURCE).options(**options).option("dbtable", STAGING_TABLE).load()


def write_data_product(df: DataFrame, args: argparse.Namespace, reporting_period: str) -> None:
    """Publish with delete-then-append, matching the SAS DELETE + PROC APPEND."""
    if args.io == "local":
        df.write.mode("overwrite").option("header", True).csv(args.output)
        LOGGER.info("Wrote local output to %s", args.output)
        return

    options = snowflake_options(args)
    period = reporting_period.replace("'", "''")
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**options)
        .option("dbtable", TARGET_TABLE)
        .option(
            "preactions",
            f"DELETE FROM {TARGET_TABLE} WHERE REPORTING_PERIOD = '{period}'",
        )
        .mode("append")
        .save()
    )
    LOGGER.info("Published %s for period %s", TARGET_TABLE, reporting_period)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def default_reporting_period(today: date) -> str:
    """SAS: intnx(month, today(), 0, beginning) formatted yymmn7. -> YYYY-MM."""
    return today.strftime("%Y-%m")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRANSACTION_ANALYTICS Synapse Spark job")
    parser.add_argument("--env", default="DEV", choices=["DEV", "UAT", "PROD"])
    parser.add_argument("--io", default="snowflake", choices=["snowflake", "local"])
    parser.add_argument("--reporting-period", help="YYYY-MM; defaults to the current month")
    parser.add_argument("--snowflake-secret-scope", default="retail-banking-kv")
    parser.add_argument("--snowflake-warehouse", default="RETAIL_BANKING_ETL_WH")
    parser.add_argument("--snowflake-role", default="RETAIL_BANKING_ETL_ROLE")
    parser.add_argument(
        "--anomaly-rule", default="sas_median_3iqr", choices=["sas_median_3iqr", "tukey_fences"]
    )
    parser.add_argument("--min-rows", type=int, default=1000)
    parser.add_argument("--max-null-rate", type=float, default=0.0)
    parser.add_argument("--input", help="local mode: STG_TXN_SUMMARY CSV path")
    parser.add_argument("--output", help="local mode: output directory")
    args = parser.parse_args(argv)
    if args.io == "local" and not (args.input and args.output):
        parser.error("--io local requires --input and --output")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s", stream=sys.stdout
    )
    args = parse_args(argv)
    run_ts = datetime.now()
    reporting_period = args.reporting_period or default_reporting_period(run_ts.date())

    spark = SparkSession.builder.appName(f"txn_analytics_{reporting_period}").getOrCreate()
    try:
        LOGGER.info("02_TXN_ANALYTICS START period=%s env=%s", reporting_period, args.env)
        stg_txn = read_staging(spark, args)
        final, _stats = build_transaction_analytics(
            stg_txn,
            reporting_period=reporting_period,
            effective_date=run_ts.date(),
            load_ts=run_ts,
            anomaly_rule=args.anomaly_rule,
        )
        final.cache()
        row_count = validate(final, min_rows=args.min_rows, max_null_rate=args.max_null_rate)
        write_data_product(final, args, reporting_period)
        LOGGER.info("02_TXN_ANALYTICS SUCCESS rowcount=%s", row_count)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
