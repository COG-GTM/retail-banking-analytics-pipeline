"""Golden record assembly - migrated from sas/04_sas_data_products.sas.

Builds DATA_PRODUCTS.CUSTOMER_MASTER_PROFILE by joining the three upstream
data products onto the STG_CUSTOMER_360 base. The SAS data step MERGE with
``IN=`` variables becomes a chain of PySpark left joins from the base
(``if _base;``), with the same per-source default handling applied wherever a
member contributes no row.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_utils.config import PipelineConfig, load_config
from pipeline_utils.run_log import ERROR, START, SUCCESS, RunLogger
from pipeline_utils.secrets import KeyVaultSecretResolver
from pipeline_utils.snowflake_io import SnowflakeIO
from pipeline_utils.validation import ValidationError, validate_table

JOB_NAME = "04_MASTER_PROFILE"
MODEL_VERSION = "MASTER_V1.5"

SEGMENT_DEFAULTS: dict[str, object] = {
    "SEGMENT_NAME": "UNCLASSIFIED",
    "LIFETIME_VALUE_SCORE": 0,
    "ENGAGEMENT_SCORE": 0,
    "CROSS_SELL_FLAG": "N",
    "UPSELL_FLAG": "N",
    "RETENTION_RISK_FLAG": "N",
}
TXN_DEFAULTS: dict[str, object] = {
    "MONTHLY_TRANSACTIONS": 0,
    "MONTHLY_SPEND": 0,
    "NET_CASH_FLOW": 0,
    "TOP_SPEND_CATEGORY": "",
    "DIGITAL_TXN_PCT": 0,
}
RISK_DEFAULTS: dict[str, object] = {
    "COMPOSITE_RISK_SCORE": None,
    "RISK_TIER": "UNKNOWN",
    "PROBABILITY_OF_DEFAULT": None,
    "WATCH_LIST_FLAG": "N",
}

MASTER_PROFILE_COLUMNS = [
    "CUSTOMER_ID",
    "FULL_NAME",
    "AGE",
    "STATE_CODE",
    "CUSTOMER_SINCE",
    "TENURE_MONTHS",
    "CUSTOMER_STATUS",
    "SEGMENT_NAME",
    "LIFETIME_VALUE_SCORE",
    "ENGAGEMENT_SCORE",
    "TOTAL_ACCOUNTS",
    "ACTIVE_ACCOUNTS",
    "TOTAL_BALANCE",
    "TOTAL_CREDIT_LIMIT",
    "CREDIT_UTILIZATION_PCT",
    "MONTHLY_TRANSACTIONS",
    "MONTHLY_SPEND",
    "NET_CASH_FLOW",
    "TOP_SPEND_CATEGORY",
    "DIGITAL_TXN_PCT",
    "COMPOSITE_RISK_SCORE",
    "RISK_TIER",
    "PROBABILITY_OF_DEFAULT",
    "WATCH_LIST_FLAG",
    "CROSS_SELL_FLAG",
    "UPSELL_FLAG",
    "RETENTION_RISK_FLAG",
    "MODEL_VERSION",
    "EFFECTIVE_DATE",
    "LOAD_TS",
]

LOGGER = logging.getLogger("retail_banking.pipeline")


def select_base(stg_customer_360: DataFrame) -> DataFrame:
    """WORK.BASE: active customers with their account-level attributes."""
    return stg_customer_360.where(F.col("CUSTOMER_STATUS") == "A").select(
        F.col("CUSTOMER_ID"),
        F.concat_ws(
            " ", F.trim(F.col("FIRST_NAME")), F.trim(F.col("LAST_NAME"))
        ).alias("FULL_NAME"),
        F.col("AGE"),
        F.col("STATE_CODE"),
        F.col("CUSTOMER_SINCE"),
        F.col("TENURE_MONTHS"),
        F.col("CUSTOMER_STATUS"),
        F.col("NUM_ACCOUNTS").alias("TOTAL_ACCOUNTS"),
        F.col("NUM_ACTIVE_ACCOUNTS").alias("ACTIVE_ACCOUNTS"),
        F.col("TOTAL_BALANCE"),
        F.col("TOTAL_CREDIT_LIMIT"),
        F.col("CREDIT_UTILIZATION_PCT"),
    )


def select_segments(customer_segments: DataFrame) -> DataFrame:
    return customer_segments.select(
        "CUSTOMER_ID",
        "SEGMENT_NAME",
        "LIFETIME_VALUE_SCORE",
        "ENGAGEMENT_SCORE",
        "CROSS_SELL_FLAG",
        "UPSELL_FLAG",
        "RETENTION_RISK_FLAG",
    )


def select_txn(
    transaction_analytics: DataFrame, effective_date: dt.date
) -> DataFrame:
    """WORK.TXN: current period only (``where EFFECTIVE_DATE = today()``)."""
    return transaction_analytics.where(
        F.col("EFFECTIVE_DATE") == F.lit(effective_date).cast("date")
    ).select(
        F.col("CUSTOMER_ID"),
        F.col("TOTAL_TRANSACTIONS").alias("MONTHLY_TRANSACTIONS"),
        F.col("TOTAL_DEBIT_AMT").alias("MONTHLY_SPEND"),
        F.col("NET_CASH_FLOW"),
        F.col("TOP_SPEND_CATEGORY"),
        F.col("DIGITAL_TXN_PCT"),
    )


def select_risk(customer_risk_scores: DataFrame) -> DataFrame:
    return customer_risk_scores.select(
        "CUSTOMER_ID",
        "COMPOSITE_RISK_SCORE",
        "RISK_TIER",
        "PROBABILITY_OF_DEFAULT",
        "WATCH_LIST_FLAG",
    )


def _left_join_with_defaults(
    left: DataFrame, right: DataFrame, defaults: dict[str, object]
) -> DataFrame:
    """Left join on CUSTOMER_ID, applying ``if not _member then do; ... end;``.

    Defaults are applied to the whole member block when the member contributes
    no row, exactly like the SAS ``IN=`` flag test - not per column - so a
    genuinely NULL column on a matched row keeps its NULL.
    """
    marker = "_member_present"
    right_marked = right.withColumn(marker, F.lit(True))
    joined = left.join(right_marked, on="CUSTOMER_ID", how="left")
    present = F.col(marker).isNotNull()
    for column, default in defaults.items():
        joined = joined.withColumn(
            column,
            F.when(present, F.col(column)).otherwise(
                F.lit(default).cast(joined.schema[column].dataType)
            ),
        )
    return joined.drop(marker)


def build_master_profile(
    base: DataFrame,
    segments: DataFrame,
    txn: DataFrame,
    risk: DataFrame,
    effective_date: dt.date,
    load_ts: dt.datetime | None = None,
    model_version: str = MODEL_VERSION,
) -> DataFrame:
    """The 4-way MERGE ... BY CUSTOMER_ID; if _base; equivalent."""
    profile = _left_join_with_defaults(base, segments, SEGMENT_DEFAULTS)
    profile = _left_join_with_defaults(profile, txn, TXN_DEFAULTS)
    profile = _left_join_with_defaults(profile, risk, RISK_DEFAULTS)

    load_ts_col = (
        F.lit(load_ts).cast("timestamp")
        if load_ts is not None
        else F.current_timestamp()
    )
    profile = (
        profile.withColumn("MODEL_VERSION", F.lit(model_version))
        .withColumn("EFFECTIVE_DATE", F.lit(effective_date).cast("date"))
        .withColumn("LOAD_TS", load_ts_col)
    )
    return profile.select(*MASTER_PROFILE_COLUMNS)


def completeness_report(master_profile: DataFrame) -> dict[str, int]:
    """The SAS "Master Profile - Completeness Check" metrics."""
    def _flag(column: str, value: str = "Y"):
        return F.sum((F.col(column) == value).cast("long"))

    row = master_profile.select(
        F.count(F.lit(1)).alias("TOTAL"),
        F.sum((F.col("SEGMENT_NAME") != "UNCLASSIFIED").cast("long")).alias(
            "HAS_SEGMENT"
        ),
        F.sum((F.col("MONTHLY_TRANSACTIONS") > 0).cast("long")).alias("HAS_TXN"),
        F.sum((F.col("RISK_TIER") != "UNKNOWN").cast("long")).alias(
            "HAS_RISK_SCORE"
        ),
        _flag("CROSS_SELL_FLAG").alias("CROSS_SELL_ELIGIBLE"),
        _flag("UPSELL_FLAG").alias("UPSELL_ELIGIBLE"),
        _flag("RETENTION_RISK_FLAG").alias("RETENTION_AT_RISK"),
        _flag("WATCH_LIST_FLAG").alias("ON_WATCH_LIST"),
    ).collect()[0]
    return {name: int(row[name] or 0) for name in row.asDict()}


def segment_distribution(master_profile: DataFrame) -> DataFrame:
    return (
        master_profile.groupBy("SEGMENT_NAME")
        .agg(
            F.count(F.lit(1)).alias("N"),
            F.round(F.avg("LIFETIME_VALUE_SCORE"), 2).alias("AVG_LTV"),
        )
        .orderBy(F.col("N").desc())
    )


def risk_tier_distribution(master_profile: DataFrame) -> DataFrame:
    return (
        master_profile.groupBy("RISK_TIER")
        .agg(
            F.count(F.lit(1)).alias("N"),
            F.round(F.avg("COMPOSITE_RISK_SCORE"), 2).alias("AVG_SCORE"),
        )
        .orderBy(F.col("AVG_SCORE").desc_nulls_last())
    )


def run(
    spark: SparkSession,
    config: PipelineConfig,
    io: SnowflakeIO,
    run_logger: RunLogger,
    effective_date: dt.date,
    min_rows: int,
) -> int:
    """Execute the job end to end; returns the master profile row count."""
    sf = config.snowflake
    run_logger.log_step(JOB_NAME, START, "Building golden record")

    run_logger.log_step(JOB_NAME, START, "Extracting upstream data products")
    base = select_base(io.read_table(sf.staging_schema, "STG_CUSTOMER_360"))
    segments = select_segments(
        io.read_table(sf.data_products_schema, "CUSTOMER_SEGMENTS")
    )
    txn = select_txn(
        io.read_table(sf.data_products_schema, "TRANSACTION_ANALYTICS"),
        effective_date,
    )
    risk = select_risk(
        io.read_table(sf.data_products_schema, "CUSTOMER_RISK_SCORES")
    )
    run_logger.log_step(JOB_NAME, SUCCESS, "All upstream data extracted")

    run_logger.log_step(JOB_NAME, START, "Merging data products")
    master_profile = build_master_profile(
        base, segments, txn, risk, effective_date
    ).cache()
    row_count = master_profile.count()
    run_logger.log_step(
        JOB_NAME, SUCCESS, "Master profile built", rowcount=row_count
    )

    LOGGER.info("Master Profile - Segment Distribution")
    segment_distribution(master_profile).show(truncate=False)
    LOGGER.info("Master Profile - Risk Tier Distribution")
    risk_tier_distribution(master_profile).show(truncate=False)
    metrics = completeness_report(master_profile)
    LOGGER.info("Master Profile - Completeness Check: %s", metrics)

    report = validate_table(
        master_profile,
        table=config.qualified(sf.data_products_schema, "CUSTOMER_MASTER_PROFILE"),
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID", "FULL_NAME", "CUSTOMER_STATUS"],
        min_rows=min_rows,
    )
    LOGGER.info("%s", report.summary())
    if not report.passed:
        run_logger.log_step(
            JOB_NAME,
            ERROR,
            f"Validation failed: {report.failures[0].name}",
            rowcount=row_count,
        )
        report.abort_if_failed()

    run_logger.log_step(
        JOB_NAME, START, "Loading DATA_PRODUCTS.CUSTOMER_MASTER_PROFILE"
    )
    io.overwrite_table(
        master_profile, sf.data_products_schema, "CUSTOMER_MASTER_PROFILE"
    )
    run_logger.log_step(
        JOB_NAME, SUCCESS, "Golden record loaded", rowcount=row_count
    )
    run_logger.log_step(JOB_NAME, SUCCESS, "Full pipeline complete")
    master_profile.unpersist()
    return row_count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--effective-date",
        type=dt.date.fromisoformat,
        default=dt.datetime.now(dt.timezone.utc).date(),
        help="Run date (defaults to today, the SAS today() equivalent)",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=1000,
        help="Minimum acceptable master profile row count",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args(argv)
    config = load_config()
    spark = SparkSession.builder.appName(
        "retail-banking-master-profile"
    ).getOrCreate()
    io = SnowflakeIO(
        spark, config, KeyVaultSecretResolver(config.key_vault_url)
    )
    run_logger = RunLogger(spark, JOB_NAME, config, io)
    try:
        run(spark, config, io, run_logger, args.effective_date, args.min_rows)
    except ValidationError as exc:
        LOGGER.error("Aborting run: %s", exc)
        run_logger.log_step(JOB_NAME, ERROR, str(exc)[:200])
        return 1
    finally:
        run_logger.flush()
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
