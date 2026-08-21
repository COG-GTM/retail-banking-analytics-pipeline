"""Golden record assembly on Synapse Spark.

PySpark port of ``sas/04_sas_data_products.sas``: builds
``CUSTOMER_MASTER_PROFILE`` from the three upstream data products
(``CUSTOMER_SEGMENTS``, ``TRANSACTION_ANALYTICS``, ``CUSTOMER_RISK_SCORES``)
plus the ``STG_CUSTOMER_360`` staging table.

SAS -> PySpark mapping
----------------------
``data ...; merge A(in=_base) B(in=_seg) C(in=_txn) D(in=_risk); by CUSTOMER_ID;
if _base;`` becomes a chain of left joins from the base DataFrame. Each right
side carries a boolean marker column so that the ``if not _seg then do; ... end;``
default blocks apply only when the customer is absent from that member — a
customer present with NULL measures keeps its NULLs, exactly as in SAS.
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timezone
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_utils.config import PipelineConfig
from pipeline_utils.run_log import RunLogger, RunLogSink, Status
from pipeline_utils.snowflake_io import SnowflakeIO
from pipeline_utils.validation import ValidationError, validate_dataframe

LOGGER = logging.getLogger(__name__)

JOB_NAME = "04_MASTER_PROFILE"
MODEL_VERSION = "MASTER_V1.5"
MIN_ROWS = 1000

# Defaults applied when a customer is absent from a merge member, mirroring the
# `if not _seg / _txn / _risk then do; ... end;` blocks in the SAS data step.
SEGMENT_DEFAULTS = {
    "SEGMENT_NAME": "UNCLASSIFIED",
    "LIFETIME_VALUE_SCORE": 0,
    "ENGAGEMENT_SCORE": 0,
    "CROSS_SELL_FLAG": "N",
    "UPSELL_FLAG": "N",
    "RETENTION_RISK_FLAG": "N",
}

TXN_DEFAULTS = {
    "MONTHLY_TRANSACTIONS": 0,
    "MONTHLY_SPEND": 0,
    "NET_CASH_FLOW": 0,
    "TOP_SPEND_CATEGORY": "",
    "DIGITAL_TXN_PCT": 0,
}

# SAS numeric missing (.) maps to NULL.
RISK_DEFAULTS = {
    "COMPOSITE_RISK_SCORE": None,
    "RISK_TIER": "UNKNOWN",
    "PROBABILITY_OF_DEFAULT": None,
    "WATCH_LIST_FLAG": "N",
}

# Column order and types of DATA_PRODUCTS.CUSTOMER_MASTER_PROFILE.
# Teradata -> Snowflake: DECIMAL(p,s) -> NUMBER(p,s), SMALLINT/INTEGER/BIGINT ->
# NUMBER, CHAR(n)/VARCHAR(n) -> VARCHAR(n), TIMESTAMP(6) -> TIMESTAMP_NTZ.
OUTPUT_COLUMNS = [
    ("CUSTOMER_ID", "bigint"),
    ("FULL_NAME", "string"),
    ("AGE", "smallint"),
    ("STATE_CODE", "string"),
    ("CUSTOMER_SINCE", "date"),
    ("TENURE_MONTHS", "int"),
    ("CUSTOMER_STATUS", "string"),
    ("SEGMENT_NAME", "string"),
    ("LIFETIME_VALUE_SCORE", "decimal(10,2)"),
    ("ENGAGEMENT_SCORE", "decimal(5,2)"),
    ("TOTAL_ACCOUNTS", "smallint"),
    ("ACTIVE_ACCOUNTS", "smallint"),
    ("TOTAL_BALANCE", "decimal(18,2)"),
    ("TOTAL_CREDIT_LIMIT", "decimal(18,2)"),
    ("CREDIT_UTILIZATION_PCT", "decimal(5,2)"),
    ("MONTHLY_TRANSACTIONS", "int"),
    ("MONTHLY_SPEND", "decimal(18,2)"),
    ("NET_CASH_FLOW", "decimal(18,2)"),
    ("TOP_SPEND_CATEGORY", "string"),
    ("DIGITAL_TXN_PCT", "decimal(5,2)"),
    ("COMPOSITE_RISK_SCORE", "decimal(6,2)"),
    ("RISK_TIER", "string"),
    ("PROBABILITY_OF_DEFAULT", "decimal(7,6)"),
    ("WATCH_LIST_FLAG", "string"),
    ("CROSS_SELL_FLAG", "string"),
    ("UPSELL_FLAG", "string"),
    ("RETENTION_RISK_FLAG", "string"),
    ("MODEL_VERSION", "string"),
    ("EFFECTIVE_DATE", "date"),
    ("LOAD_TS", "timestamp"),
]


def select_base(stg_customer_360: DataFrame) -> DataFrame:
    """SAS ``WORK.BASE``: active customers with their account summary."""
    return stg_customer_360.where(F.col("CUSTOMER_STATUS") == F.lit("A")).select(
        F.col("CUSTOMER_ID"),
        F.substring(
            F.concat_ws(" ", F.trim(F.col("FIRST_NAME")), F.trim(F.col("LAST_NAME"))),
            1,
            120,
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
    """SAS ``WORK.SEGMENTS``."""
    return customer_segments.select(
        "CUSTOMER_ID",
        "SEGMENT_NAME",
        "LIFETIME_VALUE_SCORE",
        "ENGAGEMENT_SCORE",
        "CROSS_SELL_FLAG",
        "UPSELL_FLAG",
        "RETENTION_RISK_FLAG",
    )


def select_txn(transaction_analytics: DataFrame, effective_date: date) -> DataFrame:
    """SAS ``WORK.TXN``: current period only (``where EFFECTIVE_DATE = today()``)."""
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
    """SAS ``WORK.RISK``."""
    return customer_risk_scores.select(
        "CUSTOMER_ID",
        "COMPOSITE_RISK_SCORE",
        "RISK_TIER",
        "PROBABILITY_OF_DEFAULT",
        "WATCH_LIST_FLAG",
    )


def _join_member(
    left: DataFrame,
    right: DataFrame,
    marker: str,
    defaults: Dict[str, object],
) -> DataFrame:
    """Left-join one merge member and apply its ``IN=``-driven defaults."""
    joined = left.join(
        right.withColumn(marker, F.lit(True)), on="CUSTOMER_ID", how="left"
    )
    for column, default in defaults.items():
        joined = joined.withColumn(
            column,
            F.when(F.col(marker).isNull(), F.lit(default)).otherwise(F.col(column)),
        )
    return joined.drop(marker)


def build_master_profile(
    base: DataFrame,
    segments: DataFrame,
    txn: DataFrame,
    risk: DataFrame,
    effective_date: date,
    load_ts: datetime,
    model_version: str = MODEL_VERSION,
) -> DataFrame:
    """Assemble the golden record (SAS ``data WORK.MASTER_PROFILE; merge ...``)."""
    profile = _join_member(base, segments, "_seg", SEGMENT_DEFAULTS)
    profile = _join_member(profile, txn, "_txn", TXN_DEFAULTS)
    profile = _join_member(profile, risk, "_risk", RISK_DEFAULTS)

    profile = (
        profile.withColumn("MODEL_VERSION", F.lit(model_version))
        .withColumn("EFFECTIVE_DATE", F.lit(effective_date).cast("date"))
        .withColumn("LOAD_TS", F.lit(load_ts).cast("timestamp"))
    )

    return profile.select(
        *[F.col(name).cast(dtype).alias(name) for name, dtype in OUTPUT_COLUMNS]
    )


def segment_distribution(master_profile: DataFrame) -> DataFrame:
    """SAS report: ``Master Profile - Segment Distribution``."""
    return (
        master_profile.groupBy("SEGMENT_NAME")
        .agg(
            F.count(F.lit(1)).alias("N"),
            F.round(F.avg("LIFETIME_VALUE_SCORE"), 2).alias("AVG_LTV"),
        )
        .orderBy(F.col("N").desc())
    )


def risk_tier_distribution(master_profile: DataFrame) -> DataFrame:
    """SAS report: ``Master Profile - Risk Tier Distribution``."""
    return (
        master_profile.groupBy("RISK_TIER")
        .agg(
            F.count(F.lit(1)).alias("N"),
            F.round(F.avg("COMPOSITE_RISK_SCORE"), 2).alias("AVG_SCORE"),
        )
        .orderBy(F.col("AVG_SCORE").desc())
    )


def completeness_report(master_profile: DataFrame) -> Dict[str, int]:
    """SAS report: ``Master Profile - Completeness Check``."""

    def text(column: str):
        # SAS character variables are blank-padded, never NULL, so a NULL read
        # from Snowflake compares as an empty string.
        return F.coalesce(F.col(column), F.lit(""))

    def flag_is(column: str, value: str):
        return F.sum((text(column) == F.lit(value)).cast("long"))

    row = master_profile.agg(
        F.count(F.lit(1)).alias("TOTAL"),
        F.sum((text("SEGMENT_NAME") != F.lit("UNCLASSIFIED")).cast("long")).alias(
            "HAS_SEGMENT"
        ),
        F.sum((F.col("MONTHLY_TRANSACTIONS") > F.lit(0)).cast("long")).alias("HAS_TXN"),
        F.sum((text("RISK_TIER") != F.lit("UNKNOWN")).cast("long")).alias(
            "HAS_RISK_SCORE"
        ),
        flag_is("CROSS_SELL_FLAG", "Y").alias("CROSS_SELL_ELIGIBLE"),
        flag_is("UPSELL_FLAG", "Y").alias("UPSELL_ELIGIBLE"),
        flag_is("RETENTION_RISK_FLAG", "Y").alias("RETENTION_AT_RISK"),
        flag_is("WATCH_LIST_FLAG", "Y").alias("ON_WATCH_LIST"),
    ).collect()[0]

    return {column: int(row[column] or 0) for column in row.asDict()}


def run(
    spark: SparkSession,
    config: Optional[PipelineConfig] = None,
    snowflake_io: Optional[SnowflakeIO] = None,
) -> int:
    """Execute the job end to end; returns the published row count."""
    config = config or PipelineConfig.from_env()
    snowflake_io = snowflake_io or SnowflakeIO(config.snowflake)

    logger = RunLogger(
        job_name=JOB_NAME,
        run_id=config.run_id,
        sink=RunLogSink(spark, snowflake_io, config.run_log_fqn),
    )
    logger.log_step(JOB_NAME, Status.START, "Building golden record")

    sf = config.snowflake
    try:
        logger.log_step(JOB_NAME, Status.START, "Extracting upstream data products")
        base = select_base(snowflake_io.read_table(spark, sf.staging_table("STG_CUSTOMER_360")))
        segments = select_segments(
            snowflake_io.read_table(spark, sf.data_product_table("CUSTOMER_SEGMENTS"))
        )
        txn = select_txn(
            snowflake_io.read_table(spark, sf.data_product_table("TRANSACTION_ANALYTICS")),
            config.run_date,
        )
        risk = select_risk(
            snowflake_io.read_table(spark, sf.data_product_table("CUSTOMER_RISK_SCORES"))
        )
        logger.log_step(JOB_NAME, Status.SUCCESS, "All upstream data extracted")

        logger.log_step(JOB_NAME, Status.START, "Merging data products")
        master_profile = build_master_profile(
            base,
            segments,
            txn,
            risk,
            effective_date=config.run_date,
            load_ts=datetime.now(timezone.utc),
        ).cache()
        row_count = master_profile.count()
        logger.log_step(
            JOB_NAME, Status.SUCCESS, "Master profile built", rowcount=row_count
        )

        for name, report in (
            ("segment distribution", segment_distribution(master_profile)),
            ("risk tier distribution", risk_tier_distribution(master_profile)),
        ):
            LOGGER.info("Master Profile - %s", name)
            report.show(truncate=False)

        completeness = completeness_report(master_profile)
        LOGGER.info("Master Profile - Completeness Check: %s", completeness)
        logger.log_step(
            JOB_NAME,
            Status.SUCCESS,
            "Completeness: "
            + ", ".join(f"{key}={value}" for key, value in completeness.items()),
            rowcount=row_count,
        )

        validation = validate_dataframe(
            master_profile,
            table="CUSTOMER_MASTER_PROFILE",
            key_cols=["CUSTOMER_ID"],
            not_null=["CUSTOMER_ID", "FULL_NAME", "CUSTOMER_STATUS"],
            min_rows=MIN_ROWS,
        )
        for warning in validation.warnings:
            logger.log_step(JOB_NAME, Status.WARNING, warning)
        validation.raise_for_status()

        logger.log_step(
            JOB_NAME,
            Status.START,
            f"Loading {sf.data_product_table('CUSTOMER_MASTER_PROFILE')}",
        )
        snowflake_io.write_table(
            master_profile,
            sf.data_product_table("CUSTOMER_MASTER_PROFILE"),
            mode="overwrite",
        )
        logger.log_step(
            JOB_NAME, Status.SUCCESS, "Golden record loaded", rowcount=row_count
        )
        master_profile.unpersist()
        return row_count
    except ValidationError as error:
        logger.log_step(JOB_NAME, Status.ERROR, f"Validation failed: {error}")
        raise
    except Exception as error:  # noqa: BLE001 - always record why the run aborted
        logger.log_step(JOB_NAME, Status.ERROR, f"Job failed: {error}")
        raise
    finally:
        logger.flush()


def main() -> int:
    config = PipelineConfig.from_env()
    logging.basicConfig(
        level=config.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    spark = SparkSession.builder.appName("customer_master_profile").getOrCreate()
    try:
        run(spark, config)
    except ValidationError:
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
