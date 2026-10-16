"""Port of sas/04_sas_data_products.sas - CUSTOMER_MASTER_PROFILE golden record.

Joins STG_CUSTOMER_360 (base, CUSTOMER_STATUS='A') with CUSTOMER_SEGMENTS,
TRANSACTION_ANALYTICS (current period), and CUSTOMER_RISK_SCORES into
dp.CUSTOMER_MASTER_PROFILE, replicating the SAS DATA step MERGE (in= flags)
semantics: defaults apply only when the right-side row is absent.
"""

import logging
import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)

JOB_NAME = "04_sas_data_products"
STEP_NAME = "04_MASTER_PROFILE"
MODEL_VERSION = "MASTER_V1.5"
SAS_MIN_ROWS = 1000  # SAS validate_table ran with min_rows=1000
# The 407-row committed demo dataset only runs in dev, so dev keeps a minimal guard.
DEV_MIN_ROWS = 1


def _min_rows(env: str) -> int:
    return DEV_MIN_ROWS if env == "dev" else SAS_MIN_ROWS

OUTPUT_SCHEMA = T.StructType(
    [
        T.StructField("CUSTOMER_ID", T.LongType(), False),
        T.StructField("FULL_NAME", T.StringType(), True),
        T.StructField("AGE", T.ShortType(), True),
        T.StructField("STATE_CODE", T.StringType(), True),
        T.StructField("CUSTOMER_SINCE", T.DateType(), True),
        T.StructField("TENURE_MONTHS", T.IntegerType(), True),
        T.StructField("CUSTOMER_STATUS", T.StringType(), True),
        T.StructField("SEGMENT_NAME", T.StringType(), True),
        T.StructField("LIFETIME_VALUE_SCORE", T.DecimalType(10, 2), True),
        T.StructField("ENGAGEMENT_SCORE", T.DecimalType(5, 2), True),
        T.StructField("TOTAL_ACCOUNTS", T.ShortType(), True),
        T.StructField("ACTIVE_ACCOUNTS", T.ShortType(), True),
        T.StructField("TOTAL_BALANCE", T.DecimalType(18, 2), True),
        T.StructField("TOTAL_CREDIT_LIMIT", T.DecimalType(18, 2), True),
        T.StructField("CREDIT_UTILIZATION_PCT", T.DecimalType(5, 2), True),
        T.StructField("MONTHLY_TRANSACTIONS", T.IntegerType(), True),
        T.StructField("MONTHLY_SPEND", T.DecimalType(18, 2), True),
        T.StructField("NET_CASH_FLOW", T.DecimalType(18, 2), True),
        T.StructField("TOP_SPEND_CATEGORY", T.StringType(), True),
        T.StructField("DIGITAL_TXN_PCT", T.DecimalType(5, 2), True),
        T.StructField("COMPOSITE_RISK_SCORE", T.DecimalType(6, 2), True),
        T.StructField("RISK_TIER", T.StringType(), True),
        T.StructField("PROBABILITY_OF_DEFAULT", T.DecimalType(7, 6), True),
        T.StructField("WATCH_LIST_FLAG", T.StringType(), True),
        T.StructField("CROSS_SELL_FLAG", T.StringType(), True),
        T.StructField("UPSELL_FLAG", T.StringType(), True),
        T.StructField("RETENTION_RISK_FLAG", T.StringType(), True),
        T.StructField("MODEL_VERSION", T.StringType(), True),
        T.StructField("EFFECTIVE_DATE", T.DateType(), True),
        T.StructField("LOAD_TS", T.TimestampType(), True),
    ]
)


def _base(stg_customer_360: DataFrame) -> DataFrame:
    # SAS `length=120` truncates the concatenation at 120 chars.
    full_name = F.rtrim(
        F.substring(
            F.rtrim(
                F.concat(
                    F.rtrim(F.coalesce(F.col("FIRST_NAME"), F.lit(""))),
                    F.lit(" "),
                    F.rtrim(F.coalesce(F.col("LAST_NAME"), F.lit(""))),
                )
            ),
            1,
            120,
        )
    )
    return (
        stg_customer_360.where(F.col("CUSTOMER_STATUS") == "A")
        .select(
            "CUSTOMER_ID",
            F.when(full_name == "", F.lit(None)).otherwise(full_name).alias("FULL_NAME"),
            "AGE",
            "STATE_CODE",
            "CUSTOMER_SINCE",
            "TENURE_MONTHS",
            "CUSTOMER_STATUS",
            F.col("NUM_ACCOUNTS").alias("TOTAL_ACCOUNTS"),
            F.col("NUM_ACTIVE_ACCOUNTS").alias("ACTIVE_ACCOUNTS"),
            "TOTAL_BALANCE",
            "TOTAL_CREDIT_LIMIT",
            "CREDIT_UTILIZATION_PCT",
        )
    )


def transform(
    stg_customer_360: DataFrame,
    customer_segments: DataFrame,
    transaction_analytics: DataFrame,
    customer_risk_scores: DataFrame,
    as_of_date: date,
    **params,
) -> DataFrame:
    base = _base(stg_customer_360)

    seg = customer_segments.select(
        "CUSTOMER_ID",
        "SEGMENT_NAME",
        "LIFETIME_VALUE_SCORE",
        "ENGAGEMENT_SCORE",
        "CROSS_SELL_FLAG",
        "UPSELL_FLAG",
        "RETENTION_RISK_FLAG",
        F.lit(True).alias("_IN_SEG"),
    )

    txn = (
        # SAS filters on EFFECTIVE_DATE = today() only.
        transaction_analytics.where(F.col("EFFECTIVE_DATE") == F.lit(as_of_date))
        .select(
            "CUSTOMER_ID",
            F.col("TOTAL_TRANSACTIONS").alias("MONTHLY_TRANSACTIONS"),
            F.col("TOTAL_DEBIT_AMT").alias("MONTHLY_SPEND"),
            "NET_CASH_FLOW",
            "TOP_SPEND_CATEGORY",
            "DIGITAL_TXN_PCT",
            F.lit(True).alias("_IN_TXN"),
        )
    )

    risk = customer_risk_scores.select(
        "CUSTOMER_ID",
        "COMPOSITE_RISK_SCORE",
        "RISK_TIER",
        "PROBABILITY_OF_DEFAULT",
        "WATCH_LIST_FLAG",
        F.lit(True).alias("_IN_RISK"),
    )

    merged = (
        base.join(seg, "CUSTOMER_ID", "left")
        .join(txn, "CUSTOMER_ID", "left")
        .join(risk, "CUSTOMER_ID", "left")
    )

    out = merged.select(
        "CUSTOMER_ID",
        "FULL_NAME",
        "AGE",
        "STATE_CODE",
        "CUSTOMER_SINCE",
        "TENURE_MONTHS",
        "CUSTOMER_STATUS",
        F.when(F.col("_IN_SEG").isNull(), F.lit("UNCLASSIFIED"))
        .otherwise(F.col("SEGMENT_NAME"))
        .alias("SEGMENT_NAME"),
        F.when(F.col("_IN_SEG").isNull(), F.lit(0))
        .otherwise(F.col("LIFETIME_VALUE_SCORE"))
        .alias("LIFETIME_VALUE_SCORE"),
        F.when(F.col("_IN_SEG").isNull(), F.lit(0))
        .otherwise(F.col("ENGAGEMENT_SCORE"))
        .alias("ENGAGEMENT_SCORE"),
        "TOTAL_ACCOUNTS",
        "ACTIVE_ACCOUNTS",
        "TOTAL_BALANCE",
        "TOTAL_CREDIT_LIMIT",
        "CREDIT_UTILIZATION_PCT",
        F.when(F.col("_IN_TXN").isNull(), F.lit(0))
        .otherwise(F.col("MONTHLY_TRANSACTIONS"))
        .alias("MONTHLY_TRANSACTIONS"),
        F.when(F.col("_IN_TXN").isNull(), F.lit(0))
        .otherwise(F.col("MONTHLY_SPEND"))
        .alias("MONTHLY_SPEND"),
        F.when(F.col("_IN_TXN").isNull(), F.lit(0))
        .otherwise(F.col("NET_CASH_FLOW"))
        .alias("NET_CASH_FLOW"),
        F.when(F.col("_IN_TXN").isNull(), F.lit(None))
        .otherwise(F.col("TOP_SPEND_CATEGORY"))
        .alias("TOP_SPEND_CATEGORY"),
        F.when(F.col("_IN_TXN").isNull(), F.lit(0))
        .otherwise(F.col("DIGITAL_TXN_PCT"))
        .alias("DIGITAL_TXN_PCT"),
        F.when(F.col("_IN_RISK").isNull(), F.lit(None))
        .otherwise(F.col("COMPOSITE_RISK_SCORE"))
        .alias("COMPOSITE_RISK_SCORE"),
        F.when(F.col("_IN_RISK").isNull(), F.lit("UNKNOWN"))
        .otherwise(F.col("RISK_TIER"))
        .alias("RISK_TIER"),
        F.when(F.col("_IN_RISK").isNull(), F.lit(None))
        .otherwise(F.col("PROBABILITY_OF_DEFAULT"))
        .alias("PROBABILITY_OF_DEFAULT"),
        F.when(F.col("_IN_RISK").isNull(), F.lit("N"))
        .otherwise(F.col("WATCH_LIST_FLAG"))
        .alias("WATCH_LIST_FLAG"),
        F.when(F.col("_IN_SEG").isNull(), F.lit("N"))
        .otherwise(F.col("CROSS_SELL_FLAG"))
        .alias("CROSS_SELL_FLAG"),
        F.when(F.col("_IN_SEG").isNull(), F.lit("N"))
        .otherwise(F.col("UPSELL_FLAG"))
        .alias("UPSELL_FLAG"),
        F.when(F.col("_IN_SEG").isNull(), F.lit("N"))
        .otherwise(F.col("RETENTION_RISK_FLAG"))
        .alias("RETENTION_RISK_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.lit(as_of_date).cast(T.DateType()).alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )

    return out.select(
        [F.col(f.name).cast(f.dataType).alias(f.name) for f in OUTPUT_SCHEMA.fields]
    )


def quality_report(df: DataFrame) -> DataFrame:
    agg = df.agg(
        F.count(F.lit(1)).alias("TOTAL"),
        F.sum(
            F.when(F.coalesce(F.col("SEGMENT_NAME"), F.lit("")) != "UNCLASSIFIED", 1)
            .otherwise(0)
        ).alias("HAS_SEGMENT"),
        F.sum(
            F.when(F.col("MONTHLY_TRANSACTIONS") > 0, 1).otherwise(0)
        ).alias("HAS_TXN"),
        F.sum(
            F.when(F.coalesce(F.col("RISK_TIER"), F.lit("")) != "UNKNOWN", 1)
            .otherwise(0)
        ).alias("HAS_RISK_SCORE"),
        F.sum(
            F.when(F.coalesce(F.col("CROSS_SELL_FLAG"), F.lit("")) == "Y", 1).otherwise(0)
        ).alias("CROSS_SELL_ELIGIBLE"),
        F.sum(
            F.when(F.coalesce(F.col("UPSELL_FLAG"), F.lit("")) == "Y", 1).otherwise(0)
        ).alias("UPSELL_ELIGIBLE"),
        F.sum(
            F.when(F.coalesce(F.col("RETENTION_RISK_FLAG"), F.lit("")) == "Y", 1)
            .otherwise(0)
        ).alias("RETENTION_AT_RISK"),
        F.sum(
            F.when(F.coalesce(F.col("WATCH_LIST_FLAG"), F.lit("")) == "Y", 1)
            .otherwise(0)
        ).alias("ON_WATCH_LIST"),
    )
    # On empty input sum() returns NULL; force zeros, all LongType.
    return agg.select(
        [F.coalesce(F.col(c), F.lit(0)).cast(T.LongType()).alias(c) for c in agg.columns]
    )


def segment_distribution(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("SEGMENT_NAME")
        .agg(
            F.count(F.lit(1)).alias("N"),
            F.round(F.avg("LIFETIME_VALUE_SCORE"), 2).alias("AVG_LTV"),
        )
        .orderBy(F.col("N").desc())
    )


def risk_tier_distribution(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("RISK_TIER")
        .agg(
            F.count(F.lit(1)).alias("N"),
            F.round(F.avg("COMPOSITE_RISK_SCORE"), 2).alias("AVG_SCORE"),
        )
        .orderBy(F.col("AVG_SCORE").desc_nulls_last())
    )


def main(argv: "list[str] | None" = None) -> int:
    from synapse.spark.common import pipeline_utils as pu

    spark = SparkSession.builder.appName("sjd_master_profile").getOrCreate()
    params = pu.get_run_params(argv)

    try:
        pu.log_step(spark, JOB_NAME, STEP_NAME, "START", msg="Building golden record")

        stg_customer_360 = pu.read_table(spark, "stg", "STG_CUSTOMER_360")
        customer_segments = pu.read_table(spark, "dp", "CUSTOMER_SEGMENTS")
        transaction_analytics = pu.read_table(spark, "dp", "TRANSACTION_ANALYTICS")
        customer_risk_scores = pu.read_table(spark, "dp", "CUSTOMER_RISK_SCORES")
        pu.log_step(spark, JOB_NAME, STEP_NAME, "SUCCESS", msg="All upstream data extracted")

        df = transform(
            stg_customer_360,
            customer_segments,
            transaction_analytics,
            customer_risk_scores,
            params.as_of_date,
        ).cache()
        try:
            n = df.count()
            pu.log_step(
                spark, JOB_NAME, STEP_NAME, "SUCCESS",
                msg="Master profile built", row_count=n,
            )

            completeness = quality_report(df).collect()[0].asDict()
            segments = segment_distribution(df).collect()
            tiers = risk_tier_distribution(df).collect()
            logger.info("Completeness check: %s", completeness)
            logger.info("Segment distribution: %s", [r.asDict() for r in segments])
            logger.info("Risk tier distribution: %s", [r.asDict() for r in tiers])
            msg = ", ".join(f"{k}={v}" for k, v in completeness.items())[:4000]
            pu.log_step(spark, JOB_NAME, STEP_NAME, "SUCCESS", msg=msg)

            try:
                pu.validate_table(
                    df,
                    key_cols=["CUSTOMER_ID"],
                    not_null_cols=["CUSTOMER_ID", "FULL_NAME", "CUSTOMER_STATUS"],
                    min_rows=_min_rows(params.env),
                )
            except pu.ValidationError as e:
                pu.log_step(
                    spark, JOB_NAME, STEP_NAME, "FAILED",
                    msg=f"Validation failed: {e}"[:4000],
                )
                return 1

            pu.write_table(df, "dp", "CUSTOMER_MASTER_PROFILE", mode="overwrite")
            pu.log_step(
                spark, JOB_NAME, STEP_NAME, "SUCCESS",
                msg="Golden record loaded", row_count=n,
            )
            pu.log_step(spark, JOB_NAME, STEP_NAME, "SUCCESS", msg="Full pipeline complete")
            return 0
        finally:
            df.unpersist()
    except Exception as e:
        pu.log_step(spark, JOB_NAME, STEP_NAME, "FAILED", msg=str(e)[:4000])
        raise


if __name__ == "__main__":
    sys.exit(main())
