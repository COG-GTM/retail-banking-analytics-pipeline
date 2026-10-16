"""Migrate SAS 03 risk scoring to Synapse Spark; retain all nine features because Spark has no stepwise equivalent.
The SAS threshold parameter is unused, LR has no seed parameter, and driver ties use >= for fixture parity.
"""

import math
import sys
from datetime import date
from typing import Sequence

from pyspark.ml import feature
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MODEL_VERSION = "RISK_V4.0"
JOB_NAME = "03_sas_risk_scoring"
STEP_NAME = "03_RISK_SCORING"
TARGET_SCHEMA = "dp"
TARGET_TABLE = "CUSTOMER_RISK_SCORES"
FEATURE_COLS = [
    "BUREAU_SCORE_NORM",
    "CREDIT_UTIL_RATIO",
    "PAYMENT_ONTIME_PCT",
    "BALANCE_VOLATILITY",
    "VELOCITY_RATIO",
    "ACCOUNT_OVERDRAFT_CNT",
    "LARGE_WITHDRAWAL_CNT",
    "HIGH_RISK_MERCHANT_CNT",
    "TENURE_MONTHS",
]
OUTPUT_SCHEMA = StructType(
    [
        StructField("CUSTOMER_ID", LongType(), nullable=False),
        StructField("COMPOSITE_RISK_SCORE", DecimalType(6, 2)),
        StructField("RISK_TIER", StringType()),
        StructField("PROBABILITY_OF_DEFAULT", DecimalType(7, 6)),
        StructField("CREDIT_RISK_COMPONENT", DecimalType(5, 2)),
        StructField("BEHAVIOUR_RISK_COMPONENT", DecimalType(5, 2)),
        StructField("VELOCITY_RISK_COMPONENT", DecimalType(5, 2)),
        StructField("BUREAU_SCORE_COMPONENT", DecimalType(5, 2)),
        StructField("PAYMENT_HISTORY_COMPONENT", DecimalType(5, 2)),
        StructField("PRIMARY_RISK_DRIVER", StringType()),
        StructField("SECONDARY_RISK_DRIVER", StringType()),
        StructField("SCORE_DELTA_30D", DecimalType(6, 2)),
        StructField("WATCH_LIST_FLAG", StringType()),
        StructField("REVIEW_REQUIRED_FLAG", StringType()),
        StructField("MODEL_VERSION", StringType()),
        StructField("EFFECTIVE_DATE", DateType()),
        StructField("LOAD_TS", TimestampType()),
    ]
)
MIN_ROWS_BY_ENV = {"prod": 1000}
# Prod keeps the legacy %validate_table min_rows=1000 guard so truncated staging
# data cannot overwrite the target; dev/uat use 1 because the 407-row sample
# cannot meet 1000.
DEFAULT_MIN_ROWS = 1


def _min_rows(env: str) -> int:
    return MIN_ROWS_BY_ENV.get(env, DEFAULT_MIN_ROWS)


def _build_logistic_regression() -> LogisticRegression:
    # Spark's deterministic L-BFGS LR has no seed parameter; the seed=42 rule is vacuous.
    return LogisticRegression(
        featuresCol="features",
        labelCol="DEFAULT_FLAG",
        family="binomial",
        regParam=0.0,
        elasticNetParam=0.0,
        standardization=True,
        fitIntercept=True,
        maxIter=100,
        tol=1e-8,
    )


def _pd_column(
    model: LogisticRegressionModel, feature_cols: Sequence[str]
) -> F.Column:
    intercept = float(model.intercept)
    if math.isinf(intercept):
        # SAS rejects a single response level; missing predictions coalesce to 0.
        return F.lit(0.0)

    logit = F.lit(intercept)
    for index, name in enumerate(feature_cols):
        logit = logit + F.col(name).cast("double") * float(model.coefficients[index])
    return F.lit(1.0) / (F.lit(1.0) + F.exp(-logit))


def _clip_0_100(value: F.Column) -> F.Column:
    return F.greatest(
        F.lit(0.0),
        F.least(F.lit(100.0), F.coalesce(value, F.lit(100.0))),
    )


def transform(
    stg_risk_factors: DataFrame,
    stg_customer_360: DataFrame,
    as_of_date: date,
    risk_score_threshold: int = 700,
) -> DataFrame:
    """Join active customers, fit/score logistic risk, and produce the target DDL schema.

    risk_score_threshold is accepted for run-parameter compatibility but the SAS macro is
    never referenced. Spark has no LR seed parameter (L-BFGS is deterministic); all nine
    features are retained because Spark has no equivalent to SAS stepwise selection.
    """
    _ = risk_score_threshold
    customer_attrs = stg_customer_360.select(
        "CUSTOMER_ID",
        "TENURE_MONTHS",
        "NUM_ACTIVE_ACCOUNTS",
        "TOTAL_BALANCE",
        "CUSTOMER_STATUS",
    )
    joined = (
        stg_risk_factors.join(customer_attrs, on="CUSTOMER_ID", how="inner")
        .filter(F.col("CUSTOMER_STATUS") == F.lit("A"))
        .withColumn(
            "EXTERNAL_CREDIT_SCORE",
            F.when(
                F.col("EXTERNAL_CREDIT_SCORE").isNull()
                | (F.col("EXTERNAL_CREDIT_SCORE").cast("double") <= 0),
                F.lit(680.0),
            ).otherwise(F.col("EXTERNAL_CREDIT_SCORE").cast("double")),
        )
        .withColumn(
            "BUREAU_SCORE_NORM",
            (F.col("EXTERNAL_CREDIT_SCORE").cast("double") - F.lit(300.0))
            / F.lit(550.0)
            * F.lit(100.0),
        )
        .withColumn(
            "BALANCE_TREND_RATIO",
            F.when(
                F.col("AVG_DAILY_BALANCE_90D").cast("double") > 0,
                F.col("AVG_DAILY_BALANCE_30D").cast("double")
                / F.col("AVG_DAILY_BALANCE_90D").cast("double"),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn(
            "VELOCITY_RATIO",
            F.when(
                F.col("DEBIT_VELOCITY_30D").cast("double") > 0,
                (
                    F.col("DEBIT_VELOCITY_7D").cast("double")
                    * F.lit(30.0 / 7.0)
                )
                / F.col("DEBIT_VELOCITY_30D").cast("double"),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn(
            "DEFAULT_FLAG",
            F.when(
                F.coalesce(F.col("PAYMENT_LATE_CNT").cast("double") > 2, F.lit(False)),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
    )

    for name in FEATURE_COLS:
        joined = joined.withColumn(name, F.col(name).cast("double"))

    assembler = feature.VectorAssembler(
        inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip"
    )
    training = assembler.transform(joined).select("features", "DEFAULT_FLAG")
    if training.limit(1).count() == 0:
        probability = F.lit(0.0)
    else:
        model = _build_logistic_regression().fit(training)
        probability = _pd_column(model, FEATURE_COLS)

    scored = joined.withColumn("PROB_DEFAULT", probability)
    probability_of_default = F.round(
        F.coalesce(F.col("PROB_DEFAULT"), F.lit(0.0)), 6
    )

    credit_risk = _clip_0_100(F.lit(100.0) - F.col("BUREAU_SCORE_NORM"))
    behaviour_risk = _clip_0_100(
        F.lit(100.0) - F.col("PAYMENT_ONTIME_PCT").cast("double")
    )
    velocity_risk = _clip_0_100((F.col("VELOCITY_RATIO") - 1.0) * 50.0)
    bureau_component = _clip_0_100(F.col("BUREAU_SCORE_NORM"))
    payment_history = _clip_0_100(F.col("PAYMENT_ONTIME_PCT").cast("double"))
    composite = F.round(
        credit_risk * 0.30
        + behaviour_risk * 0.25
        + velocity_risk * 0.15
        + (F.lit(100.0) - bureau_component) * 0.20
        + (F.lit(100.0) - payment_history) * 0.10,
        2,
    )
    risk_tier = (
        F.when(composite < 20, F.lit("LOW"))
        .when(composite < 40, F.lit("MODERATE"))
        .when(composite < 60, F.lit("ELEVATED"))
        .when(composite < 80, F.lit("HIGH"))
        .otherwise(F.lit("CRITICAL"))
    )

    # >= reproduces the SAS data product's top-two driver ordering on tied bureau components.
    max1, max2 = F.lit(0.0), F.lit(0.0)
    primary, secondary = F.lit(None).cast("string"), F.lit(None).cast("string")
    for value, label in (
        (credit_risk, "CREDIT_UTILIZATION"),
        (behaviour_risk, "PAYMENT_BEHAVIOUR"),
        (velocity_risk, "TRANSACTION_VELOCITY"),
        (F.lit(100.0) - bureau_component, "BUREAU_SCORE"),
    ):
        primary_condition = value >= max1
        secondary_condition = value >= max2
        next_secondary = F.when(primary_condition, primary).otherwise(
            F.when(secondary_condition, F.lit(label)).otherwise(secondary)
        )
        next_primary = F.when(primary_condition, F.lit(label)).otherwise(primary)
        next_max2 = F.when(primary_condition, max1).otherwise(
            F.when(secondary_condition, value).otherwise(max2)
        )
        max1 = F.when(primary_condition, value).otherwise(max1)
        max2 = next_max2
        primary = next_primary
        secondary = next_secondary

    output = scored.select(
        F.col("CUSTOMER_ID").cast("long").alias("CUSTOMER_ID"),
        composite.cast(DecimalType(6, 2)).alias("COMPOSITE_RISK_SCORE"),
        risk_tier.alias("RISK_TIER"),
        probability_of_default.cast(DecimalType(7, 6)).alias(
            "PROBABILITY_OF_DEFAULT"
        ),
        credit_risk.cast(DecimalType(5, 2)).alias("CREDIT_RISK_COMPONENT"),
        behaviour_risk.cast(DecimalType(5, 2)).alias("BEHAVIOUR_RISK_COMPONENT"),
        velocity_risk.cast(DecimalType(5, 2)).alias("VELOCITY_RISK_COMPONENT"),
        bureau_component.cast(DecimalType(5, 2)).alias("BUREAU_SCORE_COMPONENT"),
        payment_history.cast(DecimalType(5, 2)).alias("PAYMENT_HISTORY_COMPONENT"),
        primary.alias("PRIMARY_RISK_DRIVER"),
        secondary.alias("SECONDARY_RISK_DRIVER"),
        F.lit(0.0).cast(DecimalType(6, 2)).alias("SCORE_DELTA_30D"),
        F.when(
            (risk_tier == "CRITICAL") & (probability_of_default > 0.5), "Y"
        )
        .otherwise("N")
        .alias("WATCH_LIST_FLAG"),
        F.when((composite >= 60) & (F.col("VELOCITY_RATIO") > 2.0), "Y")
        .otherwise("N")
        .alias("REVIEW_REQUIRED_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.lit(as_of_date).cast(DateType()).alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return output


def main(argv: list[str] | None = None) -> int:
    from synapse.spark.common import pipeline_utils as pu

    spark = SparkSession.builder.appName("sjd_risk_scoring").getOrCreate()
    try:
        params = pu.get_run_params(argv)
        pu.log_step(spark, JOB_NAME, STEP_NAME, "START", msg=f"Model version {MODEL_VERSION}")
        risk_factors = pu.read_table(spark, "stg", "STG_RISK_FACTORS")
        customer_360 = pu.read_table(spark, "stg", "STG_CUSTOMER_360")
        output = transform(
            risk_factors,
            customer_360,
            params.as_of_date,
            risk_score_threshold=params.risk_score_threshold,
        ).cache()
        pu.validate_table(
            output,
            key_cols=["CUSTOMER_ID"],
            not_null_cols=["CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER"],
            min_rows=_min_rows(params.env),
        )
        pu.write_table(output, TARGET_SCHEMA, TARGET_TABLE, mode="overwrite")
        row_count = output.count()
        pu.log_step(
            spark,
            JOB_NAME,
            STEP_NAME,
            "SUCCESS",
            row_count=row_count,
            msg=f"Model version {MODEL_VERSION}",
        )
        return 0
    except Exception as exc:
        pu.log_step(spark, JOB_NAME, STEP_NAME, "FAILED", msg=str(exc)[:4000])
        raise


if __name__ == "__main__":
    sys.exit(main())
