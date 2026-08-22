"""Feature preparation - port of STEP 1 and STEP 2 of 03_sas_risk_scoring.sas."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

RISK_FACTORS_TABLE = "STG_RISK_FACTORS"
CUSTOMER_360_TABLE = "STG_CUSTOMER_360"

CUSTOMER_360_COLUMNS = (
    "TENURE_MONTHS",
    "NUM_ACTIVE_ACCOUNTS",
    "TOTAL_BALANCE",
    "CUSTOMER_STATUS",
)


def join_risk_inputs(risk_factors: DataFrame, customer_360: DataFrame) -> DataFrame:
    """Inner join STG_RISK_FACTORS with active customers from STG_CUSTOMER_360."""

    customers = customer_360.select("CUSTOMER_ID", *CUSTOMER_360_COLUMNS).where(
        F.col("CUSTOMER_STATUS") == F.lit("A")
    )
    return risk_factors.join(customers, on="CUSTOMER_ID", how="inner")


def _ratio(numerator: Column, denominator: Column, fallback: float = 1.0) -> Column:
    return F.when(denominator > 0, numerator / denominator).otherwise(F.lit(fallback))


def prepare_features(df: DataFrame, default_bureau_score: int = 680) -> DataFrame:
    """Impute the bureau score and derive the model input features.

    Mirrors the SAS DATA step: missing or non-positive bureau scores fall back to
    the population median placeholder, the bureau score is rescaled to 0-100, and
    the balance/velocity ratios default to 1 when their denominator is not
    positive.
    """

    bureau_score = F.when(
        F.col("EXTERNAL_CREDIT_SCORE").isNull() | (F.col("EXTERNAL_CREDIT_SCORE") <= 0),
        F.lit(default_bureau_score),
    ).otherwise(F.col("EXTERNAL_CREDIT_SCORE"))

    return (
        df.withColumn("EXTERNAL_CREDIT_SCORE", bureau_score.cast("double"))
        .withColumn(
            "BUREAU_SCORE_NORM",
            (F.col("EXTERNAL_CREDIT_SCORE") - F.lit(300.0)) / F.lit(550.0) * F.lit(100.0),
        )
        .withColumn(
            "BALANCE_TREND_RATIO",
            _ratio(F.col("AVG_DAILY_BALANCE_30D"), F.col("AVG_DAILY_BALANCE_90D")),
        )
        .withColumn(
            "VELOCITY_RATIO",
            _ratio(
                F.col("DEBIT_VELOCITY_7D") * F.lit(30.0 / 7.0),
                F.col("DEBIT_VELOCITY_30D"),
            ),
        )
        .withColumn(
            "DEFAULT_FLAG",
            F.when(F.col("PAYMENT_LATE_CNT") > F.lit(2), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
    )


def read_staging_inputs(
    spark: SparkSession,
    reader_options: dict[str, str],
) -> tuple[DataFrame, DataFrame]:
    """Read the two Snowflake staging tables produced by TICKET-03 and TICKET-05."""

    def _read(table: str) -> DataFrame:
        return (
            spark.read.format("snowflake").options(**reader_options).option("dbtable", table).load()
        )

    return _read(RISK_FACTORS_TABLE), _read(CUSTOMER_360_TABLE)
