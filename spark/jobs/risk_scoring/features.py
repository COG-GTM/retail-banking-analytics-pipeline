"""Feature preparation - PySpark port of STEP 1 and STEP 2 of sas/03_sas_risk_scoring.sas."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import RiskScoringParams

CUSTOMER_360_COLUMNS = (
    "CUSTOMER_ID",
    "TENURE_MONTHS",
    "NUM_ACTIVE_ACCOUNTS",
    "TOTAL_BALANCE",
    "CUSTOMER_STATUS",
)

ACTIVE_CUSTOMER_STATUS = "A"


def extract_risk_raw(risk_factors: DataFrame, customer_360: DataFrame) -> DataFrame:
    """SAS STEP 1: inner join STG_RISK_FACTORS to STG_CUSTOMER_360 for active customers."""
    customer = customer_360.select(*CUSTOMER_360_COLUMNS).where(
        F.col("CUSTOMER_STATUS") == F.lit(ACTIVE_CUSTOMER_STATUS)
    )
    return risk_factors.join(customer, on="CUSTOMER_ID", how="inner")


def _safe_ratio(numerator: Column, denominator: Column, fallback: float = 1.0) -> Column:
    """SAS `if denom > 0 then ratio else fallback` semantics (missing denominators fall back)."""
    return F.when(
        denominator.isNotNull() & (denominator > F.lit(0.0)), numerator / denominator
    ).otherwise(F.lit(fallback))


def build_features(risk_raw: DataFrame, params: RiskScoringParams) -> DataFrame:
    """SAS STEP 2: impute the bureau score and derive the modelling features.

    SAS treats missing numerics as `.` which compares low, so `EXTERNAL_CREDIT_SCORE <= 0 or
    = .` is expressed here as "null or non-positive".
    """
    bureau_score = F.when(
        F.col("EXTERNAL_CREDIT_SCORE").isNull() | (F.col("EXTERNAL_CREDIT_SCORE") <= F.lit(0)),
        F.lit(params.bureau_score_imputed),
    ).otherwise(F.col("EXTERNAL_CREDIT_SCORE").cast("double"))

    bureau_span = params.bureau_score_ceiling - params.bureau_score_floor
    bureau_score_norm = (
        (bureau_score - F.lit(params.bureau_score_floor)) / F.lit(bureau_span) * F.lit(100.0)
    )

    velocity_ratio = _safe_ratio(
        F.col("DEBIT_VELOCITY_7D").cast("double") * F.lit(30.0 / 7.0),
        F.col("DEBIT_VELOCITY_30D").cast("double"),
    )

    return (
        risk_raw.withColumn("EXTERNAL_CREDIT_SCORE", bureau_score)
        .withColumn("BUREAU_SCORE_NORM", bureau_score_norm)
        .withColumn(
            "BALANCE_TREND_RATIO",
            _safe_ratio(
                F.col("AVG_DAILY_BALANCE_30D").cast("double"),
                F.col("AVG_DAILY_BALANCE_90D").cast("double"),
            ),
        )
        .withColumn("VELOCITY_RATIO", velocity_ratio)
        .withColumn(
            "DEFAULT_FLAG",
            F.when(
                F.coalesce(F.col("PAYMENT_LATE_CNT"), F.lit(0))
                > F.lit(params.default_flag_late_cnt),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
    )
