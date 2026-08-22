"""Composite risk score, tiering and risk drivers - port of STEP 4 of the SAS job."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

COMPONENT_WEIGHTS: dict[str, float] = {
    "CREDIT_RISK_COMPONENT": 0.30,
    "BEHAVIOUR_RISK_COMPONENT": 0.25,
    "VELOCITY_RISK_COMPONENT": 0.15,
    "INVERSE_BUREAU_SCORE_COMPONENT": 0.20,
    "INVERSE_PAYMENT_HISTORY_COMPONENT": 0.10,
}

DRIVER_LABELS: tuple[str, ...] = (
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
)

RISK_TIER_BREAKPOINTS: tuple[tuple[float, str], ...] = (
    (20.0, "LOW"),
    (40.0, "MODERATE"),
    (60.0, "ELEVATED"),
    (80.0, "HIGH"),
)

OUTPUT_COLUMNS: tuple[str, ...] = (
    "CUSTOMER_ID",
    "COMPOSITE_RISK_SCORE",
    "RISK_TIER",
    "PROBABILITY_OF_DEFAULT",
    "CREDIT_RISK_COMPONENT",
    "BEHAVIOUR_RISK_COMPONENT",
    "VELOCITY_RISK_COMPONENT",
    "BUREAU_SCORE_COMPONENT",
    "PAYMENT_HISTORY_COMPONENT",
    "PRIMARY_RISK_DRIVER",
    "SECONDARY_RISK_DRIVER",
    "SCORE_DELTA_30D",
    "WATCH_LIST_FLAG",
    "REVIEW_REQUIRED_FLAG",
    "MODEL_VERSION",
    "EFFECTIVE_DATE",
    "LOAD_TS",
)

_DRIVER_SCHEMA = StructType(
    [
        StructField("primary", StringType(), True),
        StructField("secondary", StringType(), True),
    ]
)


def _clamp(expr: Column) -> Column:
    """SAS ``max(0, min(100, x))``."""
    return F.greatest(F.lit(0.0), F.least(F.lit(100.0), expr))


def _top_two_drivers(*components: float | None) -> tuple[str, str]:
    """Replicate the SAS DATA step loop that picks the top two risk drivers.

    The loop keeps two running maxima seeded at 0 and uses strict ``>``
    comparisons, so ties keep the earlier label and a component that never beats
    the running second maximum leaves SECONDARY_RISK_DRIVER empty - both quirks
    are preserved deliberately.
    """

    max1 = 0.0
    max2 = 0.0
    primary = ""
    secondary = ""
    for value, label in zip(components, DRIVER_LABELS):
        if value is None:
            continue
        value = float(value)
        if value > max1:
            max2 = max1
            secondary = primary
            max1 = value
            primary = label
        elif value > max2:
            max2 = value
            secondary = label
    return primary, secondary


_top_two_drivers_udf = F.udf(_top_two_drivers, _DRIVER_SCHEMA)


def risk_tier_expr(composite: Column) -> Column:
    tier = F.lit("CRITICAL")
    for breakpoint_, label in reversed(RISK_TIER_BREAKPOINTS):
        tier = F.when(composite < F.lit(breakpoint_), F.lit(label)).otherwise(tier)
    return tier


def classify_risk(
    df: DataFrame,
    model_version: str,
    probability_col: str = "PROB_DEFAULT",
) -> DataFrame:
    """Derive components, composite score, tier, drivers and publication flags."""

    credit = _clamp(F.lit(100.0) - F.col("BUREAU_SCORE_NORM"))
    behaviour = _clamp(F.lit(100.0) - F.col("PAYMENT_ONTIME_PCT"))
    velocity = _clamp((F.col("VELOCITY_RATIO") - F.lit(1.0)) * F.lit(50.0))
    bureau = _clamp(F.col("BUREAU_SCORE_NORM"))
    payment_history = _clamp(F.col("PAYMENT_ONTIME_PCT"))

    scored = (
        df.withColumn("CREDIT_RISK_COMPONENT", credit)
        .withColumn("BEHAVIOUR_RISK_COMPONENT", behaviour)
        .withColumn("VELOCITY_RISK_COMPONENT", velocity)
        .withColumn("BUREAU_SCORE_COMPONENT", bureau)
        .withColumn("PAYMENT_HISTORY_COMPONENT", payment_history)
    )

    composite = F.round(
        F.col("CREDIT_RISK_COMPONENT") * F.lit(COMPONENT_WEIGHTS["CREDIT_RISK_COMPONENT"])
        + F.col("BEHAVIOUR_RISK_COMPONENT") * F.lit(COMPONENT_WEIGHTS["BEHAVIOUR_RISK_COMPONENT"])
        + F.col("VELOCITY_RISK_COMPONENT") * F.lit(COMPONENT_WEIGHTS["VELOCITY_RISK_COMPONENT"])
        + (F.lit(100.0) - F.col("BUREAU_SCORE_COMPONENT"))
        * F.lit(COMPONENT_WEIGHTS["INVERSE_BUREAU_SCORE_COMPONENT"])
        + (F.lit(100.0) - F.col("PAYMENT_HISTORY_COMPONENT"))
        * F.lit(COMPONENT_WEIGHTS["INVERSE_PAYMENT_HISTORY_COMPONENT"]),
        2,
    )

    drivers = _top_two_drivers_udf(
        F.col("CREDIT_RISK_COMPONENT"),
        F.col("BEHAVIOUR_RISK_COMPONENT"),
        F.col("VELOCITY_RISK_COMPONENT"),
        F.lit(100.0) - F.col("BUREAU_SCORE_COMPONENT"),
    )

    return (
        scored.withColumn("COMPOSITE_RISK_SCORE", composite)
        .withColumn(
            "PROBABILITY_OF_DEFAULT",
            F.round(F.coalesce(F.col(probability_col), F.lit(0.0)), 6),
        )
        .withColumn("RISK_TIER", risk_tier_expr(F.col("COMPOSITE_RISK_SCORE")))
        .withColumn("_DRIVERS", drivers)
        .withColumn("PRIMARY_RISK_DRIVER", F.col("_DRIVERS.primary"))
        .withColumn("SECONDARY_RISK_DRIVER", F.col("_DRIVERS.secondary"))
        .withColumn("SCORE_DELTA_30D", F.lit(0.0))
        .withColumn(
            "WATCH_LIST_FLAG",
            F.when(
                (F.col("RISK_TIER") == F.lit("CRITICAL"))
                & (F.col("PROBABILITY_OF_DEFAULT") > F.lit(0.5)),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn(
            "REVIEW_REQUIRED_FLAG",
            F.when(
                (F.col("COMPOSITE_RISK_SCORE") >= F.lit(60.0))
                & (F.col("VELOCITY_RATIO") > F.lit(2.0)),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn("MODEL_VERSION", F.lit(model_version))
        .withColumn("EFFECTIVE_DATE", F.current_date())
        .withColumn("LOAD_TS", F.current_timestamp())
        .select(*OUTPUT_COLUMNS)
    )
