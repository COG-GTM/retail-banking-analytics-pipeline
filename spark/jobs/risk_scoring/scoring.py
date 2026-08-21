"""Composite score, risk tiers and risk drivers - port of STEP 4 of sas/03_sas_risk_scoring.sas."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import RiskScoringParams
from .model import FittedModel, probability_column

OUTPUT_COLUMNS = (
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

DRIVER_LABELS = (
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
)


def _sas_min(left: Column, right: Column) -> Column:
    """SAS MIN(): ignores missing arguments instead of propagating them."""
    return F.least(left, right)


def _sas_max(left: Column, right: Column) -> Column:
    """SAS MAX(): ignores missing arguments instead of propagating them."""
    return F.greatest(left, right)


def _clip_0_100(value: Column) -> Column:
    """SAS `max(0, min(100, x))`. A missing x yields 100, exactly as in SAS."""
    return _sas_max(F.lit(0.0), _sas_min(F.lit(100.0), value))


def _risk_tier(score: Column, params: RiskScoringParams) -> Column:
    tier = F.lit(params.top_tier)
    for bound, label in reversed(params.tier_boundaries):
        tier = F.when(score < F.lit(float(bound)), F.lit(label)).otherwise(tier)
    return tier


def _risk_drivers(components: list[Column]) -> tuple[Column, Column]:
    """Replicate the SAS DATA-step loop that picks the top two contributing components.

    Comparisons are `>=`, matching the driver assignments in the SAS data product: on a tie the
    later component in array order takes the slot (SAS numeric comparison of the equal
    CREDIT_RISK_COMPONENT and 100 - BUREAU_SCORE_COMPONENT values resolves in favour of
    BUREAU_SCORE). The secondary driver stays missing until a second component beats the running
    runner-up.
    """
    max1: Column = F.lit(0.0)
    max2: Column = F.lit(0.0)
    primary: Column = F.lit(None).cast("string")
    secondary: Column = F.lit(None).cast("string")

    for component, label in zip(components, DRIVER_LABELS):
        is_new_max = component >= max1
        is_new_runner_up = (~is_new_max) & (component >= max2)

        new_secondary = (
            F.when(is_new_max, primary).when(is_new_runner_up, F.lit(label)).otherwise(secondary)
        )
        new_max2 = F.when(is_new_max, max1).when(is_new_runner_up, component).otherwise(max2)
        new_primary = F.when(is_new_max, F.lit(label)).otherwise(primary)
        new_max1 = F.when(is_new_max, component).otherwise(max1)

        primary, secondary, max1, max2 = new_primary, new_secondary, new_max1, new_max2

    return primary, secondary


def score_customers(
    features: DataFrame,
    model: FittedModel,
    params: RiskScoringParams,
    effective_date: str | None = None,
) -> DataFrame:
    """Apply the fitted model and the weighted composite score, and classify each customer."""
    weights = params.composite_weights

    credit_risk = _clip_0_100(F.lit(100.0) - F.col("BUREAU_SCORE_NORM"))
    behaviour_risk = _clip_0_100(F.lit(100.0) - F.col("PAYMENT_ONTIME_PCT").cast("double"))
    velocity_risk = _clip_0_100((F.col("VELOCITY_RATIO") - F.lit(1.0)) * F.lit(50.0))
    bureau_component = _clip_0_100(F.col("BUREAU_SCORE_NORM"))
    payment_history = _clip_0_100(F.col("PAYMENT_ONTIME_PCT").cast("double"))

    scored = (
        features.withColumn("CREDIT_RISK_COMPONENT", credit_risk)
        .withColumn("BEHAVIOUR_RISK_COMPONENT", behaviour_risk)
        .withColumn("VELOCITY_RISK_COMPONENT", velocity_risk)
        .withColumn("BUREAU_SCORE_COMPONENT", bureau_component)
        .withColumn("PAYMENT_HISTORY_COMPONENT", payment_history)
        .withColumn("PROB_DEFAULT", probability_column(model))
    )

    composite = F.round(
        F.col("CREDIT_RISK_COMPONENT") * F.lit(float(weights["CREDIT_RISK_COMPONENT"]))
        + F.col("BEHAVIOUR_RISK_COMPONENT") * F.lit(float(weights["BEHAVIOUR_RISK_COMPONENT"]))
        + F.col("VELOCITY_RISK_COMPONENT") * F.lit(float(weights["VELOCITY_RISK_COMPONENT"]))
        + (F.lit(100.0) - F.col("BUREAU_SCORE_COMPONENT"))
        * F.lit(float(weights["INVERSE_BUREAU_SCORE_COMPONENT"]))
        + (F.lit(100.0) - F.col("PAYMENT_HISTORY_COMPONENT"))
        * F.lit(float(weights["INVERSE_PAYMENT_HISTORY_COMPONENT"])),
        2,
    )

    scored = scored.withColumn("COMPOSITE_RISK_SCORE", composite).withColumn(
        "PROBABILITY_OF_DEFAULT", F.round(F.coalesce(F.col("PROB_DEFAULT"), F.lit(0.0)), 6)
    )

    primary_driver, secondary_driver = _risk_drivers(
        [
            F.col("CREDIT_RISK_COMPONENT"),
            F.col("BEHAVIOUR_RISK_COMPONENT"),
            F.col("VELOCITY_RISK_COMPONENT"),
            F.lit(100.0) - F.col("BUREAU_SCORE_COMPONENT"),
        ]
    )

    effective = F.current_date() if effective_date is None else F.to_date(F.lit(effective_date))

    return (
        scored.withColumn("RISK_TIER", _risk_tier(F.col("COMPOSITE_RISK_SCORE"), params))
        .withColumn("PRIMARY_RISK_DRIVER", primary_driver)
        .withColumn("SECONDARY_RISK_DRIVER", secondary_driver)
        .withColumn("SCORE_DELTA_30D", F.lit(0.0))
        .withColumn(
            "WATCH_LIST_FLAG",
            F.when(
                (F.col("RISK_TIER") == F.lit(params.top_tier))
                & (F.col("PROBABILITY_OF_DEFAULT") > F.lit(params.watch_list_pod_threshold)),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn(
            "REVIEW_REQUIRED_FLAG",
            F.when(
                (F.col("COMPOSITE_RISK_SCORE") >= F.lit(params.review_required_min_score))
                & (F.col("VELOCITY_RATIO") > F.lit(params.review_required_velocity_ratio)),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn("MODEL_VERSION", F.lit(params.model_version))
        .withColumn("EFFECTIVE_DATE", effective)
        .withColumn("LOAD_TS", F.current_timestamp())
        .select(*OUTPUT_COLUMNS)
    )
