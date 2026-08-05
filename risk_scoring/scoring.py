"""STEP 4 of ``sas/03_sas_risk_scoring.sas`` — composite scoring and tiering.

Ports ``data WORK.RISK_CLASSIFIED`` (and the ``PAYMENT_HISTORY_COMP`` rename of
``WORK.CUSTOMER_RISK_FINAL``) to PySpark: the five clamped component scores, the
weighted composite, the risk tier, the top-two risk drivers, the watch-list and
review flags, and the run metadata.

Output columns are exactly :data:`risk_scoring.schemas.CUSTOMER_RISK_SCORES_COLUMNS`
in that order, still in analytic ``double`` types — the DECIMAL casts of the
target DDL are applied by the sink.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from risk_scoring.config import PipelineConfig
from risk_scoring.schemas import CUSTOMER_RISK_SCORES_COLUMNS, RISK_DRIVER_LABELS

#: Composite weights, in SAS expression order.
CREDIT_WEIGHT = 0.30
BEHAVIOUR_WEIGHT = 0.25
VELOCITY_WEIGHT = 0.15
BUREAU_WEIGHT = 0.20
PAYMENT_HISTORY_WEIGHT = 0.10

#: ``round(COMPOSITE_RISK_SCORE, 0.01)`` / ``round(PROBABILITY_OF_DEFAULT, 0.000001)``.
COMPOSITE_SCALE = 2
PROBABILITY_SCALE = 6

#: Lower bound of every tier above LOW, paired with the tier it opens.
RISK_TIER_BOUNDS = ((20.0, "LOW"), (40.0, "MODERATE"), (60.0, "ELEVATED"), (80.0, "HIGH"))
RISK_TIER_TOP = "CRITICAL"

WATCH_LIST_PROBABILITY = 0.5
REVIEW_COMPOSITE_MIN = 60.0
REVIEW_VELOCITY_MIN = 2.0


def _double(name: str) -> Column:
    """Read a numeric input as ``double``: SAS holds every numeric as a float."""
    return F.col(name).cast("double")


def _clamp(expr: Column) -> Column:
    """SAS ``max(0, min(100, expr))``.

    ``least``/``greatest`` skip nulls exactly like the SAS ``MIN``/``MAX``
    functions, so a missing input yields ``100`` under both engines.
    """
    return F.greatest(F.lit(0.0), F.least(F.lit(100.0), expr))


def _component_scores(df: DataFrame) -> DataFrame:
    """The five 0-100 component scores of the SAS DATA step."""
    return df.withColumns({
        "CREDIT_RISK_COMPONENT": _clamp(F.lit(100.0) - _double("BUREAU_SCORE_NORM")),
        "BEHAVIOUR_RISK_COMPONENT": _clamp(F.lit(100.0) - _double("PAYMENT_ONTIME_PCT")),
        "VELOCITY_RISK_COMPONENT": _clamp((_double("VELOCITY_RATIO") - F.lit(1.0)) * F.lit(50.0)),
        "BUREAU_SCORE_COMPONENT": _clamp(_double("BUREAU_SCORE_NORM")),
        "PAYMENT_HISTORY_COMPONENT": _clamp(_double("PAYMENT_ONTIME_PCT")),
    })


def _composite_score() -> Column:
    """The weighted composite, ported literally.

    ``(100 - BUREAU_SCORE_COMPONENT)`` and ``(100 - PAYMENT_HISTORY_COMPONENT)``
    repeat ``CREDIT_RISK_COMPONENT`` and ``BEHAVIOUR_RISK_COMPONENT`` whenever
    the clamps are inactive; they diverge at the clamp boundaries, so the
    duplication is kept rather than folded into the first two weights.
    """
    weighted = (
        F.col("CREDIT_RISK_COMPONENT") * F.lit(CREDIT_WEIGHT)
        + F.col("BEHAVIOUR_RISK_COMPONENT") * F.lit(BEHAVIOUR_WEIGHT)
        + F.col("VELOCITY_RISK_COMPONENT") * F.lit(VELOCITY_WEIGHT)
        + (F.lit(100.0) - F.col("BUREAU_SCORE_COMPONENT")) * F.lit(BUREAU_WEIGHT)
        + (F.lit(100.0) - F.col("PAYMENT_HISTORY_COMPONENT")) * F.lit(PAYMENT_HISTORY_WEIGHT)
    )
    return F.round(weighted, COMPOSITE_SCALE)


def _risk_tier() -> Column:
    tier = F.when(F.col("COMPOSITE_RISK_SCORE") < F.lit(RISK_TIER_BOUNDS[0][0]), RISK_TIER_BOUNDS[0][1])
    for bound, label in RISK_TIER_BOUNDS[1:]:
        tier = tier.when(F.col("COMPOSITE_RISK_SCORE") < F.lit(bound), label)
    return tier.otherwise(RISK_TIER_TOP)


def _ranked_drivers() -> Column:
    """Top-two driver labels, ordered as the SAS ``do i = 1 to 4`` loop leaves them.

    The SAS loop compares with strict ``>`` against ``_max1``/``_max2``, both
    seeded at ``0``. That makes it a plain top-two selection over the components
    that are strictly positive, with the earlier array index winning a tie. The
    negated index in the sort key reproduces that: sorting descending on
    ``(score, -index)`` cannot reorder tied scores away from array order.
    """
    components = (
        F.col("CREDIT_RISK_COMPONENT"),
        F.col("BEHAVIOUR_RISK_COMPONENT"),
        F.col("VELOCITY_RISK_COMPONENT"),
        F.lit(100.0) - F.col("BUREAU_SCORE_COMPONENT"),
    )
    candidates = F.array(*[
        F.struct(
            component.alias("SCORE"),
            F.lit(-index).alias("ORDER"),
            F.lit(label).alias("LABEL"),
        )
        for index, (component, label) in enumerate(zip(components, RISK_DRIVER_LABELS))
    ])
    positive = F.filter(candidates, lambda candidate: candidate["SCORE"] > F.lit(0.0))
    return F.transform(F.sort_array(positive, asc=False), lambda candidate: candidate["LABEL"])


def classify_risk(risk_scored: DataFrame, config: PipelineConfig) -> DataFrame:
    """Classify scored customers into the CUSTOMER_RISK_SCORES data product.

    :param risk_scored: ``ModelResult.scored`` — the STEP 2 feature columns plus
        ``PROB_DEFAULT``.
    :param config: supplies ``model_version`` (SAS ``&MODEL_VERSION.``).
    :returns: exactly ``schemas.CUSTOMER_RISK_SCORES_COLUMNS``, in order, in
        analytic ``double`` types.
    """
    scored = _component_scores(risk_scored).withColumns({
        "COMPOSITE_RISK_SCORE": _composite_score(),
        "PROBABILITY_OF_DEFAULT": F.round(
            F.coalesce(_double("PROB_DEFAULT"), F.lit(0.0)), PROBABILITY_SCALE
        ),
    })

    drivers = _ranked_drivers()
    classified = scored.withColumns({
        "RISK_TIER": _risk_tier(),
        "PRIMARY_RISK_DRIVER": F.get(drivers, F.lit(0)),
        "SECONDARY_RISK_DRIVER": F.get(drivers, F.lit(1)),
        "SCORE_DELTA_30D": F.lit(0.0),
        "MODEL_VERSION": F.lit(config.model_version),
        "EFFECTIVE_DATE": F.current_date(),
        "LOAD_TS": F.current_timestamp(),
    })

    flagged = classified.withColumns({
        "WATCH_LIST_FLAG": F.when(
            (F.col("RISK_TIER") == F.lit(RISK_TIER_TOP))
            & (F.col("PROBABILITY_OF_DEFAULT") > F.lit(WATCH_LIST_PROBABILITY)),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
        "REVIEW_REQUIRED_FLAG": F.when(
            (F.col("COMPOSITE_RISK_SCORE") >= F.lit(REVIEW_COMPOSITE_MIN))
            & (_double("VELOCITY_RATIO") > F.lit(REVIEW_VELOCITY_MIN)),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    })

    return flagged.select(*CUSTOMER_RISK_SCORES_COLUMNS)
