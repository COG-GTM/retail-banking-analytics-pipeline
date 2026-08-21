"""Pre-publish validation - port of %validate_table and the PROC FREQ tier monitoring."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import RiskScoringParams

KEY_COLUMNS = ("CUSTOMER_ID",)
NOT_NULL_COLUMNS = ("CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER")


class ValidationError(RuntimeError):
    """Raised when CUSTOMER_RISK_SCORES fails validation; the job must not publish."""


@dataclass
class ValidationReport:
    row_count: int
    distinct_keys: int
    tier_distribution: dict[str, int]
    min_score: float | None
    max_score: float | None
    min_probability: float | None
    max_probability: float | None
    subprime_bureau_pct: float | None = None
    failures: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures


def validate_risk_scores(
    scores: DataFrame, params: RiskScoringParams, features: DataFrame | None = None
) -> ValidationReport:
    """Row counts, key uniqueness, not-null keys, score ranges and tier distribution."""
    scores.cache()
    aggregates = scores.agg(
        F.count(F.lit(1)).alias("ROW_COUNT"),
        F.countDistinct(*KEY_COLUMNS).alias("DISTINCT_KEYS"),
        F.min("COMPOSITE_RISK_SCORE").alias("MIN_SCORE"),
        F.max("COMPOSITE_RISK_SCORE").alias("MAX_SCORE"),
        F.min("PROBABILITY_OF_DEFAULT").alias("MIN_POD"),
        F.max("PROBABILITY_OF_DEFAULT").alias("MAX_POD"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"NULLS_{c}") for c in NOT_NULL_COLUMNS],
    ).collect()[0]

    tier_distribution = {
        row["RISK_TIER"]: row["COUNT"]
        for row in scores.groupBy("RISK_TIER").agg(F.count(F.lit(1)).alias("COUNT")).collect()
    }

    report = ValidationReport(
        row_count=int(aggregates["ROW_COUNT"]),
        distinct_keys=int(aggregates["DISTINCT_KEYS"]),
        tier_distribution=tier_distribution,
        min_score=aggregates["MIN_SCORE"],
        max_score=aggregates["MAX_SCORE"],
        min_probability=aggregates["MIN_POD"],
        max_probability=aggregates["MAX_POD"],
    )

    if features is not None:
        # Monitoring metric driven by the injected RISK_SCORE_THRESHOLD pipeline parameter.
        subprime = features.agg(
            F.avg(
                (F.col("EXTERNAL_CREDIT_SCORE") < F.lit(params.risk_score_threshold)).cast("double")
            ).alias("PCT")
        ).collect()[0]["PCT"]
        report.subprime_bureau_pct = None if subprime is None else round(float(subprime) * 100.0, 4)

    if report.row_count < params.min_rows:
        report.failures.append(
            f"row count {report.row_count} is below the minimum of {params.min_rows}"
        )
    if report.distinct_keys != report.row_count:
        report.failures.append(
            f"CUSTOMER_ID is not unique: {report.distinct_keys} distinct of {report.row_count} rows"
        )
    for column in NOT_NULL_COLUMNS:
        nulls = int(aggregates[f"NULLS_{column}"])
        null_pct = 0.0 if report.row_count == 0 else nulls / report.row_count * 100.0
        if null_pct > params.max_null_pct:
            report.failures.append(f"{column} has {nulls} null values ({null_pct:.4f}%)")
    if report.min_score is not None and (report.min_score < 0 or report.max_score > 100):
        report.failures.append(
            f"COMPOSITE_RISK_SCORE out of range [0, 100]: [{report.min_score}, {report.max_score}]"
        )
    if report.min_probability is not None and (
        report.min_probability < 0 or report.max_probability > 1
    ):
        report.failures.append(
            "PROBABILITY_OF_DEFAULT out of range [0, 1]: "
            f"[{report.min_probability}, {report.max_probability}]"
        )
    expected_tiers = {label for _, label in params.tier_boundaries} | {params.top_tier}
    unexpected = set(tier_distribution) - expected_tiers
    if unexpected:
        report.failures.append(f"unexpected risk tiers produced: {sorted(unexpected)}")

    return report
