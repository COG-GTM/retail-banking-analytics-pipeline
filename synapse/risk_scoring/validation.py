"""Publish-gate validation - port of %validate_table and the PROC FREQ monitor step."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_TIERS = ("LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL")


class ValidationError(RuntimeError):
    """Raised when CUSTOMER_RISK_SCORES fails a publish-gate check."""


@dataclass
class ValidationReport:
    row_count: int
    tier_distribution: dict[str, int]
    failures: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures


def validate_risk_scores(df: DataFrame, min_rows: int = 1000) -> ValidationReport:
    """Check row count, key uniqueness, not-null columns, score ranges and tiers."""

    df = df.cache()
    stats = df.select(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("CUSTOMER_ID").alias("distinct_customers"),
        F.sum(F.col("CUSTOMER_ID").isNull().cast("int")).alias("null_customer_id"),
        F.sum(F.col("COMPOSITE_RISK_SCORE").isNull().cast("int")).alias("null_score"),
        F.sum(F.col("RISK_TIER").isNull().cast("int")).alias("null_tier"),
        F.min("COMPOSITE_RISK_SCORE").alias("min_score"),
        F.max("COMPOSITE_RISK_SCORE").alias("max_score"),
        F.min("PROBABILITY_OF_DEFAULT").alias("min_pd"),
        F.max("PROBABILITY_OF_DEFAULT").alias("max_pd"),
    ).first()

    tier_rows = df.groupBy("RISK_TIER").count().collect()
    tier_distribution = {row["RISK_TIER"]: row["count"] for row in tier_rows}

    failures: list[str] = []
    if stats["row_count"] < min_rows:
        failures.append(f"row count {stats['row_count']} is below the minimum {min_rows}")
    if stats["distinct_customers"] != stats["row_count"]:
        failures.append("CUSTOMER_ID is not unique")
    for column in ("customer_id", "score", "tier"):
        if stats[f"null_{column}"]:
            failures.append(f"{stats[f'null_{column}']} null values in {column}")
    if stats["row_count"] and not (0.0 <= stats["min_score"] and stats["max_score"] <= 100.0):
        failures.append(
            f"COMPOSITE_RISK_SCORE outside 0-100 ({stats['min_score']} .. {stats['max_score']})"
        )
    if stats["row_count"] and not (0.0 <= stats["min_pd"] and stats["max_pd"] <= 1.0):
        failures.append(
            f"PROBABILITY_OF_DEFAULT outside 0-1 ({stats['min_pd']} .. {stats['max_pd']})"
        )
    unexpected = sorted(set(tier_distribution) - set(VALID_TIERS))
    if unexpected:
        failures.append(f"unexpected risk tiers: {', '.join(unexpected)}")

    return ValidationReport(
        row_count=int(stats["row_count"]),
        tier_distribution=tier_distribution,
        failures=failures,
    )
