"""Synapse Spark entry point: PySpark port of sas/03_sas_risk_scoring.sas.

Pipeline steps mirror the SAS program one for one:
  1. read STG_RISK_FACTORS + STG_CUSTOMER_360 (active customers) from Snowflake;
  2. prepare features (bureau imputation, normalisation, ratios, default flag);
  3. fit the stepwise logistic regression and score the probability of default;
  4. build the composite score, tiers, drivers and publication flags;
  5. validate the data product;
  6. publish CUSTOMER_RISK_SCORES and the model audit record.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from .config import RiskScoringConfig, config_from_args
from .features import join_risk_inputs, prepare_features, read_staging_inputs
from .model import fit_stepwise, score_probability
from .publish import persist_model_audit, publish_risk_scores
from .scoring import classify_risk
from .validation import ValidationError, ValidationReport, validate_risk_scores

LOGGER = logging.getLogger("risk_scoring")


def build_risk_scores(
    risk_factors: DataFrame,
    customer_360: DataFrame,
    config: RiskScoringConfig,
) -> tuple[DataFrame, object]:
    """Feature prep -> stepwise model -> composite scoring. Returns (scores, model)."""

    joined = join_risk_inputs(risk_factors, customer_360)
    features = prepare_features(joined, default_bureau_score=config.default_bureau_score)

    model_result = fit_stepwise(
        features,
        candidate_features=config.candidate_features,
        slentry=config.slentry,
        slstay=config.slstay,
    )
    LOGGER.info(
        "Stepwise selection kept %s (threshold parameter RISK_SCORE_THRESHOLD=%s)",
        model_result.selected_features or "no variables",
        config.risk_score_threshold,
    )

    scored = score_probability(features, model_result)
    return classify_risk(scored, model_version=config.model_version), model_result


def run(spark: SparkSession, config: RiskScoringConfig) -> ValidationReport:
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    risk_factors, customer_360 = read_staging_inputs(
        spark, config.snowflake.reader_options(config.snowflake.staging_schema)
    )
    risk_scores, model_result = build_risk_scores(risk_factors, customer_360, config)
    risk_scores = risk_scores.cache()

    report = validate_risk_scores(risk_scores, min_rows=config.min_rows)
    LOGGER.info("Risk tier distribution: %s", report.tier_distribution)
    if not report.passed:
        raise ValidationError("; ".join(report.failures))

    persist_model_audit(spark, model_result, config, run_timestamp)
    if config.dry_run:
        LOGGER.info("Dry run - skipping publication of CUSTOMER_RISK_SCORES")
    else:
        publish_risk_scores(risk_scores, config)

    LOGGER.info("Risk scoring complete: %s rows", report.row_count)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    config = config_from_args(argv)
    spark = SparkSession.builder.appName("retail-banking-risk-scoring").getOrCreate()
    try:
        run(spark, config)
    except ValidationError as exc:
        LOGGER.error("Validation failed, CUSTOMER_RISK_SCORES not published: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
