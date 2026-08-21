"""Synapse Spark entrypoint for CUSTOMER_RISK_SCORES (migrated from sas/03_sas_risk_scoring.sas).

Pipeline: read STG_RISK_FACTORS + STG_CUSTOMER_360 from Snowflake, engineer features, fit the
stepwise logistic model, build the weighted composite score, tiers and risk drivers, validate,
then publish CUSTOMER_RISK_SCORES together with the model audit tables.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from dataclasses import replace

from pyspark.sql import SparkSession

from .config import RiskScoringParams, SnowflakeOptions
from .features import build_features, extract_risk_raw
from .io import (
    CUSTOMER_RISK_SCORES,
    STG_CUSTOMER_360,
    STG_RISK_FACTORS,
    read_table,
    write_model_audit,
    write_table,
)
from .model import fit_model
from .scoring import score_customers
from .validation import ValidationError, validate_risk_scores

LOGGER = logging.getLogger("risk_scoring")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Risk scoring job (TICKET-08 / MBA-2209)")
    parser.add_argument(
        "--source",
        choices=("snowflake", "local"),
        default="snowflake",
        help="Read from and write to Snowflake, or to local CSV fixtures for reconciliation runs",
    )
    parser.add_argument("--input-dir", default="data", help="Input directory in local mode")
    parser.add_argument(
        "--output-dir", default="build/risk_scoring", help="Output directory in local mode"
    )
    parser.add_argument(
        "--risk-score-threshold",
        type=float,
        default=None,
        help="Override RISK_SCORE_THRESHOLD from config/pipeline_config.cfg",
    )
    parser.add_argument("--model-version", default=None, help="Override RISK_MODEL_VERSION")
    parser.add_argument(
        "--effective-date", default=None, help="EFFECTIVE_DATE for the run (default: today)"
    )
    parser.add_argument(
        "--min-rows", type=int, default=None, help="Override the minimum published row count"
    )
    parser.add_argument(
        "--allow-intercept-only-model",
        action="store_true",
        help="Score with an intercept-only model when the cohort cannot support a fitted model",
    )
    parser.add_argument("--run-id", default=None, help="Run identifier used in the audit tables")
    return parser


def resolve_params(args: argparse.Namespace) -> RiskScoringParams:
    params = RiskScoringParams.from_env()
    if args.risk_score_threshold is not None:
        params = replace(params, risk_score_threshold=args.risk_score_threshold)
    if args.model_version is not None:
        params = replace(params, model_version=args.model_version)
    if args.min_rows is not None:
        params = replace(params, min_rows=args.min_rows)
    if args.allow_intercept_only_model:
        params = replace(params, allow_intercept_only_model=True)
    params.validate()
    return params


def run(spark: SparkSession, args: argparse.Namespace) -> int:
    params = resolve_params(args)
    run_id = args.run_id or uuid.uuid4().hex
    snowflake = SnowflakeOptions.from_env() if args.source == "snowflake" else None
    local_input = None if args.source == "snowflake" else args.input_dir
    local_output = None if args.source == "snowflake" else args.output_dir

    LOGGER.info(
        "03_RISK_SCORING START run_id=%s model_version=%s risk_score_threshold=%s",
        run_id,
        params.model_version,
        params.risk_score_threshold,
    )

    risk_factors = read_table(spark, STG_RISK_FACTORS, snowflake, local_input)
    customer_360 = read_table(spark, STG_CUSTOMER_360, snowflake, local_input)

    features = build_features(extract_risk_raw(risk_factors, customer_360), params).cache()
    LOGGER.info("Extracted risk factors rowcount=%s", features.count())

    model = fit_model(features, params)
    LOGGER.info(
        "Model fitted selected_features=%s converged=%s n=%s events=%s",
        list(model.selected_features),
        model.converged,
        model.n_observations,
        model.n_events,
    )
    for entry in model.selection_log:
        LOGGER.info("Stepwise: %s", entry)

    scores = score_customers(features, model, params, effective_date=args.effective_date)

    report = validate_risk_scores(scores, params, features=features)
    LOGGER.info("Validation report: %s", json.dumps(report.__dict__, default=str))
    if not report.passed:
        raise ValidationError(
            "CUSTOMER_RISK_SCORES failed validation: " + "; ".join(report.failures)
        )

    write_model_audit(spark, model, params.model_version, run_id, snowflake, local_output)
    write_table(scores, CUSTOMER_RISK_SCORES, snowflake, local_output)
    LOGGER.info("03_RISK_SCORING SUCCESS rowcount=%s run_id=%s", report.row_count, run_id)
    return report.row_count


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_arg_parser().parse_args(argv)
    spark = (
        SparkSession.builder.appName("retail-banking-risk-scoring")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        run(spark, args)
    except ValidationError as exc:
        LOGGER.error("03_RISK_SCORING ERROR %s", exc)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
