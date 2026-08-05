"""End-to-end driver — the PySpark equivalent of ``sas/03_sas_risk_scoring.sas``.

Runs STEP 1 -> STEP 6 in the original order:

1. Extract risk factor staging data          (``ingestion.read_risk_raw``)
2. Feature preparation                       (``ingestion.build_risk_features``)
3. Logistic regression probability of default(``model.train_and_score``)
4. Composite score and tier classification   (``scoring.classify_risk``)
5. Validate, abort on failure                (``validation.validate_table``)
6. Truncate-load DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES (``sink``)

Usage::

    python -m risk_scoring.driver                    # CSV backend, local Spark
    PIPELINE_IO_BACKEND=jdbc python -m risk_scoring.driver
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession

from .audit import AuditLog
from .config import PipelineConfig
from .connections import Connections, JdbcConfigurationError
from .ingestion import build_risk_features, read_risk_raw
from .model import train_and_score
from .schemas import CUSTOMER_RISK_SCORES
from .scoring import classify_risk
from .session import build_spark_session
from .sink import write_customer_risk_scores
from .validation import ValidationError, validate_table

logger = logging.getLogger(__name__)

STEP = "03_RISK_SCORING"


def log_tier_distribution(classified: DataFrame, audit: AuditLog) -> list[tuple[str, int]]:
    """Replacement for the ``PROC FREQ`` / ``PROC PRINT`` monitoring block."""
    rows = (
        classified.groupBy("RISK_TIER")
        .count()
        .orderBy("RISK_TIER")
        .collect()
    )
    distribution = [(r["RISK_TIER"], r["count"]) for r in rows]
    for tier, count in distribution:
        audit.log_step(
            step=STEP, status="SUCCESS", msg=f"Risk tier distribution {tier}", rowcount=count
        )
    return distribution


def run(config: PipelineConfig | None = None, spark: SparkSession | None = None) -> int:
    """Execute the pipeline. Returns the number of rows loaded to the target."""
    config = config or PipelineConfig.load()
    spark = spark or build_spark_session()
    connections = Connections(spark, config)

    audit = AuditLog(config.job_name)
    audit.log_step(
        step=STEP, status="START", msg=f"Model version {config.model_version}"
    )

    # ---- STEP 1: extract ------------------------------------------------- #
    risk_raw = read_risk_raw(connections).cache()
    audit.log_step(
        step=STEP,
        status="SUCCESS",
        msg="Extracted risk factors",
        rowcount=risk_raw.count(),
    )

    # ---- STEP 2: feature preparation ------------------------------------- #
    risk_features = build_risk_features(risk_raw)

    # ---- STEP 3: logistic regression ------------------------------------- #
    audit.log_step(
        step=STEP, status="START", msg="Training logistic regression model"
    )
    model_result = train_and_score(risk_features, config)
    audit.log_step(
        step=STEP,
        status="SUCCESS",
        msg=f"Model fitted on predictors {','.join(model_result.selected_features)}",
    )

    # ---- STEP 4: composite score and tiers ------------------------------- #
    audit.log_step(step=STEP, status="START", msg="Computing composite scores")
    classified = classify_risk(model_result.scored, config).cache()
    row_count = classified.count()
    audit.log_step(
        step=STEP, status="SUCCESS", msg="Risk scores computed", rowcount=row_count
    )

    # ---- STEP 5: validate ------------------------------------------------ #
    result = validate_table(
        classified,
        table=CUSTOMER_RISK_SCORES,
        key_cols=("CUSTOMER_ID",),
        not_null=("CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER"),
        min_rows=config.min_rows,
        audit=audit,
    )
    if not result.passed:
        audit.log_step(step=STEP, status="ERROR", msg="Validation failed")
        raise ValidationError(
            f"Validation failed for {result.table}: {'; '.join(result.errors)}"
        )

    log_tier_distribution(classified, audit)

    # ---- STEP 6: load ---------------------------------------------------- #
    # write_customer_risk_scores emits its own START/SUCCESS audit rows.
    written = write_customer_risk_scores(classified, connections, audit=audit)

    risk_raw.unpersist()
    classified.unpersist()
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--io-backend",
        choices=("csv", "jdbc"),
        help="override PIPELINE_IO_BACKEND",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        help="override the validation row-count floor (SAS: min_rows=1000)",
    )
    parser.add_argument("--master", help="Spark master, e.g. local[2]")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    overrides = {}
    if args.io_backend:
        overrides["io_backend"] = args.io_backend
    if args.min_rows is not None:
        overrides["min_rows"] = args.min_rows
    config = PipelineConfig.load(**overrides)

    spark = build_spark_session(master=args.master)
    try:
        written = run(config, spark)
    except (ValidationError, JdbcConfigurationError) as exc:
        # Both are operator-actionable: a failed data gate or a misconfigured
        # connection. Report the message, not a traceback.
        logger.error("%s", exc)
        return 1
    finally:
        spark.stop()
    logger.info("Loaded %s rows to %s", written, CUSTOMER_RISK_SCORES)
    return 0


if __name__ == "__main__":
    sys.exit(main())
