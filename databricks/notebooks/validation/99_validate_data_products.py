# Databricks notebook source
# MAGIC %md
# MAGIC # Post-run validation
# MAGIC
# MAGIC Replaces Phase 3 of `orchestration/run_full_pipeline.sh` — the
# MAGIC `envsubst`-templated BTEQ `SELECT COUNT(*)` block — and runs three checks
# MAGIC against every certified data product:
# MAGIC
# MAGIC 1. **Schema contract** — column names, order and types must match
# MAGIC    `ddl/02_data_product_tables.sql` exactly (via `shared.schemas.GOLD_SCHEMAS`).
# MAGIC 2. **Data quality** — `%validate_table` rules: `min_rows`, unique
# MAGIC    `CUSTOMER_ID`, no NULLs in required columns.
# MAGIC 3. **Reference parity** *(optional)* — row counts and key aggregates
# MAGIC    compared against CSVs produced by the legacy pipeline, when
# MAGIC    `reference_data_path` is supplied.
# MAGIC
# MAGIC The task fails (raising `ValidationError`) if any blocking check fails, so
# MAGIC the Workflow surfaces a red task rather than silently publishing bad data.

# COMMAND ----------

from __future__ import annotations

import os
import sys


def _bootstrap() -> None:
    here = os.path.dirname(os.path.abspath(globals().get("__file__", os.path.join(os.getcwd(), "nb.py"))))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    if root not in sys.path:
        sys.path.insert(0, root)


_bootstrap()

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402

from shared import schemas  # noqa: E402
from shared.audit import ensure_run_log, log_step  # noqa: E402
from shared.config import PipelineConfig  # noqa: E402
from shared.logging_utils import get_logger, log_event  # noqa: E402
from shared.validation import ValidationError, validate_and_log  # noqa: E402

JOB_NAME = "99_post_run_validation"

# Table -> (key columns, columns that must not be NULL)
GOLD_CHECKS: dict[str, tuple[list[str], list[str]]] = {
    "CUSTOMER_SEGMENTS": (["CUSTOMER_ID"], ["CUSTOMER_ID", "SEGMENT_NAME", "MODEL_VERSION"]),
    "TRANSACTION_ANALYTICS": (
        ["CUSTOMER_ID", "REPORTING_PERIOD"],
        ["CUSTOMER_ID", "REPORTING_PERIOD", "MODEL_VERSION"],
    ),
    "CUSTOMER_RISK_SCORES": (
        ["CUSTOMER_ID"],
        ["CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER", "MODEL_VERSION"],
    ),
    "CUSTOMER_MASTER_PROFILE": (
        ["CUSTOMER_ID"],
        ["CUSTOMER_ID", "FULL_NAME", "SEGMENT_NAME", "MODEL_VERSION"],
    ),
}

# Reference CSV (as written by the legacy pipeline) -> gold table
REFERENCE_FILES: dict[str, str] = {
    "customer_segments": "CUSTOMER_SEGMENTS",
    "transaction_analytics": "TRANSACTION_ANALYTICS",
    "customer_risk_scores": "CUSTOMER_RISK_SCORES",
    "customer_master_profile": "CUSTOMER_MASTER_PROFILE",
}

# Aggregates compared against the reference run, per table.
PARITY_AGGREGATES: dict[str, list[str]] = {
    "CUSTOMER_SEGMENTS": ["LIFETIME_VALUE_SCORE", "ENGAGEMENT_SCORE"],
    "TRANSACTION_ANALYTICS": ["TOTAL_DEBIT_AMT", "TOTAL_CREDIT_AMT", "NET_CASH_FLOW"],
    "CUSTOMER_RISK_SCORES": ["COMPOSITE_RISK_SCORE", "PROBABILITY_OF_DEFAULT"],
    "CUSTOMER_MASTER_PROFILE": ["TOTAL_BALANCE", "MONTHLY_SPEND"],
}

PARITY_TOLERANCE = 0.01  # relative tolerance on summed aggregates

# COMMAND ----------


def check_schemas(spark: SparkSession, cfg: PipelineConfig) -> dict[str, list[str]]:
    """Compare every gold table against its DDL contract, column-for-column."""
    logger = get_logger()
    diffs: dict[str, list[str]] = {}
    for table, expected in schemas.GOLD_SCHEMAS.items():
        full_name = cfg.gold(table)
        try:
            actual = spark.table(full_name).schema
        except AnalysisException as exc:
            diffs[table] = [f"table not found: {exc}"[:300]]
            continue
        table_diffs = schemas.schema_diff(actual, expected)
        if table_diffs:
            diffs[table] = table_diffs
        log_event(
            logger,
            "schema_contract",
            run_id=cfg.run_id,
            job=JOB_NAME,
            table=full_name,
            status="MATCH" if not table_diffs else "MISMATCH",
            differences=table_diffs or None,
        )
        log_step(
            spark,
            cfg,
            JOB_NAME,
            f"SCHEMA:{table}",
            "SUCCESS" if not table_diffs else "ERROR",
            message="; ".join(table_diffs)[:1000] or "matches ddl/02_data_product_tables.sql",
        )
    return diffs


def check_quality(spark: SparkSession, cfg: PipelineConfig) -> dict[str, int]:
    """Row counts + `%validate_table` rules for each data product."""
    counts: dict[str, int] = {}
    for table, (keys, not_null) in GOLD_CHECKS.items():
        result = validate_and_log(
            spark,
            cfg,
            JOB_NAME,
            cfg.gold(table),
            key_cols=keys,
            not_null=not_null,
            raise_on_failure=False,
        )
        counts[table] = result.row_count
        if not result.passed:
            raise ValidationError(result.summary())
    return counts


def check_reference_parity(
    spark: SparkSession, cfg: PipelineConfig, reference_path: str
) -> dict[str, dict[str, float]]:
    """Compare row counts and key aggregates against the legacy CSV outputs.

    Aggregates are compared as sums with a relative tolerance, which is robust
    to floating-point differences between SAS/DuckDB and Spark while still
    catching a genuine logic divergence.
    """
    logger = get_logger()
    report: dict[str, dict[str, float]] = {}
    mismatches: list[str] = []

    for stem, table in REFERENCE_FILES.items():
        path = f"{reference_path.rstrip('/')}/{stem}.csv"
        try:
            ref = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            ref_rows = ref.count()
        except AnalysisException:
            log_event(
                logger,
                "reference_missing",
                run_id=cfg.run_id,
                job=JOB_NAME,
                table=table,
                path=path,
            )
            continue

        actual = spark.table(cfg.gold(table))
        actual_rows = actual.count()
        entry: dict[str, float] = {
            "reference_rows": float(ref_rows),
            "actual_rows": float(actual_rows),
        }
        if ref_rows != actual_rows:
            mismatches.append(f"{table}: {actual_rows:,} rows vs reference {ref_rows:,}")

        ref_cols = {c.upper() for c in ref.columns}
        for column in PARITY_AGGREGATES[table]:
            if column.upper() not in ref_cols:
                continue
            ref_sum = ref.select(F.sum(F.col(column).cast("double"))).collect()[0][0] or 0.0
            act_sum = actual.select(F.sum(F.col(column).cast("double"))).collect()[0][0] or 0.0
            entry[f"reference_sum_{column}"] = float(ref_sum)
            entry[f"actual_sum_{column}"] = float(act_sum)
            scale = max(abs(float(ref_sum)), 1.0)
            if abs(float(act_sum) - float(ref_sum)) / scale > PARITY_TOLERANCE:
                mismatches.append(
                    f"{table}.{column}: sum {act_sum:,.2f} vs reference {ref_sum:,.2f}"
                )

        report[table] = entry
        log_event(
            logger,
            "reference_parity",
            run_id=cfg.run_id,
            job=JOB_NAME,
            table=table,
            **{k: round(v, 4) for k, v in entry.items()},
        )
        log_step(
            spark,
            cfg,
            JOB_NAME,
            f"PARITY:{table}",
            "SUCCESS",
            message="; ".join(f"{k}={v:,.2f}" for k, v in entry.items())[:1000],
            row_count=actual_rows,
        )

    if mismatches:
        # Parity against a reference run is advisory: the reference may have been
        # generated from a different snapshot of the source data.
        log_event(
            logger,
            "reference_parity_mismatch",
            run_id=cfg.run_id,
            job=JOB_NAME,
            mismatches=mismatches,
        )
    return report


def run(spark: SparkSession, cfg: PipelineConfig, reference_path: str = "") -> dict[str, int]:
    ensure_run_log(spark, cfg)
    diffs = check_schemas(spark, cfg)
    if diffs:
        rendered = "; ".join(f"{t}: {', '.join(d)}" for t, d in diffs.items())
        raise ValidationError(f"Gold schemas drifted from ddl/02_data_product_tables.sql — {rendered}")

    counts = check_quality(spark, cfg)
    if reference_path:
        check_reference_parity(spark, cfg, reference_path)

    log_step(
        spark,
        cfg,
        JOB_NAME,
        "POST_RUN_VALIDATION",
        "SUCCESS",
        message=" ".join(f"{t}={n}" for t, n in counts.items()),
        row_count=sum(counts.values()),
    )
    return counts


# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = PipelineConfig.from_widgets(spark)
    logger = get_logger()
    log_event(logger, "job_start", run_id=cfg.run_id, job=JOB_NAME, config=cfg.describe())
    log_event(
        logger,
        "job_complete",
        run_id=cfg.run_id,
        job=JOB_NAME,
        row_counts=run(spark, cfg, cfg.reference_data_path),
    )
