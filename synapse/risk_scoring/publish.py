"""Snowflake publication and model audit persistence.

Replaces the SAS ``PROC SQL ... execute (DELETE ...) by teradata`` plus
``PROC APPEND`` pattern with a single transactional overwrite of
``CUSTOMER_RISK_SCORES`` through the Snowflake Spark connector.
"""

from __future__ import annotations

import json
import logging

from pyspark.sql import DataFrame, SparkSession

from .config import RiskScoringConfig
from .model import StepwiseResult

LOGGER = logging.getLogger(__name__)

RISK_SCORES_TABLE = "CUSTOMER_RISK_SCORES"
MODEL_AUDIT_TABLE = "RISK_MODEL_RUNS"


def publish_risk_scores(df: DataFrame, config: RiskScoringConfig) -> None:
    """Overwrite CUSTOMER_RISK_SCORES, keeping the table definition in place."""

    options = config.snowflake.reader_options(config.snowflake.data_product_schema)
    (
        df.write.format("snowflake")
        .options(**options)
        .option("dbtable", RISK_SCORES_TABLE)
        .option("truncate_table", "on")
        .option("usestagingtable", "on")
        .mode("overwrite")
        .save()
    )
    LOGGER.info("Published %s.%s", config.snowflake.data_product_schema, RISK_SCORES_TABLE)


def persist_model_audit(
    spark: SparkSession,
    result: StepwiseResult,
    config: RiskScoringConfig,
    run_timestamp: str,
) -> None:
    """Persist coefficients and the selected variable set for every scoring run."""

    record = result.as_audit_record(config.model_version)
    record["run_timestamp"] = run_timestamp
    record["risk_score_threshold"] = config.risk_score_threshold
    record["slentry"] = config.slentry
    record["slstay"] = config.slstay
    payload = json.dumps(record, sort_keys=True)

    LOGGER.info("Model audit record: %s", payload)

    audit_df = spark.createDataFrame(
        [(run_timestamp, config.model_version, payload)],
        "RUN_TIMESTAMP string, MODEL_VERSION string, MODEL_AUDIT string",
    )

    if config.model_audit_path:
        (
            audit_df.coalesce(1)
            .write.mode("append")
            .json(f"{config.model_audit_path.rstrip('/')}/run_timestamp={run_timestamp}")
        )

    if not config.dry_run:
        options = config.snowflake.reader_options(config.snowflake.data_product_schema)
        (
            audit_df.write.format("snowflake")
            .options(**options)
            .option("dbtable", MODEL_AUDIT_TABLE)
            .mode("append")
            .save()
        )
