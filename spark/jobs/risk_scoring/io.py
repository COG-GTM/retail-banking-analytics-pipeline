"""Snowflake / local IO for the risk scoring job.

Snowflake is reached through the Spark connector (`net.snowflake.spark.snowflake`), which is
pre-installed on Synapse Spark pools. A `local` mode reads and writes the CSV fixtures in
`data/` so the job can be exercised and reconciled without a warehouse.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import SnowflakeOptions
from .model import FittedModel

SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"

COEFFICIENT_SCHEMA = (
    "RUN_ID string, MODEL_VERSION string, TERM string, COEFFICIENT double, "
    "STD_ERROR double, P_VALUE double, SELECTED boolean"
)
RUN_SCHEMA = (
    "RUN_ID string, MODEL_VERSION string, SELECTED_FEATURES string, SELECTION_LOG string, "
    "N_OBSERVATIONS bigint, N_EVENTS bigint, LOG_LIKELIHOOD double, CONVERGED boolean"
)

STG_RISK_FACTORS = "STG_RISK_FACTORS"
STG_CUSTOMER_360 = "STG_CUSTOMER_360"
CUSTOMER_RISK_SCORES = "CUSTOMER_RISK_SCORES"
RISK_MODEL_RUNS = "RISK_MODEL_RUNS"
RISK_MODEL_COEFFICIENTS = "RISK_MODEL_COEFFICIENTS"

LOCAL_TABLE_FILES = {
    STG_RISK_FACTORS: "02_bteq_staging/stg_risk_factors.csv",
    STG_CUSTOMER_360: "02_bteq_staging/stg_customer_360.csv",
}


def read_table(
    spark: SparkSession,
    table: str,
    snowflake: SnowflakeOptions | None,
    local_dir: str | None,
) -> DataFrame:
    if snowflake is not None:
        frame = (
            spark.read.format(SNOWFLAKE_FORMAT)
            .options(**snowflake.connector_options(snowflake.staging_schema))
            .option("dbtable", table)
            .load()
        )
    else:
        if local_dir is None:
            raise ValueError("local_dir is required when no Snowflake options are provided")
        path = Path(local_dir) / LOCAL_TABLE_FILES[table]
        frame = spark.read.csv(str(path), header=True, inferSchema=True)
    return frame.select([F.col(c).alias(c.upper()) for c in frame.columns])


def write_table(
    frame: DataFrame,
    table: str,
    snowflake: SnowflakeOptions | None,
    local_dir: str | None,
    mode: str = "overwrite",
) -> None:
    if snowflake is not None:
        (
            frame.write.format(SNOWFLAKE_FORMAT)
            .options(**snowflake.connector_options(snowflake.data_product_schema))
            .option("dbtable", table)
            .mode(mode)
            .save()
        )
        return
    if local_dir is None:
        raise ValueError("local_dir is required when no Snowflake options are provided")
    target = Path(local_dir) / table.lower()
    frame.coalesce(1).write.mode(mode).option("header", True).csv(str(target))


def write_model_audit(
    spark: SparkSession,
    model: FittedModel,
    model_version: str,
    run_id: str,
    snowflake: SnowflakeOptions | None,
    local_dir: str | None,
) -> None:
    """Persist the fitted coefficients and the selected variable set for every scoring run."""
    coefficient_rows = [{"RUN_ID": run_id, **row} for row in model.as_rows(model_version)]
    coefficients = spark.createDataFrame(coefficient_rows, COEFFICIENT_SCHEMA).withColumn(
        "LOAD_TS", F.current_timestamp()
    )

    run_row = [
        {
            "RUN_ID": run_id,
            "MODEL_VERSION": model_version,
            "SELECTED_FEATURES": ",".join(model.selected_features),
            "SELECTION_LOG": " | ".join(model.selection_log),
            "N_OBSERVATIONS": model.n_observations,
            "N_EVENTS": model.n_events,
            "LOG_LIKELIHOOD": model.log_likelihood,
            "CONVERGED": model.converged,
        }
    ]
    runs = spark.createDataFrame(run_row, RUN_SCHEMA).withColumn("LOAD_TS", F.current_timestamp())

    write_table(runs, RISK_MODEL_RUNS, snowflake, local_dir, mode="append")
    write_table(coefficients, RISK_MODEL_COEFFICIENTS, snowflake, local_dir, mode="append")

    if snowflake is None and local_dir is not None:
        summary = Path(local_dir) / f"{RISK_MODEL_RUNS.lower()}_{run_id}.json"
        summary.parent.mkdir(parents=True, exist_ok=True)
        summary.write_text(json.dumps(asdict(model), indent=2, default=str))
