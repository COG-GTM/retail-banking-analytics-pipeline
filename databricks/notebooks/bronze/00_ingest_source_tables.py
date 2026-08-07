# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — source table ingestion
# MAGIC
# MAGIC Registers the six operational source tables as Delta tables in
# MAGIC `<catalog>.<bronze_schema>`, replacing the Teradata source databases
# MAGIC `CORE_BANKING_DB` and `TXN_PROCESSING_DB` referenced by the BTEQ scripts.
# MAGIC
# MAGIC | Legacy source                              | Bronze table |
# MAGIC |--------------------------------------------|--------------|
# MAGIC | `CORE_BANKING_DB.CUSTOMERS`                | `CUSTOMERS` |
# MAGIC | `CORE_BANKING_DB.ACCOUNTS`                 | `ACCOUNTS` |
# MAGIC | `CORE_BANKING_DB.ADDRESSES`                | `ADDRESSES` |
# MAGIC | `CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES`   | `CUSTOMER_BUREAU_SCORES` |
# MAGIC | `TXN_PROCESSING_DB.TRANSACTIONS`           | `TRANSACTIONS` |
# MAGIC | `TXN_PROCESSING_DB.TRANSACTION_TYPES`      | `TRANSACTION_TYPES` |
# MAGIC
# MAGIC Two ingestion modes, selected with the `source_format` job parameter:
# MAGIC
# MAGIC * `csv` (default) — reads the extracts produced by `export_data.py` from a
# MAGIC   Unity Catalog volume; no credentials involved.
# MAGIC * `jdbc` — reads directly from the legacy Teradata system, with the
# MAGIC   connection details taken from a Databricks secret scope (replacing the
# MAGIC   `{SAS004}` passwords in `sas/macros/connect_teradata.sas`).

# COMMAND ----------

from __future__ import annotations

import os
import sys


def _bootstrap() -> None:
    """Put the ``databricks/`` folder on ``sys.path`` for ``shared.*`` imports."""
    here = os.path.dirname(os.path.abspath(globals().get("__file__", os.path.join(os.getcwd(), "nb.py"))))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    if root not in sys.path:
        sys.path.insert(0, root)


_bootstrap()

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from shared import io, schemas  # noqa: E402
from shared.audit import ensure_run_log, step  # noqa: E402
from shared.config import PipelineConfig, exit_if_skipped  # noqa: E402
from shared.logging_utils import get_logger, log_event  # noqa: E402
from shared.validation import validate_and_log  # noqa: E402

JOB_NAME = "00_bronze_ingest"

# Key columns used for the %validate_table uniqueness check.
KEY_COLUMNS = {
    "CUSTOMERS": ["CUSTOMER_ID"],
    "ACCOUNTS": ["ACCOUNT_ID"],
    "ADDRESSES": ["ADDRESS_ID"],
    "TRANSACTIONS": ["TRANSACTION_ID"],
    "TRANSACTION_TYPES": ["TRANSACTION_TYPE_CD"],
    "CUSTOMER_BUREAU_SCORES": [],  # one row per bureau pull, not per customer
}

# COMMAND ----------


def read_source(spark: SparkSession, cfg: PipelineConfig, file_stem: str, table: str) -> DataFrame:
    """Read one source table as raw strings, then cast onto its DDL contract."""
    if cfg.source_format == "jdbc":
        from shared.secrets import jdbc_options

        database = "TXN_PROCESSING_DB" if table.startswith("TRANSACTION") else "CORE_BANKING_DB"
        raw = (
            spark.read.format("jdbc")
            .options(**jdbc_options(cfg, spark))
            .option("dbtable", f"{database}.{table}")
            .load()
        )
    else:
        raw = spark.read.csv(
            f"{cfg.landing_path}/{file_stem}.csv",
            header=True,
            inferSchema=False,
            multiLine=True,
            escape='"',
        )
    return schemas.conform(raw, schemas.SOURCE_SCHEMAS[table])


def ingest(spark: SparkSession, cfg: PipelineConfig) -> dict[str, int]:
    io.ensure_schemas(spark, cfg)
    ensure_run_log(spark, cfg)
    counts: dict[str, int] = {}

    for file_stem, table in schemas.SOURCE_FILES.items():
        with step(spark, cfg, JOB_NAME, f"LOAD:{table}") as ctx:
            df = read_source(spark, cfg, file_stem, table)
            rows = io.write_table(
                spark, cfg, df, cfg.bronze(table), merge_keys=KEY_COLUMNS[table] or None
            )
            ctx["row_count"] = rows
            ctx["message"] = f"source_format={cfg.source_format}"
            counts[table] = rows

        validate_and_log(
            spark,
            cfg,
            JOB_NAME,
            cfg.bronze(table),
            key_cols=KEY_COLUMNS[table],
            not_null=[schemas.SOURCE_SCHEMAS[table].fields[0].name],
        )
    return counts


# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = PipelineConfig.from_widgets(spark)
    logger = get_logger()
    log_event(logger, "job_start", run_id=cfg.run_id, job=JOB_NAME, config=cfg.describe())

    if not exit_if_skipped(cfg, "bronze", spark):
        log_event(
            logger,
            "job_complete",
            run_id=cfg.run_id,
            job=JOB_NAME,
            row_counts=ingest(spark, cfg),
        )
