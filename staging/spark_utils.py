"""Shared PySpark helpers: SparkSession, structured logging, schemas and IO.

These utilities replace the cross-cutting concerns that BTEQ handled with
dot-commands: ``.LOGON`` (session bootstrap), ``.SET`` options, run-scoped
audit logging, and the ``CREATE TABLE ... WITH DATA`` write step.
"""
from __future__ import annotations

import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from staging.config import StagingConfig

# Monetary and rate types mirror ddl/00_source_tables.sql. Using DECIMAL (not
# DOUBLE) keeps SUM/MAX arithmetic exact, matching Teradata and avoiding
# floating-point noise (e.g. 17603.620000000003) in aggregated dollar amounts.
_MONEY = DecimalType(15, 2)
_RATE = DecimalType(5, 4)

# --------------------------------------------------------------------------- #
# Structured, run-scoped logging (replaces BTEQ ETL_RUN_LOG audit inserts and
# bare echo/print). Emits one JSON object per line with a correlation run_id.
# --------------------------------------------------------------------------- #


class _JsonFormatter(logging.Formatter):
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "run_id": self.run_id,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Attach structured extras (e.g. step, table, row_count, status).
        for key, value in getattr(record, "extra_fields", {}).items():
            payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


class _StepLoggerAdapter(logging.LoggerAdapter):
    """Lets callers pass structured fields via ``logger.info(msg, step=...)``."""

    def process(self, msg, kwargs):
        extra_fields = {
            k: kwargs.pop(k)
            for k in list(kwargs)
            if k not in ("exc_info", "stack_info", "stacklevel", "extra")
        }
        kwargs["extra"] = {"extra_fields": extra_fields}
        return msg, kwargs


def get_logger(cfg: StagingConfig, name: str = "staging") -> _StepLoggerAdapter:
    logger = logging.getLogger(name)
    logger.setLevel(cfg.log_level)
    logger.handlers.clear()
    logger.propagate = False
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter(cfg.run_id))
    logger.addHandler(handler)
    return _StepLoggerAdapter(logger, {})


# --------------------------------------------------------------------------- #
# SparkSession (replaces BTEQ .LOGON / .SET session directives)
# --------------------------------------------------------------------------- #


def build_spark_session(cfg: StagingConfig) -> SparkSession:
    return (
        SparkSession.builder.appName(cfg.app_name)
        .master(cfg.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# Source table schemas (from ddl/00_source_tables.sql). Explicit schemas make
# date/decimal arithmetic correct and reads deterministic (no inferSchema).
# --------------------------------------------------------------------------- #

SOURCE_SCHEMAS: dict[str, StructType] = {
    "customers": StructType([
        StructField("customer_id", LongType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("date_of_birth", DateType()),
        StructField("ssn_hash", StringType()),
        StructField("email", StringType()),
        StructField("phone_primary", StringType()),
        StructField("customer_since", DateType()),
        StructField("customer_status", StringType()),
        StructField("segment_code", StringType()),
        StructField("branch_id", IntegerType()),
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
    ]),
    "accounts": StructType([
        StructField("account_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("account_type", StringType()),
        StructField("account_status", StringType()),
        StructField("open_date", DateType()),
        StructField("close_date", DateType()),
        StructField("current_balance", _MONEY),
        StructField("available_balance", _MONEY),
        StructField("credit_limit", _MONEY),
        StructField("interest_rate", _RATE),
        StructField("branch_id", IntegerType()),
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
    ]),
    "addresses": StructType([
        StructField("address_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("address_type", StringType()),
        StructField("address_line_1", StringType()),
        StructField("address_line_2", StringType()),
        StructField("city", StringType()),
        StructField("state_code", StringType()),
        StructField("zip_code", StringType()),
        StructField("country_code", StringType()),
        StructField("is_primary", StringType()),
        StructField("effective_date", DateType()),
        StructField("expiration_date", DateType()),
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
    ]),
    "transactions": StructType([
        StructField("transaction_id", LongType()),
        StructField("account_id", LongType()),
        StructField("transaction_type_cd", StringType()),
        StructField("transaction_date", DateType()),
        StructField("transaction_ts", TimestampType()),
        StructField("amount", _MONEY),
        StructField("running_balance", _MONEY),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("channel_code", StringType()),
        StructField("reference_num", StringType()),
        StructField("status_code", StringType()),
        StructField("created_ts", TimestampType()),
    ]),
    "transaction_types": StructType([
        StructField("transaction_type_cd", StringType()),
        StructField("description", StringType()),
        StructField("category", StringType()),
        StructField("is_revenue", StringType()),
        StructField("effective_date", DateType()),
        StructField("expiration_date", DateType()),
    ]),
    "customer_bureau_scores": StructType([
        StructField("customer_id", LongType()),
        StructField("external_credit_score", IntegerType()),
        StructField("report_date", DateType()),
    ]),
}


def read_source(spark: SparkSession, cfg: StagingConfig, table: str) -> DataFrame:
    """Read a source CSV using its explicit schema (empty string -> NULL)."""
    if table not in SOURCE_SCHEMAS:
        raise KeyError(f"Unknown source table: {table}")
    path = str(Path(cfg.source_dir) / f"{table}.csv")
    return (
        spark.read.option("header", True)
        .option("nullValue", "")
        .schema(SOURCE_SCHEMAS[table])
        .csv(path)
    )


def read_all_sources(spark: SparkSession, cfg: StagingConfig) -> dict[str, DataFrame]:
    return {t: read_source(spark, cfg, t) for t in SOURCE_SCHEMAS}


def write_staging(
    df: DataFrame,
    cfg: StagingConfig,
    table_name: str,
    logger: Optional[_StepLoggerAdapter] = None,
) -> str:
    """Persist a staging DataFrame (replaces ``CREATE TABLE ... WITH DATA``).

    ``csv`` (default) preserves the existing single-file data contract at
    ``data/02_bteq_staging/<table>.csv`` so the downstream SAS layer is
    unaffected; ``parquet`` writes a partitioned dataset directory instead.
    """
    fmt = cfg.output_format
    out_dir = Path(cfg.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        target = out_dir / f"{table_name}.csv"
        tmp = out_dir / f".{table_name}__tmp"
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(tmp))
        )
        part_files = list(tmp.glob("part-*.csv"))
        if not part_files:
            raise RuntimeError(f"No CSV part file produced for {table_name}")
        shutil.move(str(part_files[0]), str(target))
        shutil.rmtree(tmp, ignore_errors=True)
    else:
        target = out_dir / table_name
        df.write.mode("overwrite").format(fmt).save(str(target))

    if logger is not None:
        logger.info("staging output written", table=table_name, path=str(target),
                    format=fmt)
    return str(target)
