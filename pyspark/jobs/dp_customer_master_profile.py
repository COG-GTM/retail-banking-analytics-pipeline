"""Data-product job 04 -- CUSTOMER_MASTER_PROFILE (the golden record).

Faithful PySpark port of ``sas/04_sas_data_products.sas``: assembles the
enterprise "golden record" by left-merging the active-customer BASE (from
``STG_CUSTOMER_360``) with the three upstream data products (CUSTOMER_SEGMENTS,
TRANSACTION_ANALYTICS, CUSTOMER_RISK_SCORES).

SAS -> PySpark mapping:
* ``proc sql`` extracts (``WORK.BASE``/``SEGMENTS``/``TXN``/``RISK``) -> the four
  ``_*_projection`` pure functions (rename + column selection).
* ``BASE.FULL_NAME = trim(FIRST_NAME) || ' ' || trim(LAST_NAME)`` -> ``concat``
  over trimmed, null-coalesced names (SAS treats a missing char value as blank,
  so a null name contributes an empty string rather than nulling the result).
* ``where CUSTOMER_STATUS = 'A'`` (BASE) and ``where EFFECTIVE_DATE = today()``
  (TXN) -> Spark filters, the latter driven by ``config.run_date`` (never the
  wall clock inside the transform).
* ``data ...; merge BASE(in=_base) SEGMENTS(in=_seg) TXN(in=_txn) RISK(in=_risk);
  by CUSTOMER_ID; if _base;`` -> a left join from BASE onto each product keyed on
  ``customer_id`` (``if _base`` keeps only rows present in BASE).
* ``if not _seg/_txn/_risk then do; ... end;`` -> per-source presence flags
  (``_seg``/``_txn``/``_risk``) plus ``when(flag.isNull(), default)``, so an
  absent product row gets the SAS defaults while a matched row keeps its values.
* ``MODEL_VERSION``/``EFFECTIVE_DATE = today()``/``LOAD_TS = datetime()`` ->
  literal metadata columns; the final ``enforce_schema`` pins the output to the
  ``CUSTOMER_MASTER_PROFILE`` DDL column order + types.

Mirrors the reference job ``staging_customer_360``: pure ``transform`` functions
+ a thin :func:`run` that wires I/O, validation, audit and the schema contract.
"""

from __future__ import annotations

import argparse
import datetime as _dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.dates import load_timestamp
from common.io import DataIO, LocalDataIO
from common.spark import build_spark
from common.validation import abort_on_failure, validate_table

JOB_NAME = "04_customer_master_profile"
TARGET = "CUSTOMER_MASTER_PROFILE"

# %let MODEL_VERSION = MASTER_V1.5; (also PipelineConfig.MODEL_VERSIONS["master_profile"]).
MODEL_VERSION = "MASTER_V1.5"

_ACTIVE_STATUS = "A"


def base_profile(stg_customer_360: DataFrame) -> DataFrame:
    """``WORK.BASE``: active customers with the renamed base attributes."""
    full_name = F.concat(
        F.coalesce(F.trim(F.col("first_name")), F.lit("")),
        F.lit(" "),
        F.coalesce(F.trim(F.col("last_name")), F.lit("")),
    )
    return (
        stg_customer_360
        .filter(F.col("customer_status") == _ACTIVE_STATUS)
        .select(
            F.col("customer_id"),
            full_name.alias("full_name"),
            F.col("age"),
            F.col("state_code"),
            F.col("customer_since"),
            F.col("tenure_months"),
            F.col("customer_status"),
            F.col("num_accounts").alias("total_accounts"),
            F.col("num_active_accounts").alias("active_accounts"),
            F.col("total_balance"),
            F.col("total_credit_limit"),
            F.col("credit_utilization_pct"),
        )
    )


def segment_features(customer_segments: DataFrame) -> DataFrame:
    """``WORK.SEGMENTS`` projection (``_seg`` marks a matched row)."""
    return customer_segments.select(
        F.col("customer_id"),
        F.col("segment_name"),
        F.col("lifetime_value_score"),
        F.col("engagement_score"),
        F.col("cross_sell_flag"),
        F.col("upsell_flag"),
        F.col("retention_risk_flag"),
        F.lit(True).alias("_seg"),
    )


def txn_features(transaction_analytics: DataFrame, run_date: _dt.date) -> DataFrame:
    """``WORK.TXN`` projection for the current period (``_txn`` marks a match)."""
    return (
        transaction_analytics
        .filter(F.col("effective_date") == F.lit(run_date))
        .select(
            F.col("customer_id"),
            F.col("total_transactions").alias("monthly_transactions"),
            F.col("total_debit_amt").alias("monthly_spend"),
            F.col("net_cash_flow"),
            F.col("top_spend_category"),
            F.col("digital_txn_pct"),
            F.lit(True).alias("_txn"),
        )
    )


def risk_features(customer_risk_scores: DataFrame) -> DataFrame:
    """``WORK.RISK`` projection (``_risk`` marks a matched row)."""
    return customer_risk_scores.select(
        F.col("customer_id"),
        F.col("composite_risk_score"),
        F.col("risk_tier"),
        F.col("probability_of_default"),
        F.col("watch_list_flag"),
        F.lit(True).alias("_risk"),
    )


# (source presence flag, column, SAS default) for the "if not _x then ..." blocks.
_SEG_DEFAULTS = (
    ("segment_name", "UNCLASSIFIED"),
    ("lifetime_value_score", 0),
    ("engagement_score", 0),
    ("cross_sell_flag", "N"),
    ("upsell_flag", "N"),
    ("retention_risk_flag", "N"),
)
_TXN_DEFAULTS = (
    ("monthly_transactions", 0),
    ("monthly_spend", 0),
    ("net_cash_flow", 0),
    ("top_spend_category", ""),
    ("digital_txn_pct", 0),
)
_RISK_DEFAULTS = (
    ("composite_risk_score", None),
    ("risk_tier", "UNKNOWN"),
    ("probability_of_default", None),
    ("watch_list_flag", "N"),
)


def apply_defaults(df: DataFrame) -> DataFrame:
    """Port the ``if not _seg/_txn/_risk then do; ... end;`` default blocks.

    A null presence flag means the product row was absent (left join miss), so the
    SAS default is applied; a matched row keeps its joined value verbatim.
    """
    for flag, defaults in (("_seg", _SEG_DEFAULTS), ("_txn", _TXN_DEFAULTS), ("_risk", _RISK_DEFAULTS)):
        absent = F.col(flag).isNull()
        for col, default in defaults:
            df = df.withColumn(col, F.when(absent, F.lit(default)).otherwise(F.col(col)))
    return df


def transform(
    stg_customer_360: DataFrame,
    customer_segments: DataFrame,
    transaction_analytics: DataFrame,
    customer_risk_scores: DataFrame,
    config: PipelineConfig,
) -> DataFrame:
    """Build CUSTOMER_MASTER_PROFILE (schema-enforced to the DDL)."""
    run_date = config.run_date

    base = base_profile(stg_customer_360)
    seg = segment_features(customer_segments)
    txn = txn_features(transaction_analytics, run_date)
    risk = risk_features(customer_risk_scores)

    merged = (
        base
        .join(seg, "customer_id", "left")
        .join(txn, "customer_id", "left")
        .join(risk, "customer_id", "left")
    )
    merged = apply_defaults(merged)

    out = (
        merged
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("effective_date", F.lit(run_date))
        .withColumn("load_ts", F.lit(load_timestamp()).cast("timestamp"))
    )
    return schemas.enforce_schema(out, schemas.CUSTOMER_MASTER_PROFILE)


def run(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog | None = None,
) -> DataFrame:
    """Read BASE + the three products, merge, validate, and write the golden record."""
    audit = audit or AuditLog(log_level=config.log_level)
    audit.log_step(JOB_NAME, "START", "Building golden record")

    base = io.read_staging("STG_CUSTOMER_360")
    segments = io.read_data_product("CUSTOMER_SEGMENTS")
    txn = io.read_data_product("TRANSACTION_ANALYTICS")
    risk = io.read_data_product("CUSTOMER_RISK_SCORES")

    out = transform(base, segments, txn, risk, config).cache()
    n = out.count()

    result = validate_table(
        out,
        TARGET,
        key_cols=["customer_id"],
        not_null=["customer_id", "full_name", "customer_status"],
        min_rows=1,
        audit=audit,
    )
    abort_on_failure(result)

    io.write_data_product(out, TARGET)
    audit.run_log_row(JOB_NAME, n)
    audit.log_step(JOB_NAME, "SUCCESS", "Golden record loaded", rowcount=n)
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build CUSTOMER_MASTER_PROFILE")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--lake-dir", required=True)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args(argv)

    config = PipelineConfig.from_env().with_overrides(
        **({"run_date": _dt.date.fromisoformat(args.run_date)} if args.run_date else {})
    )
    spark = build_spark(JOB_NAME)
    io = LocalDataIO(spark, config, args.source_dir, args.lake_dir)
    run(spark, io, config)


if __name__ == "__main__":
    main()
