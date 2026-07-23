"""End-to-end pipeline driver (Ticket 10).

Each ``run_*`` function is a self-contained step: it reads its Unity Catalog
inputs, applies the ported transformation, writes its Delta output, validates it
(raising on failure) and records an audit row. ``run_pipeline`` wires the steps
in dependency order:

    stg_customer_360 ─┬─> stg_txn_summary ─> transaction_analytics ─┐
                      ├─> stg_risk_factors ─> customer_risk_scores ─┤
                      └─> customer_segments ───────────────────────┴─> master_profile
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from common import audit
from common.validation import validate_dataframe
from jobs import (
    customer_segments,
    master_profile,
    risk_scoring,
    stg_customer_360,
    stg_risk_factors,
    stg_txn_summary,
    transaction_analytics,
)


def _write(df: DataFrame, fqn: str, partition_by: str | None = None) -> DataFrame:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by is not None:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(fqn)
    return df


def _log(spark, config, job_name, status, message="", row_count=None, start_ts=None):
    audit.log_step(
        spark, config, job_name=job_name, status=status, message=message,
        row_count=row_count, start_ts=start_ts, end_ts=datetime.now(),
    )


def run_stg_customer_360(spark: SparkSession, config, min_rows: int = 1) -> int:
    start = datetime.now()
    _log(spark, config, "04_stg_customer_360", "START", start_ts=start)
    df = stg_customer_360.build_stg_customer_360(
        spark.table(config.core("customers")),
        spark.table(config.core("accounts")),
        spark.table(config.core("addresses")),
        config.run_date,
        config.load_ts,
    )
    fqn = config.staging("stg_customer_360")
    _write(df, fqn)
    n = validate_dataframe(
        spark.table(fqn), name=fqn, key_cols=["customer_id"],
        not_null=["customer_id"], min_rows=min_rows,
    )
    _log(spark, config, "04_stg_customer_360", "SUCCESS", row_count=n, start_ts=start)
    return n


def run_stg_txn_summary(spark: SparkSession, config, min_rows: int = 1) -> int:
    start = datetime.now()
    _log(spark, config, "05_stg_txn_summary", "START", start_ts=start)
    df = stg_txn_summary.build_stg_txn_summary(
        spark.table(config.txn("transactions")),
        spark.table(config.txn("transaction_types")),
        spark.table(config.core("accounts")),
        config.lookback_months,
        config.run_date,
        config.load_ts,
    )
    fqn = config.staging("stg_txn_summary")
    _write(df, fqn)
    n = validate_dataframe(
        spark.table(fqn), name=fqn, key_cols=["customer_id", "account_id"],
        not_null=["customer_id", "account_id"], min_rows=min_rows,
    )
    _log(spark, config, "05_stg_txn_summary", "SUCCESS", row_count=n, start_ts=start)
    return n


def run_stg_risk_factors(spark: SparkSession, config, min_rows: int = 1) -> int:
    start = datetime.now()
    _log(spark, config, "06_stg_risk_factors", "START", start_ts=start)
    df = stg_risk_factors.build_stg_risk_factors(
        spark.table(config.core("customers")),
        spark.table(config.core("accounts")),
        spark.table(config.txn("transactions")),
        spark.table(config.txn("transaction_types")),
        spark.table(config.core("customer_bureau_scores")),
        config.run_date,
        config.load_ts,
    )
    fqn = config.staging("stg_risk_factors")
    _write(df, fqn)
    n = validate_dataframe(
        spark.table(fqn), name=fqn, key_cols=["customer_id"],
        not_null=["customer_id"], min_rows=min_rows,
    )
    _log(spark, config, "06_stg_risk_factors", "SUCCESS", row_count=n, start_ts=start)
    return n


def run_customer_segments(spark: SparkSession, config, min_rows: int = 1) -> int:
    start = datetime.now()
    _log(spark, config, "07_customer_segments", "START", start_ts=start)
    df = customer_segments.build_customer_segments(
        spark.table(config.staging("stg_customer_360")), config.run_date, config.load_ts,
    )
    fqn = config.product("customer_segments")
    _write(df, fqn)
    n = validate_dataframe(
        spark.table(fqn), name=fqn, key_cols=["customer_id"],
        not_null=["customer_id", "segment_name", "segment_id"], min_rows=min_rows,
    )
    _log(spark, config, "07_customer_segments", "SUCCESS", row_count=n, start_ts=start)
    return n


def run_transaction_analytics(spark: SparkSession, config, min_rows: int = 1) -> int:
    start = datetime.now()
    _log(spark, config, "08_transaction_analytics", "START", start_ts=start)
    df = transaction_analytics.build_transaction_analytics(
        spark.table(config.staging("stg_txn_summary")), config.run_date, config.load_ts,
    )
    fqn = config.product("transaction_analytics")
    _write(df, fqn, partition_by="reporting_period")
    n = validate_dataframe(
        spark.table(fqn), name=fqn, key_cols=["customer_id"],
        not_null=["customer_id", "reporting_period", "total_transactions"], min_rows=min_rows,
    )
    _log(spark, config, "08_transaction_analytics", "SUCCESS", row_count=n, start_ts=start)
    return n


def run_customer_risk_scores(spark: SparkSession, config, min_rows: int = 1) -> int:
    start = datetime.now()
    _log(spark, config, "09_customer_risk_scores", "START", start_ts=start)
    df = risk_scoring.build_customer_risk_scores(
        spark.table(config.staging("stg_risk_factors")),
        spark.table(config.staging("stg_customer_360")),
        config.run_date,
        config.load_ts,
    )
    fqn = config.product("customer_risk_scores")
    _write(df, fqn)
    n = validate_dataframe(
        spark.table(fqn), name=fqn, key_cols=["customer_id"],
        not_null=["customer_id", "composite_risk_score", "risk_tier"], min_rows=min_rows,
    )
    _log(spark, config, "09_customer_risk_scores", "SUCCESS", row_count=n, start_ts=start)
    return n


def run_master_profile(spark: SparkSession, config, min_rows: int = 1) -> int:
    start = datetime.now()
    _log(spark, config, "10_master_profile", "START", start_ts=start)
    df = master_profile.build_master_profile(
        spark.table(config.staging("stg_customer_360")),
        spark.table(config.product("customer_segments")),
        spark.table(config.product("transaction_analytics")),
        spark.table(config.product("customer_risk_scores")),
        config.run_date,
        config.load_ts,
    )
    fqn = config.product("customer_master_profile")
    _write(df, fqn)
    n = validate_dataframe(
        spark.table(fqn), name=fqn, key_cols=["customer_id"],
        not_null=["customer_id", "full_name", "customer_status"], min_rows=min_rows,
    )
    _log(spark, config, "10_master_profile", "SUCCESS", row_count=n, start_ts=start)
    return n


def run_pipeline(spark: SparkSession, config, min_rows: int = 1) -> dict:
    """Run every step in dependency order. Returns a name -> row-count dict."""
    audit.init_audit(spark, config)
    _log(spark, config, "00_pipeline", "START", message=f"run_date={config.run_date}")
    counts = {
        "stg_customer_360": run_stg_customer_360(spark, config, min_rows),
        "stg_txn_summary": run_stg_txn_summary(spark, config, min_rows),
        "stg_risk_factors": run_stg_risk_factors(spark, config, min_rows),
        "customer_segments": run_customer_segments(spark, config, min_rows),
        "transaction_analytics": run_transaction_analytics(spark, config, min_rows),
        "customer_risk_scores": run_customer_risk_scores(spark, config, min_rows),
        "customer_master_profile": run_master_profile(spark, config, min_rows),
    }
    _log(spark, config, "00_pipeline", "SUCCESS", message="pipeline complete")
    return counts
