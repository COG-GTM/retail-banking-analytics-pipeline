"""Job: data_products -- build the CUSTOMER_MASTER_PROFILE golden record.

PySpark port of ``sas/04_sas_data_products.sas``.

Legacy construct -> PySpark mapping
-----------------------------------
* SAS ``DATA ... MERGE ... BY CUSTOMER_ID`` with ``IN=`` flags and ``if _base``
  (a 4-way outer merge kept to base rows) -> a base ``LEFT JOIN`` of
  ``etl_staging.stg_customer_360`` (filtered to active customers) against the
  three gold data products (``customer_segments``, ``transaction_analytics``,
  ``customer_risk_scores``) on ``customer_id``.
* SAS ``if not _seg / _txn / _risk then do; ... end;`` default blocks ->
  ``coalesce`` defaults for missing segment / transaction / risk attributes,
  mirroring the legacy defaults exactly (UNCLASSIFIED / 0 / UNKNOWN / 'N').
* SAS ``where EFFECTIVE_DATE = today()`` on TRANSACTION_ANALYTICS ("current
  period only") -> keep the latest ``effective_date`` row per customer
  (deterministic, so re-runs are reproducible).
* SAS ``%validate_table`` + ``%ABORT CANCEL`` -> :func:`validate_table`.
* SAS ``%log_step`` + ``WORK.PIPELINE_AUDIT`` -> :func:`log_step`.
* SAS ``proc sql DELETE`` + ``proc append`` -> idempotent Delta overwrite.

Produces: ``data_products.customer_master_profile``.
"""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.audit import init_audit, log_step
from common.config import Config
from common.spark import read_delta, write_delta
from common.validation import validate_table

JOB_NAME = "data_products"
MODEL_VERSION = "MASTER_V1.5"

# Final CUSTOMER_MASTER_PROFILE column order + types (ddl/02_data_product_tables.sql).
OUTPUT_TYPES: dict[str, T.DataType] = {
    "customer_id": T.LongType(),
    "full_name": T.StringType(),
    "age": T.IntegerType(),
    "state_code": T.StringType(),
    "customer_since": T.DateType(),
    "tenure_months": T.IntegerType(),
    "customer_status": T.StringType(),
    "segment_name": T.StringType(),
    "lifetime_value_score": T.DecimalType(10, 2),
    "engagement_score": T.DecimalType(5, 2),
    "total_accounts": T.IntegerType(),
    "active_accounts": T.IntegerType(),
    "total_balance": T.DecimalType(18, 2),
    "total_credit_limit": T.DecimalType(18, 2),
    "credit_utilization_pct": T.DecimalType(5, 2),
    "monthly_transactions": T.IntegerType(),
    "monthly_spend": T.DecimalType(18, 2),
    "net_cash_flow": T.DecimalType(18, 2),
    "top_spend_category": T.StringType(),
    "digital_txn_pct": T.DecimalType(5, 2),
    "composite_risk_score": T.DecimalType(6, 2),
    "risk_tier": T.StringType(),
    "probability_of_default": T.DecimalType(7, 6),
    "watch_list_flag": T.StringType(),
    "cross_sell_flag": T.StringType(),
    "upsell_flag": T.StringType(),
    "retention_risk_flag": T.StringType(),
    "model_version": T.StringType(),
    "effective_date": T.DateType(),
    "load_ts": T.TimestampType(),
}


def _latest_per_customer(df: DataFrame) -> DataFrame:
    """Keep the most recent row per customer_id (SAS "current period only")."""
    order_cols = []
    if "effective_date" in df.columns:
        order_cols.append(F.col("effective_date").desc_nulls_last())
    if "load_ts" in df.columns:
        order_cols.append(F.col("load_ts").desc_nulls_last())
    if not order_cols:
        return df.dropDuplicates(["customer_id"])
    window = Window.partitionBy("customer_id").orderBy(*order_cols)
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_master_profile(
    base: DataFrame,
    segments: DataFrame,
    txn: DataFrame,
    risk: DataFrame,
    *,
    run_date: str,
) -> DataFrame:
    """Assemble the golden record from the four upstream inputs (pure transform)."""
    base_sel = base.filter(F.col("customer_status") == F.lit("A")).select(
        F.col("customer_id"),
        F.concat_ws(
            " ", F.trim(F.col("first_name")), F.trim(F.col("last_name"))
        ).alias("full_name"),
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

    seg_sel = _latest_per_customer(segments).select(
        "customer_id",
        "segment_name",
        "lifetime_value_score",
        "engagement_score",
        "cross_sell_flag",
        "upsell_flag",
        "retention_risk_flag",
    )

    txn_sel = _latest_per_customer(txn).select(
        "customer_id",
        F.col("total_transactions").alias("monthly_transactions"),
        F.col("total_debit_amt").alias("monthly_spend"),
        F.col("net_cash_flow"),
        F.col("top_spend_category"),
        F.col("digital_txn_pct"),
    )

    risk_sel = _latest_per_customer(risk).select(
        "customer_id",
        "composite_risk_score",
        "risk_tier",
        "probability_of_default",
        "watch_list_flag",
    )

    joined = (
        base_sel.join(seg_sel, "customer_id", "left")
        .join(txn_sel, "customer_id", "left")
        .join(risk_sel, "customer_id", "left")
    )

    profile = joined.select(
        F.col("customer_id"),
        F.col("full_name"),
        F.col("age"),
        F.col("state_code"),
        F.col("customer_since"),
        F.col("tenure_months"),
        F.col("customer_status"),
        # Segment defaults (SAS: if not _seg)
        F.coalesce(F.col("segment_name"), F.lit("UNCLASSIFIED")).alias("segment_name"),
        F.coalesce(F.col("lifetime_value_score"), F.lit(0)).alias("lifetime_value_score"),
        F.coalesce(F.col("engagement_score"), F.lit(0)).alias("engagement_score"),
        F.col("total_accounts"),
        F.col("active_accounts"),
        F.col("total_balance"),
        F.col("total_credit_limit"),
        F.col("credit_utilization_pct"),
        # Transaction defaults (SAS: if not _txn)
        F.coalesce(F.col("monthly_transactions"), F.lit(0)).alias("monthly_transactions"),
        F.coalesce(F.col("monthly_spend"), F.lit(0)).alias("monthly_spend"),
        F.coalesce(F.col("net_cash_flow"), F.lit(0)).alias("net_cash_flow"),
        F.coalesce(F.col("top_spend_category"), F.lit("")).alias("top_spend_category"),
        F.coalesce(F.col("digital_txn_pct"), F.lit(0)).alias("digital_txn_pct"),
        # Risk defaults (SAS: if not _risk) -- scores stay NULL, tier/flag defaulted
        F.col("composite_risk_score"),
        F.coalesce(F.col("risk_tier"), F.lit("UNKNOWN")).alias("risk_tier"),
        F.col("probability_of_default"),
        F.coalesce(F.col("watch_list_flag"), F.lit("N")).alias("watch_list_flag"),
        F.coalesce(F.col("cross_sell_flag"), F.lit("N")).alias("cross_sell_flag"),
        F.coalesce(F.col("upsell_flag"), F.lit("N")).alias("upsell_flag"),
        F.coalesce(F.col("retention_risk_flag"), F.lit("N")).alias("retention_risk_flag"),
        # Metadata
        F.lit(MODEL_VERSION).alias("model_version"),
        F.to_date(F.lit(run_date)).alias("effective_date"),
        F.current_timestamp().alias("load_ts"),
    )

    return profile.select(
        *[F.col(c).cast(t).alias(c) for c, t in OUTPUT_TYPES.items()]
    )


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    """Build and persist ``data_products.customer_master_profile``."""
    run_id = str(uuid.uuid4())
    init_audit(spark, cfg)
    log_step(spark, cfg, run_id, JOB_NAME, "extract", "START",
             message="Reading upstream data products")

    base = read_delta(spark, cfg, cfg.schema_stg, "stg_customer_360")
    segments = read_delta(spark, cfg, cfg.schema_dp, "customer_segments")
    txn = read_delta(spark, cfg, cfg.schema_dp, "transaction_analytics")
    risk = read_delta(spark, cfg, cfg.schema_dp, "customer_risk_scores")

    profile = build_master_profile(base, segments, txn, risk, run_date=cfg.run_date)

    validate_table(
        profile,
        min_rows=1,
        not_null_cols=["customer_id", "full_name", "customer_status"],
        unique_keys=["customer_id"],
    )

    write_delta(profile, cfg, cfg.schema_dp, "customer_master_profile", mode="overwrite")

    result = read_delta(spark, cfg, cfg.schema_dp, "customer_master_profile")
    log_step(spark, cfg, run_id, JOB_NAME, "load", "SUCCESS",
             row_count=result.count(),
             message="Golden record customer_master_profile written")
    return result
