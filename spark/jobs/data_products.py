"""04 - Golden record assembly (PySpark port of ``04_sas_data_products.sas``).

SAS -> PySpark mapping
    PROC SQL extracts        -> DataFrame selects
    DATA step 4-way MERGE    -> left joins on CUSTOMER_ID with default handling
    COLLECT STATISTICS       -> dropped (no Spark equivalent)

Reads  ETL_STAGING_DB.STG_CUSTOMER_360 + the three upstream data products
Writes DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ..config import PipelineConfig
from ..logging_utils import PipelineAudit
from ..session import DataLayer, get_spark
from ..validation import enforce, validate_table

STEP = "04_MASTER_PROFILE"

OUTPUT_COLUMNS = [
    "customer_id", "full_name", "age", "state_code", "customer_since",
    "tenure_months", "customer_status", "segment_name", "lifetime_value_score",
    "engagement_score", "total_accounts", "active_accounts", "total_balance",
    "total_credit_limit", "credit_utilization_pct", "monthly_transactions",
    "monthly_spend", "net_cash_flow", "top_spend_category", "digital_txn_pct",
    "composite_risk_score", "risk_tier", "probability_of_default",
    "watch_list_flag", "cross_sell_flag", "upsell_flag", "retention_risk_flag",
    "model_version", "effective_date", "load_ts",
]


def extract_base(cust360: DataFrame) -> DataFrame:
    active = cust360.where(F.col("customer_status") == "A")
    return active.select(
        F.col("customer_id"),
        F.concat_ws(" ", F.trim(F.col("first_name")), F.trim(F.col("last_name"))).alias("full_name"),
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


def extract_segments(segments: DataFrame) -> DataFrame:
    return segments.select(
        "customer_id", "segment_name", "lifetime_value_score", "engagement_score",
        "cross_sell_flag", "upsell_flag", "retention_risk_flag",
    )


def extract_txn(txn: DataFrame, config: PipelineConfig) -> DataFrame:
    return txn.where(F.col("effective_date").cast("string") == config.effective_date).select(
        "customer_id",
        F.col("total_transactions").alias("monthly_transactions"),
        F.col("total_debit_amt").alias("monthly_spend"),
        F.col("net_cash_flow"),
        F.col("top_spend_category"),
        F.col("digital_txn_pct"),
    )


def extract_risk(risk: DataFrame) -> DataFrame:
    return risk.select(
        "customer_id", "composite_risk_score", "risk_tier",
        "probability_of_default", "watch_list_flag",
    )


def build_master_profile(base: DataFrame, segments: DataFrame, txn: DataFrame,
                         risk: DataFrame, config: PipelineConfig) -> DataFrame:
    merged = (
        base
        .join(segments, on="customer_id", how="left")
        .join(txn, on="customer_id", how="left")
        .join(risk, on="customer_id", how="left")
    )

    return merged.select(
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("full_name"),
        F.col("age").cast("int").alias("age"),
        F.col("state_code"),
        F.col("customer_since"),
        F.col("tenure_months").cast("int").alias("tenure_months"),
        F.col("customer_status"),
        # segment defaults (SAS: if not _seg)
        F.coalesce(F.col("segment_name"), F.lit("UNCLASSIFIED")).alias("segment_name"),
        F.coalesce(F.col("lifetime_value_score"), F.lit(0.0)).alias("lifetime_value_score"),
        F.coalesce(F.col("engagement_score"), F.lit(0.0)).alias("engagement_score"),
        F.col("total_accounts").cast("int").alias("total_accounts"),
        F.col("active_accounts").cast("int").alias("active_accounts"),
        F.col("total_balance"),
        F.col("total_credit_limit"),
        F.col("credit_utilization_pct"),
        # txn defaults (SAS: if not _txn)
        F.coalesce(F.col("monthly_transactions"), F.lit(0)).cast("int").alias("monthly_transactions"),
        F.coalesce(F.col("monthly_spend"), F.lit(0.0)).alias("monthly_spend"),
        F.coalesce(F.col("net_cash_flow"), F.lit(0.0)).alias("net_cash_flow"),
        F.coalesce(F.col("top_spend_category"), F.lit("")).alias("top_spend_category"),
        F.coalesce(F.col("digital_txn_pct"), F.lit(0.0)).alias("digital_txn_pct"),
        # risk defaults (SAS: if not _risk)
        F.col("composite_risk_score"),
        F.coalesce(F.col("risk_tier"), F.lit("UNKNOWN")).alias("risk_tier"),
        F.col("probability_of_default"),
        F.coalesce(F.col("watch_list_flag"), F.lit("N")).alias("watch_list_flag"),
        F.coalesce(F.col("cross_sell_flag"), F.lit("N")).alias("cross_sell_flag"),
        F.coalesce(F.col("upsell_flag"), F.lit("N")).alias("upsell_flag"),
        F.coalesce(F.col("retention_risk_flag"), F.lit("N")).alias("retention_risk_flag"),
        F.lit(config.master_model_version).alias("model_version"),
        F.lit(config.effective_date).alias("effective_date"),
        F.lit(config.run_ts).alias("load_ts"),
    ).select(*OUTPUT_COLUMNS)


def run(config: PipelineConfig) -> str:
    audit = PipelineAudit(run_id=config.run_id)
    spark = get_spark(config)
    data = DataLayer(spark, config)

    audit.log_step(step=STEP, status="START", msg="Building golden record")
    base = extract_base(data.read_staging("stg_customer_360"))
    segments = extract_segments(data.read_product("customer_segments"))
    txn = extract_txn(data.read_product("transaction_analytics"), config)
    risk = extract_risk(data.read_product("customer_risk_scores"))
    audit.log_step(step=STEP, status="SUCCESS", msg="All upstream data extracted")

    result_df = build_master_profile(base, segments, txn, risk, config).cache()
    row_count = result_df.count()
    audit.log_step(step=STEP, status="SUCCESS", msg="Master profile built", rowcount=row_count)

    validation = validate_table(
        result_df, table="CUSTOMER_MASTER_PROFILE",
        key_cols=["customer_id"],
        not_null=["customer_id", "full_name", "customer_status"],
        min_rows=config.min_rows, audit=audit,
    )
    enforce(validation, "CUSTOMER_MASTER_PROFILE", audit)

    audit.log_step(step=STEP, status="START", msg="Loading DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE")
    path = data.write_product(result_df, "customer_master_profile")
    # COLLECT STATISTICS has no Spark equivalent and is intentionally omitted.
    audit.log_step(step=STEP, status="SUCCESS", msg=f"Golden record loaded -> {path}", rowcount=row_count)
    result_df.unpersist()
    return path


if __name__ == "__main__":
    run(PipelineConfig.from_env())
