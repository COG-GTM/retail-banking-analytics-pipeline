import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..audit import StepLogger, assert_rows, step
from ..config import RunConfig
from ..dq import validate_table
from ..tables import write_overwrite

MODEL_VERSION = "MASTER_V1.5"


def _base(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    customers = spark.table(cfg.fqn(cfg.silver_schema, "stg_customer_360")).where(
        F.col("customer_status") == "A"
    )
    return customers.select(
        "customer_id",
        F.concat_ws(" ", F.trim("first_name"), F.trim("last_name")).alias("full_name"),
        "age",
        "state_code",
        "customer_since",
        "tenure_months",
        "customer_status",
        F.col("num_accounts").alias("total_accounts"),
        F.col("num_active_accounts").alias("active_accounts"),
        "total_balance",
        "total_credit_limit",
        "credit_utilization_pct",
    )


def build(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    base = _base(spark, cfg)
    segments = spark.table(cfg.fqn(cfg.gold_schema, "customer_segments")).select(
        "customer_id",
        "segment_name",
        "lifetime_value_score",
        "engagement_score",
        "cross_sell_flag",
        "upsell_flag",
        "retention_risk_flag",
    )
    transactions = (
        spark.table(cfg.fqn(cfg.gold_schema, "transaction_analytics"))
        .where(F.col("reporting_period") == cfg.run_date.strftime("%Y-%m"))
        .select(
            "customer_id",
            F.col("total_transactions").alias("monthly_transactions"),
            F.col("total_debit_amt").alias("monthly_spend"),
            "net_cash_flow",
            "top_spend_category",
            "digital_txn_pct",
        )
    )
    risk = spark.table(cfg.fqn(cfg.gold_schema, "customer_risk_scores")).select(
        "customer_id",
        "composite_risk_score",
        "risk_tier",
        "probability_of_default",
        "watch_list_flag",
    )
    return (
        base.join(segments, "customer_id", "left")
        .join(transactions, "customer_id", "left")
        .join(risk, "customer_id", "left")
        .select(
            "customer_id",
            "full_name",
            F.col("age").cast("smallint").alias("age"),
            "state_code",
            "customer_since",
            F.col("tenure_months").cast("int").alias("tenure_months"),
            "customer_status",
            F.coalesce("segment_name", F.lit("UNCLASSIFIED")).alias("segment_name"),
            F.coalesce("lifetime_value_score", F.lit(0))
            .cast("decimal(10,2)")
            .alias("lifetime_value_score"),
            F.coalesce("engagement_score", F.lit(0)).cast("decimal(5,2)").alias("engagement_score"),
            F.col("total_accounts").cast("smallint").alias("total_accounts"),
            F.col("active_accounts").cast("smallint").alias("active_accounts"),
            F.col("total_balance").cast("decimal(18,2)").alias("total_balance"),
            F.col("total_credit_limit").cast("decimal(18,2)").alias("total_credit_limit"),
            F.col("credit_utilization_pct").cast("decimal(5,2)").alias("credit_utilization_pct"),
            F.coalesce("monthly_transactions", F.lit(0)).cast("int").alias("monthly_transactions"),
            F.coalesce("monthly_spend", F.lit(0)).cast("decimal(18,2)").alias("monthly_spend"),
            F.coalesce("net_cash_flow", F.lit(0)).cast("decimal(18,2)").alias("net_cash_flow"),
            F.coalesce("top_spend_category", F.lit("")).alias("top_spend_category"),
            F.coalesce("digital_txn_pct", F.lit(0)).cast("decimal(5,2)").alias("digital_txn_pct"),
            F.col("composite_risk_score").cast("decimal(6,2)").alias("composite_risk_score"),
            F.coalesce("risk_tier", F.lit("UNKNOWN")).alias("risk_tier"),
            F.col("probability_of_default").cast("decimal(7,6)").alias("probability_of_default"),
            F.coalesce("watch_list_flag", F.lit("N")).alias("watch_list_flag"),
            F.coalesce("cross_sell_flag", F.lit("N")).alias("cross_sell_flag"),
            F.coalesce("upsell_flag", F.lit("N")).alias("upsell_flag"),
            F.coalesce("retention_risk_flag", F.lit("N")).alias("retention_risk_flag"),
            F.lit(MODEL_VERSION).alias("model_version"),
            F.lit(cfg.run_date).cast("date").alias("effective_date"),
            F.current_timestamp().alias("load_ts"),
        )
    )


def run(spark: SparkSession, cfg: RunConfig):
    fqn = cfg.fqn(cfg.gold_schema, "customer_master_profile")
    with step(spark, cfg, "04_customer_master_profile", "FULL_LOAD") as state:
        result = build(spark, cfg)
        state["row_count"] = assert_rows(result, "customer_master_profile")
        write_overwrite(result, fqn)
        completeness = {
            "total": result.count(),
            "with_segment": result.where(F.col("segment_name") != "UNCLASSIFIED").count(),
            "with_txn": result.where(F.col("monthly_transactions") > 0).count(),
            "with_risk": result.where(F.col("risk_tier") != "UNKNOWN").count(),
        }
        segment_distribution = {
            row["segment_name"]: row["count"]
            for row in result.groupBy("segment_name").count().collect()
        }
        risk_distribution = {
            row["risk_tier"]: row["count"] for row in result.groupBy("risk_tier").count().collect()
        }
        report = {
            **completeness,
            "segment_distribution": segment_distribution,
            "risk_tier_distribution": risk_distribution,
        }
        StepLogger.log_step(
            spark,
            cfg,
            "04_customer_master_profile",
            "COMPLETENESS",
            "SUCCESS",
            json.dumps(report, sort_keys=True),
            completeness["total"],
        )
    validate_table(
        spark,
        fqn,
        ["customer_id"],
        ["customer_id", "full_name", "customer_status"],
        cfg.dq_min_rows,
    )
    return result, report
