"""PySpark translation of sas/03_sas_risk_scoring.sas.

Joins the risk-factor staging table with customer-360 baseline attributes,
prepares model features, trains a logistic regression for probability of
default (pyspark.ml replaces PROC LOGISTIC), computes the weighted composite
risk score, and classifies customers into risk tiers.
"""
from __future__ import annotations

import datetime as dt

from common import DEFAULT_RUN_DATE
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MODEL_VERSION = "RISK_V4.0"

# Model features (mirrors the PROC LOGISTIC MODEL statement)
MODEL_FEATURES = [
    "bureau_score_norm",
    "credit_util_ratio",
    "payment_ontime_pct",
    "balance_volatility",
    "velocity_ratio",
    "account_overdraft_cnt",
    "large_withdrawal_cnt",
    "high_risk_merchant_cnt",
    "tenure_months",
]


def build_customer_risk_scores(
    spark: SparkSession,
    stg_risk_factors: DataFrame,
    stg_customer_360: DataFrame,
    run_date: dt.date = DEFAULT_RUN_DATE,
    pd_floor: float = 0.05,
) -> DataFrame:
    """Return the CUSTOMER_RISK_SCORES data product for active customers."""
    # STEP 1: join risk factors with baseline attributes, active customers only
    base = stg_risk_factors.alias("r").join(
        stg_customer_360.filter(F.col("customer_status") == "A").select(
            "customer_id", "tenure_months", "num_active_accounts", "total_balance"
        ).alias("c"),
        "customer_id",
        "inner",
    )

    # STEP 2: feature preparation - imputation and derived ratios
    feats = (
        base
        # Impute missing bureau scores with the population median placeholder
        .withColumn(
            "external_credit_score",
            F.when(F.col("external_credit_score") <= 0, 680)
            .when(F.col("external_credit_score").isNull(), 680)
            .otherwise(F.col("external_credit_score")),
        )
        # Normalise bureau score from the 300-850 range onto 0-100
        .withColumn(
            "bureau_score_norm",
            (F.col("external_credit_score") - 300) / (850 - 300) * 100,
        )
        # Balance trend: 30-day average relative to 90-day average
        .withColumn(
            "balance_trend_ratio",
            F.when(
                F.col("avg_daily_balance_90d") > 0,
                F.col("avg_daily_balance_30d") / F.col("avg_daily_balance_90d"),
            ).otherwise(1.0),
        )
        # Velocity ratio: annualised 7-day debit pace vs 30-day pace
        .withColumn(
            "velocity_ratio",
            F.when(
                F.col("debit_velocity_30d") > 0,
                (F.col("debit_velocity_7d") * (30.0 / 7.0)) / F.col("debit_velocity_30d"),
            ).otherwise(1.0),
        )
        # Default proxy target: more than two late payments
        .withColumn("default_flag", (F.col("payment_late_cnt") > 2).cast("double"))
    )

    # STEP 3: logistic regression for probability of default
    assembled = VectorAssembler(
        inputCols=MODEL_FEATURES, outputCol="features", handleInvalid="keep"
    ).transform(feats)
    positives = assembled.filter(F.col("default_flag") == 1.0).count()
    if positives > 0 and positives < assembled.count():
        model = LogisticRegression(
            featuresCol="features", labelCol="default_flag", maxIter=50
        ).fit(assembled)
        extract_p1 = F.udf(lambda v: float(v[1]), "double")
        scored = model.transform(assembled).withColumn(
            "prob_default", extract_p1(F.col("probability"))
        )
    else:
        # Degenerate target (all one class): probability falls back to the floor
        scored = assembled.withColumn("prob_default", F.lit(float(pd_floor)))

    clip = lambda c: F.greatest(F.lit(0.0), F.least(F.lit(100.0), c))

    # STEP 4: component scores, weighted composite, tiers, and drivers
    classified = (
        scored
        .withColumn("credit_risk_component", clip(100 - F.col("bureau_score_norm")))
        .withColumn("behaviour_risk_component", clip(100 - F.col("payment_ontime_pct")))
        .withColumn("velocity_risk_component", clip((F.col("velocity_ratio") - 1) * 50))
        .withColumn("bureau_score_component", clip(F.col("bureau_score_norm")))
        .withColumn("payment_history_component", clip(F.col("payment_ontime_pct")))
        .withColumn(
            # Weighted composite: higher = higher risk
            "composite_risk_score",
            F.round(
                F.col("credit_risk_component") * 0.30
                + F.col("behaviour_risk_component") * 0.25
                + F.col("velocity_risk_component") * 0.15
                + (100 - F.col("bureau_score_component")) * 0.20
                + (100 - F.col("payment_history_component")) * 0.10,
                2,
            ),
        )
        # Probability of default floored at pd_floor (matches golden output)
        .withColumn(
            "probability_of_default",
            F.round(F.greatest(F.coalesce(F.col("prob_default"), F.lit(0.0)),
                               F.lit(float(pd_floor))), 6),
        )
        .withColumn(
            "risk_tier",
            F.when(F.col("composite_risk_score") < 20, "LOW")
            .when(F.col("composite_risk_score") < 40, "MODERATE")
            .when(F.col("composite_risk_score") < 60, "ELEVATED")
            .when(F.col("composite_risk_score") < 80, "HIGH")
            .otherwise("CRITICAL"),
        )
    )

    # Primary/secondary risk drivers: top two of the four driver components.
    # Values are rounded to 6 dp so that arithmetically-equal components tie,
    # and ties resolve in the legacy scan order: BUREAU_SCORE, then
    # TRANSACTION_VELOCITY, then CREDIT_UTILIZATION, then PAYMENT_BEHAVIOUR.
    driver_cols = F.array(
        F.struct(F.round(100 - F.col("bureau_score_component"), 6).alias("v"),
                 F.lit("BUREAU_SCORE").alias("l")),
        F.struct(F.round(F.col("velocity_risk_component"), 6).alias("v"),
                 F.lit("TRANSACTION_VELOCITY").alias("l")),
        F.struct(F.round(F.col("credit_risk_component"), 6).alias("v"),
                 F.lit("CREDIT_UTILIZATION").alias("l")),
        F.struct(F.round(F.col("behaviour_risk_component"), 6).alias("v"),
                 F.lit("PAYMENT_BEHAVIOUR").alias("l")),
    )
    # array_sort with a comparator is stable, preserving the tie order above
    with_drivers = classified.withColumn("_drivers", driver_cols).withColumn(
        "_sorted",
        F.expr(
            "array_sort(_drivers, (a, b) -> CASE WHEN a.v > b.v THEN -1 "
            "WHEN a.v < b.v THEN 1 ELSE 0 END)"
        ),
    )

    return with_drivers.select(
        "customer_id",
        "composite_risk_score",
        "risk_tier",
        "probability_of_default",
        "credit_risk_component",
        "behaviour_risk_component",
        "velocity_risk_component",
        "bureau_score_component",
        "payment_history_component",
        F.col("_sorted")[0]["l"].alias("primary_risk_driver"),
        F.col("_sorted")[1]["l"].alias("secondary_risk_driver"),
        # Placeholder: production would diff against the prior day's score
        F.lit(0.0).alias("score_delta_30d"),
        # Watch list: critical tier with high probability of default
        F.when(
            (F.col("risk_tier") == "CRITICAL") & (F.col("probability_of_default") > 0.5), "Y"
        )
        .otherwise("N")
        .alias("watch_list_flag"),
        # Review: elevated-or-worse score with a recent velocity spike
        F.when(
            (F.col("composite_risk_score") >= 60) & (F.col("velocity_ratio") > 2.0), "Y"
        )
        .otherwise("N")
        .alias("review_required_flag"),
        F.lit(MODEL_VERSION).alias("model_version"),
        F.lit(run_date).alias("effective_date"),
        F.current_timestamp().alias("load_ts"),
    )
