"""Ticket 9 - Risk scoring.

PySpark / Spark MLlib port of ``sas/03_sas_risk_scoring.sas``.

Inputs  : etl_staging.stg_risk_factors, etl_staging.stg_customer_360
Output  : data_products.customer_risk_scores  (Delta)

    * PROC LOGISTIC -> ml.classification.LogisticRegression (probability of default)
    * weighted composite score + risk-tier classification preserved verbatim
    * legacy PAYMENT_HISTORY_COMP -> renamed to PAYMENT_HISTORY_COMPONENT on output
"""
from __future__ import annotations

from datetime import date, datetime

from typing import List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array

MODEL_VERSION = "RISK_V4.0"

MODEL_FEATURES = [
    "bureau_score_norm", "credit_util_ratio", "payment_ontime_pct",
    "balance_volatility", "velocity_ratio", "account_overdraft_cnt",
    "large_withdrawal_cnt", "high_risk_merchant_cnt", "tenure_months",
]

DRIVER_LABELS = [
    "CREDIT_UTILIZATION", "PAYMENT_BEHAVIOUR", "TRANSACTION_VELOCITY", "BUREAU_SCORE",
]

OUTPUT_COLUMNS = [
    "customer_id", "composite_risk_score", "risk_tier", "probability_of_default",
    "credit_risk_component", "behaviour_risk_component", "velocity_risk_component",
    "bureau_score_component", "payment_history_component", "primary_risk_driver",
    "secondary_risk_driver", "score_delta_30d", "watch_list_flag",
    "review_required_flag", "model_version", "effective_date", "load_ts",
]

def _top_two_drivers(components: List[Tuple[Column, str]]) -> Tuple[Column, Column]:
    """Faithful port of the SAS array/do-loop that selects the top two drivers.

    Implemented as a pure column-expression fold (no Python UDF) so it runs
    natively on the executors and needs no module import on the workers. The
    strict ``>`` comparisons preserve the SAS tie-break: on equal values the
    earlier index keeps priority.
    """
    max1: Column = F.lit(0.0)
    max2: Column = F.lit(0.0)
    primary: Column = F.lit("")
    secondary: Column = F.lit("")
    for value, label in components:
        val = value.cast("double")
        gt1 = val > max1
        gt2 = (~gt1) & (val > max2)
        new_max1 = F.when(gt1, val).otherwise(max1)
        new_primary = F.when(gt1, F.lit(label)).otherwise(primary)
        new_max2 = F.when(gt1, max1).when(gt2, val).otherwise(max2)
        new_secondary = F.when(gt1, primary).when(gt2, F.lit(label)).otherwise(secondary)
        max1, primary, max2, secondary = new_max1, new_primary, new_max2, new_secondary
    return primary, secondary


def _clip(col: "Column") -> "Column":
    return F.least(F.lit(100.0), F.greatest(F.lit(0.0), col))


def prepare_features(stg_risk_factors: DataFrame, stg_customer_360: DataFrame) -> DataFrame:
    joined = stg_risk_factors.join(
        stg_customer_360.select(
            "customer_id", "tenure_months", "num_active_accounts",
            "total_balance", "customer_status",
        ),
        "customer_id",
        "inner",
    ).where(F.col("customer_status") == "A")

    ext_score = F.when(
        (F.col("external_credit_score") <= 0) | F.col("external_credit_score").isNull(), F.lit(680)
    ).otherwise(F.col("external_credit_score"))

    bureau_norm = (ext_score - 300) / (850 - 300) * 100
    balance_trend = F.when(
        F.col("avg_daily_balance_90d") > 0,
        F.col("avg_daily_balance_30d") / F.col("avg_daily_balance_90d"),
    ).otherwise(F.lit(1))
    velocity_ratio = F.when(
        F.col("debit_velocity_30d") > 0,
        (F.col("debit_velocity_7d") * (30.0 / 7.0)) / F.col("debit_velocity_30d"),
    ).otherwise(F.lit(1))

    return joined.select(
        "customer_id",
        ext_score.cast("double").alias("external_credit_score"),
        bureau_norm.cast("double").alias("bureau_score_norm"),
        balance_trend.cast("double").alias("balance_trend_ratio"),
        velocity_ratio.cast("double").alias("velocity_ratio"),
        F.col("credit_util_ratio").cast("double").alias("credit_util_ratio"),
        F.col("payment_ontime_pct").cast("double").alias("payment_ontime_pct"),
        F.col("balance_volatility").cast("double").alias("balance_volatility"),
        F.col("account_overdraft_cnt").cast("double").alias("account_overdraft_cnt"),
        F.col("large_withdrawal_cnt").cast("double").alias("large_withdrawal_cnt"),
        F.col("high_risk_merchant_cnt").cast("double").alias("high_risk_merchant_cnt"),
        F.col("tenure_months").cast("double").alias("tenure_months"),
        (F.col("payment_late_cnt") > 2).cast("double").alias("default_flag"),
    )


def _score_probability(features: DataFrame) -> DataFrame:
    """Fit LogisticRegression in-sample and return PROB_DEFAULT per customer."""
    distinct_labels = [r[0] for r in features.select("default_flag").distinct().collect()]
    if len(set(distinct_labels)) < 2:
        base_rate = float(distinct_labels[0]) if distinct_labels else 0.0
        return features.select("customer_id", F.lit(base_rate).alias("prob_default"))

    assembler = VectorAssembler(inputCols=MODEL_FEATURES, outputCol="_features")
    assembled = assembler.transform(features)
    lr = LogisticRegression(
        featuresCol="_features", labelCol="default_flag", probabilityCol="_probability"
    )
    scored = lr.fit(assembled).transform(assembled)
    return scored.select(
        "customer_id",
        vector_to_array(F.col("_probability"))[1].alias("prob_default"),
    )


def build_customer_risk_scores(
    stg_risk_factors: DataFrame,
    stg_customer_360: DataFrame,
    run_date: date,
    load_ts: datetime,
) -> DataFrame:
    features = prepare_features(stg_risk_factors, stg_customer_360)
    prob = _score_probability(features)
    scored = features.join(prob, "customer_id", "left")

    credit_risk = _clip(100 - F.col("bureau_score_norm"))
    behaviour_risk = _clip(100 - F.col("payment_ontime_pct"))
    velocity_risk = _clip((F.col("velocity_ratio") - 1) * 50)
    bureau_component = _clip(F.col("bureau_score_norm"))
    payment_history = _clip(F.col("payment_ontime_pct"))

    composite = F.round(
        credit_risk * 0.30
        + behaviour_risk * 0.25
        + velocity_risk * 0.15
        + (100 - bureau_component) * 0.20
        + (100 - payment_history) * 0.10,
        2,
    )
    prob_default = F.round(F.coalesce(F.col("prob_default"), F.lit(0)), 6)

    tier = (
        F.when(F.col("composite_risk_score") < 20, "LOW")
        .when(F.col("composite_risk_score") < 40, "MODERATE")
        .when(F.col("composite_risk_score") < 60, "ELEVATED")
        .when(F.col("composite_risk_score") < 80, "HIGH")
        .otherwise("CRITICAL")
    )

    enriched = scored.select(
        "customer_id",
        "velocity_ratio",
        credit_risk.cast("decimal(5,2)").alias("credit_risk_component"),
        behaviour_risk.cast("decimal(5,2)").alias("behaviour_risk_component"),
        velocity_risk.cast("decimal(5,2)").alias("velocity_risk_component"),
        bureau_component.cast("decimal(5,2)").alias("bureau_score_component"),
        payment_history.cast("decimal(5,2)").alias("payment_history_component"),
        composite.cast("decimal(6,2)").alias("composite_risk_score"),
        prob_default.cast("decimal(7,6)").alias("probability_of_default"),
    ).withColumn("risk_tier", tier)

    primary_driver, secondary_driver = _top_two_drivers([
        (F.col("credit_risk_component"), DRIVER_LABELS[0]),
        (F.col("behaviour_risk_component"), DRIVER_LABELS[1]),
        (F.col("velocity_risk_component"), DRIVER_LABELS[2]),
        (100 - F.col("bureau_score_component"), DRIVER_LABELS[3]),
    ])

    watch_list = F.when(
        (F.col("risk_tier") == "CRITICAL") & (F.col("probability_of_default") > 0.5), "Y"
    ).otherwise("N")
    review_required = F.when(
        (F.col("composite_risk_score") >= 60) & (F.col("velocity_ratio") > 2.0), "Y"
    ).otherwise("N")

    return enriched.select(
        F.col("customer_id"),
        F.col("composite_risk_score"),
        F.col("risk_tier"),
        F.col("probability_of_default"),
        F.col("credit_risk_component"),
        F.col("behaviour_risk_component"),
        F.col("velocity_risk_component"),
        F.col("bureau_score_component"),
        F.col("payment_history_component"),
        primary_driver.alias("primary_risk_driver"),
        secondary_driver.alias("secondary_risk_driver"),
        F.lit(0).cast("decimal(6,2)").alias("score_delta_30d"),
        watch_list.alias("watch_list_flag"),
        review_required.alias("review_required_flag"),
        F.lit(MODEL_VERSION).alias("model_version"),
        F.lit(run_date).cast("date").alias("effective_date"),
        F.lit(load_ts).cast("timestamp").alias("load_ts"),
    )
