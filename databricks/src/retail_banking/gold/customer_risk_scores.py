from __future__ import annotations

from datetime import date

import pyspark.sql.functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array

FEATURE_COLS = ["bureau_score_norm", "credit_util_ratio", "payment_ontime_pct",
                "balance_volatility", "velocity_ratio", "account_overdraft_cnt",
                "large_withdrawal_cnt", "high_risk_merchant_cnt",
                "tenure_months"]
DRIVERS = [("credit_risk_component", "CREDIT_UTILIZATION"),
           ("behaviour_risk_component", "PAYMENT_BEHAVIOUR"),
           ("velocity_risk_component", "TRANSACTION_VELOCITY"),
           ("_inv_bureau", "BUREAU_SCORE")]
OUT_COLS = ["customer_id", "composite_risk_score", "risk_tier",
            "probability_of_default", "credit_risk_component",
            "behaviour_risk_component", "velocity_risk_component",
            "bureau_score_component", "payment_history_component",
            "primary_risk_driver", "secondary_risk_driver",
            "score_delta_30d", "watch_list_flag", "review_required_flag",
            "model_version", "effective_date", "load_ts"]


def build_customer_risk_scores(stg_risk_factors, stg_customer_360,
                               run_date: date, seed: int = 42):
    """Port of sas/03_sas_risk_scoring.sas (phase3c)."""
    df = (stg_risk_factors.join(
        stg_customer_360.filter(F.col("customer_status") == "A")
        .select("customer_id", "tenure_months", "num_active_accounts",
                "total_balance", "customer_status"),
        "customer_id"))

    df = df.withColumn("external_credit_score", F.when(
        F.coalesce(F.col("external_credit_score"), F.lit(0)) == 0, 680)
        .otherwise(F.col("external_credit_score")))
    df = df.withColumn("bureau_score_norm",
                       (F.col("external_credit_score") - 300) / 550 * 100)
    df = df.withColumn("balance_trend_ratio", F.when(
        F.col("avg_daily_balance_90d") > 0,
        F.col("avg_daily_balance_30d") / F.col("avg_daily_balance_90d"))
        .otherwise(F.lit(1.0)))
    df = df.withColumn("velocity_ratio", F.when(
        F.col("debit_velocity_30d") > 0,
        F.col("debit_velocity_7d") * (30.0 / 7) / F.col("debit_velocity_30d"))
        .otherwise(F.lit(1.0)))
    df = df.withColumn("default_flag",
                       (F.col("payment_late_cnt") > 2).cast("int"))

    for c in FEATURE_COLS:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)).cast("double"))

    n_classes = df.select("default_flag").distinct().count()
    if n_classes >= 2:
        assembler = VectorAssembler(inputCols=FEATURE_COLS,
                                    outputCol="_feat_raw",
                                    handleInvalid="keep")
        scaler = StandardScaler(inputCol="_feat_raw", outputCol="features",
                                withMean=True, withStd=True)
        assembled = assembler.transform(df)
        scaler_model = scaler.fit(assembled)
        scaled = scaler_model.transform(assembled)
        lr = LogisticRegression(maxIter=200, featuresCol="features",
                                labelCol="default_flag",
                                probabilityCol="_prob")
        model = lr.fit(scaled)
        df = model.transform(scaled).withColumn(
            "prob_default", vector_to_array("_prob")[1])
    else:
        df = df.withColumn("prob_default",
                           F.col("default_flag") * 0.8 + 0.05)

    clip100 = lambda c: F.greatest(F.lit(0.0), F.least(c, F.lit(100.0)))
    df = df.withColumn("credit_risk_component",
                       clip100(100 - F.col("bureau_score_norm")))
    df = df.withColumn("behaviour_risk_component",
                       clip100(100 - F.col("payment_ontime_pct")))
    df = df.withColumn("velocity_risk_component",
                       clip100((F.col("velocity_ratio") - 1) * 50))
    df = df.withColumn("bureau_score_component",
                       clip100(F.col("bureau_score_norm")))
    df = df.withColumn("payment_history_component",
                       clip100(F.col("payment_ontime_pct")))
    df = df.withColumn("_inv_bureau", 100 - F.col("bureau_score_component"))

    df = df.withColumn("composite_risk_score", F.round(
        F.col("credit_risk_component") * 0.30
        + F.col("behaviour_risk_component") * 0.25
        + F.col("velocity_risk_component") * 0.15
        + (100 - F.col("bureau_score_component")) * 0.20
        + (100 - F.col("payment_history_component")) * 0.10, 2))
    df = df.withColumn("probability_of_default",
                       F.round(F.col("prob_default"), 6))

    df = df.withColumn("risk_tier", F.when(
        F.col("composite_risk_score") <= 20, "LOW")
        .when(F.col("composite_risk_score") <= 40, "MODERATE")
        .when(F.col("composite_risk_score") <= 60, "ELEVATED")
        .when(F.col("composite_risk_score") <= 80, "HIGH")
        .otherwise("CRITICAL"))

    # Top-2 drivers, ties broken in DRIVERS list order (np.argsort desc on a
    # stable-ordered value list equivalent)
    arr = F.sort_array(F.array(*[
        F.struct((-F.col(c)).alias("v"),
                 F.lit(i).alias("ord"),
                 F.lit(label).alias("label"))
        for i, (c, label) in enumerate(DRIVERS)]))
    df = df.withColumn("primary_risk_driver", arr[0]["label"])
    df = df.withColumn("secondary_risk_driver", arr[1]["label"])

    df = df.withColumn("score_delta_30d", F.lit(0.0).cast("decimal(6,2)"))
    df = df.withColumn("watch_list_flag", F.when(
        (F.col("risk_tier") == "CRITICAL")
        & (F.col("probability_of_default") > 0.5), "Y").otherwise("N"))
    df = df.withColumn("review_required_flag", F.when(
        (F.col("composite_risk_score") >= 60)
        & (F.col("velocity_ratio") > 2.0), "Y").otherwise("N"))
    df = df.withColumn("model_version", F.lit("RISK_V4.0"))
    df = df.withColumn("effective_date", F.lit(run_date))
    df = df.withColumn("load_ts", F.current_timestamp())

    return df.select(
        "customer_id",
        F.col("composite_risk_score").cast("decimal(6,2)")
            .alias("composite_risk_score"),
        "risk_tier",
        F.col("probability_of_default").cast("decimal(7,6)")
            .alias("probability_of_default"),
        *[F.col(c).cast("decimal(5,2)").alias(c) for c in
          ["credit_risk_component", "behaviour_risk_component",
           "velocity_risk_component", "bureau_score_component",
           "payment_history_component"]],
        "primary_risk_driver", "secondary_risk_driver", "score_delta_30d",
        "watch_list_flag", "review_required_flag", "model_version",
        "effective_date", "load_ts")
