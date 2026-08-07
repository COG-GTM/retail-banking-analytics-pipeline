# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — `CUSTOMER_RISK_SCORES`
# MAGIC
# MAGIC Port of `sas/03_sas_risk_scoring.sas`.
# MAGIC
# MAGIC | SAS step | PySpark equivalent |
# MAGIC |---|---|
# MAGIC | `PROC SQL` join of `STG_RISK_FACTORS` + `STG_CUSTOMER_360` (`CUSTOMER_STATUS='A'`) | `prepare_features()` |
# MAGIC | `DATA` step imputation / ratios | `prepare_features()` |
# MAGIC | `PROC LOGISTIC ... selection=stepwise` | `pyspark.ml.classification.LogisticRegression` on all nine predictors |
# MAGIC | `DATA` step components, tiers, drivers | `score_risk()` |
# MAGIC | `DELETE` + `PROC APPEND` | `write.mode("overwrite")` |
# MAGIC
# MAGIC **Model selection.** `PROC LOGISTIC` uses stepwise selection
# MAGIC (`slentry=0.10`, `slstay=0.05`); Spark ML has no stepwise equivalent, so the
# MAGIC full nine-predictor model is fitted with `regParam=0` (unpenalised MLE, the
# MAGIC same estimator SAS uses) and `standardization=True`. This is the option the
# MAGIC DuckDB reference implementation also takes. `PROBABILITY_OF_DEFAULT` is
# MAGIC therefore close to, but not bit-identical with, the SAS output; every
# MAGIC downstream field except `WATCH_LIST_FLAG` is derived from the deterministic
# MAGIC component scores and is unaffected. Fitting statsmodels on a collected
# MAGIC DataFrame would give exact parity at the cost of single-node scoring — see
# MAGIC `databricks/README.md`.

# COMMAND ----------

import os
import sys
from pathlib import Path

for _p in [os.getcwd(), *[str(p) for p in Path(os.getcwd()).parents]]:
    if os.path.isdir(os.path.join(_p, "shared")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shared.audit import AUDIT_TABLE, AuditLogger
from shared.dq import validate_dataframe
from shared.runtime import (
    PipelineConfig,
    exit_if_skipped,
    get_int_param,
    get_param,
    in_databricks,
)
from shared.schemas import CUSTOMER_RISK_SCORES, conform

JOB_NAME = "03_risk_scoring"
TARGET_TABLE = "CUSTOMER_RISK_SCORES"
MODEL_VERSION = "RISK_V4.0"

PREDICTORS = [
    "BUREAU_SCORE_NORM",
    "CREDIT_UTIL_RATIO",
    "PAYMENT_ONTIME_PCT",
    "BALANCE_VOLATILITY",
    "VELOCITY_RATIO",
    "ACCOUNT_OVERDRAFT_CNT",
    "LARGE_WITHDRAWAL_CNT",
    "HIGH_RISK_MERCHANT_CNT",
    "TENURE_MONTHS",
]

DRIVER_LABELS = [
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
]

# COMMAND ----------


def prepare_features(risk_factors: DataFrame, cust_360: DataFrame) -> DataFrame:
    """SAS STEPS 1-2 — join to customer attributes, impute and derive ratios."""
    c = cust_360.where(F.col("CUSTOMER_STATUS") == "A").select(
        "CUSTOMER_ID", "TENURE_MONTHS", "NUM_ACTIVE_ACCOUNTS", "TOTAL_BALANCE", "CUSTOMER_STATUS"
    )
    joined = risk_factors.join(c, ["CUSTOMER_ID"], "inner")

    bureau_score = F.when(
        F.col("EXTERNAL_CREDIT_SCORE").isNull() | (F.col("EXTERNAL_CREDIT_SCORE") <= 0), F.lit(680)
    ).otherwise(F.col("EXTERNAL_CREDIT_SCORE"))

    return (
        joined.withColumn("EXTERNAL_CREDIT_SCORE", bureau_score)
        .withColumn(
            "BUREAU_SCORE_NORM",
            (F.col("EXTERNAL_CREDIT_SCORE") - 300) / (850 - 300) * 100,
        )
        .withColumn(
            "BALANCE_TREND_RATIO",
            F.when(
                F.col("AVG_DAILY_BALANCE_90D") > 0,
                F.col("AVG_DAILY_BALANCE_30D") / F.col("AVG_DAILY_BALANCE_90D"),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn(
            "VELOCITY_RATIO",
            F.when(
                F.col("DEBIT_VELOCITY_30D") > 0,
                (F.col("DEBIT_VELOCITY_7D") * (30.0 / 7.0)) / F.col("DEBIT_VELOCITY_30D"),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn("DEFAULT_FLAG", (F.col("PAYMENT_LATE_CNT") > 2).cast("int"))
    )


def predict_default(features: DataFrame) -> DataFrame:
    """SAS STEP 3 — logistic regression producing `PROB_DEFAULT`."""
    prepared = features
    for col in PREDICTORS:
        prepared = prepared.withColumn(
            f"_p_{col}", F.coalesce(F.col(col).cast("double"), F.lit(0.0))
        )

    assembled = VectorAssembler(
        inputCols=[f"_p_{c}" for c in PREDICTORS], outputCol="_features"
    ).transform(prepared)

    distinct_labels = assembled.select("DEFAULT_FLAG").distinct().count()
    if distinct_labels < 2:
        # Degenerate target: mirror the reference implementation's fallback.
        return assembled.withColumn(
            "PROB_DEFAULT", F.col("DEFAULT_FLAG") * 0.8 + 0.05
        ).drop("_features", *[f"_p_{c}" for c in PREDICTORS])

    model = LogisticRegression(
        featuresCol="_features",
        labelCol="DEFAULT_FLAG",
        predictionCol="_prediction",
        probabilityCol="_probability",
        rawPredictionCol="_raw_prediction",
        maxIter=200,
        regParam=0.0,
        standardization=True,
    ).fit(assembled)

    scored = model.transform(assembled).withColumn(
        "PROB_DEFAULT", vector_to_array(F.col("_probability"))[1]
    )
    return scored.drop(
        "_features",
        "_probability",
        "_raw_prediction",
        "_prediction",
        *[f"_p_{c}" for c in PREDICTORS],
    )


def _top_two_drivers(components: list[F.Column]) -> tuple[F.Column, F.Column]:
    """Unrolled form of the SAS `do i = 1 to 4` top-two driver loop."""
    max1 = F.lit(0.0)
    max2 = F.lit(0.0)
    primary = F.lit(None).cast("string")
    secondary = F.lit(None).cast("string")

    for component, label in zip(components, DRIVER_LABELS):
        next_max2 = F.when(component > max1, max1).when(component > max2, component).otherwise(max2)
        next_secondary = (
            F.when(component > max1, primary)
            .when(component > max2, F.lit(label))
            .otherwise(secondary)
        )
        next_max1 = F.when(component > max1, component).otherwise(max1)
        next_primary = F.when(component > max1, F.lit(label)).otherwise(primary)
        max1, max2, primary, secondary = next_max1, next_max2, next_primary, next_secondary

    return primary, secondary


def score_risk(scored: DataFrame) -> DataFrame:
    """SAS STEP 4 — components, composite score, tiers, drivers and flags."""

    def clamp(expr: F.Column) -> F.Column:
        return F.greatest(F.lit(0.0), F.least(F.lit(100.0), expr))

    with_components = (
        scored.withColumn("CREDIT_RISK_COMPONENT", clamp(100 - F.col("BUREAU_SCORE_NORM")))
        .withColumn("BEHAVIOUR_RISK_COMPONENT", clamp(100 - F.col("PAYMENT_ONTIME_PCT")))
        .withColumn("VELOCITY_RISK_COMPONENT", clamp((F.col("VELOCITY_RATIO") - 1) * 50))
        .withColumn("BUREAU_SCORE_COMPONENT", clamp(F.col("BUREAU_SCORE_NORM")))
        .withColumn("PAYMENT_HISTORY_COMPONENT", clamp(F.col("PAYMENT_ONTIME_PCT")))
    )

    composite = F.round(
        F.col("CREDIT_RISK_COMPONENT") * 0.30
        + F.col("BEHAVIOUR_RISK_COMPONENT") * 0.25
        + F.col("VELOCITY_RISK_COMPONENT") * 0.15
        + (100 - F.col("BUREAU_SCORE_COMPONENT")) * 0.20
        + (100 - F.col("PAYMENT_HISTORY_COMPONENT")) * 0.10,
        2,
    )

    classified = with_components.withColumn("COMPOSITE_RISK_SCORE", composite).withColumn(
        "PROBABILITY_OF_DEFAULT", F.round(F.coalesce(F.col("PROB_DEFAULT"), F.lit(0.0)), 6)
    )

    risk_tier = (
        F.when(F.col("COMPOSITE_RISK_SCORE") < 20, "LOW")
        .when(F.col("COMPOSITE_RISK_SCORE") < 40, "MODERATE")
        .when(F.col("COMPOSITE_RISK_SCORE") < 60, "ELEVATED")
        .when(F.col("COMPOSITE_RISK_SCORE") < 80, "HIGH")
        .otherwise("CRITICAL")
    )
    classified = classified.withColumn("RISK_TIER", risk_tier)

    primary, secondary = _top_two_drivers(
        [
            F.col("CREDIT_RISK_COMPONENT"),
            F.col("BEHAVIOUR_RISK_COMPONENT"),
            F.col("VELOCITY_RISK_COMPONENT"),
            100 - F.col("BUREAU_SCORE_COMPONENT"),
        ]
    )

    result = classified.select(
        F.col("CUSTOMER_ID"),
        F.col("COMPOSITE_RISK_SCORE"),
        F.col("RISK_TIER"),
        F.col("PROBABILITY_OF_DEFAULT"),
        F.col("CREDIT_RISK_COMPONENT"),
        F.col("BEHAVIOUR_RISK_COMPONENT"),
        F.col("VELOCITY_RISK_COMPONENT"),
        F.col("BUREAU_SCORE_COMPONENT"),
        F.col("PAYMENT_HISTORY_COMPONENT"),
        primary.alias("PRIMARY_RISK_DRIVER"),
        secondary.alias("SECONDARY_RISK_DRIVER"),
        F.lit(0).alias("SCORE_DELTA_30D"),
        F.when(
            (F.col("RISK_TIER") == "CRITICAL") & (F.col("PROBABILITY_OF_DEFAULT") > 0.5), "Y"
        ).otherwise("N").alias("WATCH_LIST_FLAG"),
        F.when(
            (F.col("COMPOSITE_RISK_SCORE") >= 60) & (F.col("VELOCITY_RATIO") > 2.0), "Y"
        ).otherwise("N").alias("REVIEW_REQUIRED_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.current_date().alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return conform(result, CUSTOMER_RISK_SCORES)


def build_customer_risk_scores(risk_factors: DataFrame, cust_360: DataFrame) -> DataFrame:
    return score_risk(predict_default(prepare_features(risk_factors, cust_360)))


# COMMAND ----------

if in_databricks() and not exit_if_skipped("skip_gold", JOB_NAME):
    cfg = PipelineConfig.from_widgets()
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))
    target = cfg.gold(TARGET_TABLE)

    with audit.step("RISK_SCORING", f"-> {target}") as ctx:
        df = build_customer_risk_scores(
            spark.table(cfg.silver("STG_RISK_FACTORS")),
            spark.table(cfg.silver("STG_CUSTOMER_360")),
        )
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)
        ctx.row_count = validate_dataframe(
            spark.table(target),
            target,
            key_cols=["CUSTOMER_ID"],
            not_null=["CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER"],
            min_rows=get_int_param("min_gold_rows", "1000"),
        )

    # Risk tier distribution for monitoring (replaces PROC FREQ / PROC PRINT).
    spark.table(target).groupBy("RISK_TIER").agg(
        F.count(F.lit(1)).alias("N"),
        F.round(F.avg("COMPOSITE_RISK_SCORE"), 2).alias("AVG_SCORE"),
    ).orderBy(F.col("AVG_SCORE").desc()).show(truncate=False)
