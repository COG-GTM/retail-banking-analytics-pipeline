"""03 - Risk scoring (PySpark port of ``03_sas_risk_scoring.sas``).

SAS -> PySpark mapping
    PROC SQL join/extract         -> DataFrame join
    Feature-prep data step        -> withColumn transforms
    PROC LOGISTIC (PD model)      -> pyspark.ml.classification.LogisticRegression
    Composite scoring data step   -> withColumn + a UDF for the top-2 driver loop

Reads  ETL_STAGING_DB.STG_RISK_FACTORS + ETL_STAGING_DB.STG_CUSTOMER_360 (status 'A')
Writes DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES
"""
from __future__ import annotations

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ..config import PipelineConfig
from ..logging_utils import PipelineAudit
from ..session import DataLayer, get_spark
from ..validation import enforce, validate_table

STEP = "03_RISK_SCORING"

# PROC LOGISTIC MODEL statement predictors (stepwise selection has no direct
# Spark equivalent; the port fits LogisticRegression on the full predictor set).
MODEL_PREDICTORS = [
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

DRIVER_LABELS = [
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
]

OUTPUT_COLUMNS = [
    "customer_id", "composite_risk_score", "risk_tier", "probability_of_default",
    "credit_risk_component", "behaviour_risk_component", "velocity_risk_component",
    "bureau_score_component", "payment_history_component",
    "primary_risk_driver", "secondary_risk_driver", "score_delta_30d",
    "watch_list_flag", "review_required_flag", "model_version",
    "effective_date", "load_ts",
]

_DRIVER_STRUCT = StructType([
    StructField("primary", StringType(), True),
    StructField("secondary", StringType(), True),
])


@F.udf(returnType=_DRIVER_STRUCT)
def _top_two_drivers(c0, c1, c2, c3):
    """Exact reproduction of the SAS top-two risk-driver loop."""
    comps = [c0, c1, c2, c3]
    max1 = 0.0
    max2 = 0.0
    primary = ""
    secondary = ""
    for value, label in zip(comps, DRIVER_LABELS):
        v = float(value) if value is not None else 0.0
        if v > max1:
            max2 = max1
            secondary = primary
            max1 = v
            primary = label
        elif v > max2:
            max2 = v
            secondary = label
    return (primary, secondary)


def _clamp(col: "F.Column") -> "F.Column":
    return F.greatest(F.lit(0.0), F.least(F.lit(100.0), col))


def extract_risk_raw(risk: DataFrame, cust360: DataFrame) -> DataFrame:
    """STEP 1: join risk factors with base customer attributes (status 'A')."""
    cust = cust360.where(F.col("customer_status") == "A").select(
        F.col("customer_id").alias("_c_customer_id"),
        F.col("tenure_months"),
        F.col("num_active_accounts"),
        F.col("total_balance"),
        F.col("customer_status"),
    )
    return risk.join(
        cust, risk["customer_id"] == cust["_c_customer_id"], "inner"
    ).drop("_c_customer_id")


def prepare_features(risk_raw: DataFrame, config: PipelineConfig) -> DataFrame:
    """STEP 2: imputation and derived ratios."""
    bureau = F.when(
        (F.col("external_credit_score") <= 0) | F.col("external_credit_score").isNull(),
        F.lit(config.default_bureau_score),
    ).otherwise(F.col("external_credit_score"))

    return (
        risk_raw.withColumn("external_credit_score", bureau)
        .withColumn("bureau_score_norm", (F.col("external_credit_score") - 300) / (850 - 300) * 100)
        .withColumn(
            "balance_trend_ratio",
            F.when(F.col("avg_daily_balance_90d") > 0,
                   F.col("avg_daily_balance_30d") / F.col("avg_daily_balance_90d")).otherwise(F.lit(1.0)),
        )
        .withColumn(
            "velocity_ratio",
            F.when(F.col("debit_velocity_30d") > 0,
                   (F.col("debit_velocity_7d") * (30.0 / 7.0)) / F.col("debit_velocity_30d")).otherwise(F.lit(1.0)),
        )
        .withColumn("default_flag", (F.col("payment_late_cnt") > 2).cast("int"))
    )


def score_probability_of_default(features: DataFrame, audit: PipelineAudit) -> DataFrame:
    """STEP 3: probability-of-default model (PROC LOGISTIC -> LogisticRegression)."""
    model_input = features
    for col in MODEL_PREDICTORS:
        model_input = model_input.withColumn(col + "_x", F.coalesce(F.col(col).cast("double"), F.lit(0.0)))
    feat_cols = [c + "_x" for c in MODEL_PREDICTORS]

    class_counts = model_input.groupBy("default_flag").count().collect()
    positive = sum(r["count"] for r in class_counts if r["default_flag"] == 1)
    total = sum(r["count"] for r in class_counts)

    if positive == 0 or positive == total:
        # Degenerate target: logistic regression cannot separate a single class.
        base_rate = float(positive) / float(total) if total else 0.0
        audit.log_step(step=STEP, status="WARNING",
                       msg=f"Single-class target (pos={positive}/{total}); using base rate {base_rate}")
        return features.withColumn("prob_default", F.lit(base_rate))

    assembler = VectorAssembler(inputCols=feat_cols, outputCol="_model_features")
    assembled = assembler.transform(model_input)
    lr = LogisticRegression(
        featuresCol="_model_features", labelCol="default_flag",
        predictionCol="_pred", probabilityCol="_probability",
        rawPredictionCol="_raw", regParam=0.0, standardization=True,
    )
    model = lr.fit(assembled)
    scored = model.transform(assembled)
    scored = scored.withColumn("prob_default", vector_to_array(F.col("_probability"))[1])
    keep = features.columns + ["prob_default"]
    return scored.select(*keep)


def classify(scored: DataFrame, config: PipelineConfig) -> DataFrame:
    """STEP 4: component scores, composite, tiers, drivers, and flags."""
    credit = _clamp(100 - F.col("bureau_score_norm"))
    behaviour = _clamp(100 - F.col("payment_ontime_pct"))
    velocity = _clamp((F.col("velocity_ratio") - 1) * 50)
    bureau_comp = _clamp(F.col("bureau_score_norm"))
    payment_hist = _clamp(F.col("payment_ontime_pct"))

    df = (
        scored
        .withColumn("credit_risk_component", credit)
        .withColumn("behaviour_risk_component", behaviour)
        .withColumn("velocity_risk_component", velocity)
        .withColumn("bureau_score_component", bureau_comp)
        .withColumn("payment_history_component", payment_hist)
    )

    composite = F.round(
        F.col("credit_risk_component") * 0.30
        + F.col("behaviour_risk_component") * 0.25
        + F.col("velocity_risk_component") * 0.15
        + (100 - F.col("bureau_score_component")) * 0.20
        + (100 - F.col("payment_history_component")) * 0.10,
        2,
    )
    df = df.withColumn("composite_risk_score", composite)
    df = df.withColumn(
        "probability_of_default", F.round(F.coalesce(F.col("prob_default"), F.lit(0.0)), 6)
    )

    tier = (
        F.when(F.col("composite_risk_score") < 20, "LOW")
        .when(F.col("composite_risk_score") < 40, "MODERATE")
        .when(F.col("composite_risk_score") < 60, "ELEVATED")
        .when(F.col("composite_risk_score") < 80, "HIGH")
        .otherwise("CRITICAL")
    )
    df = df.withColumn("risk_tier", tier)

    drivers = _top_two_drivers(
        F.col("credit_risk_component"),
        F.col("behaviour_risk_component"),
        F.col("velocity_risk_component"),
        (100 - F.col("bureau_score_component")),
    )
    df = df.withColumn("_drivers", drivers)
    df = (
        df.withColumn("primary_risk_driver", F.col("_drivers.primary"))
        .withColumn("secondary_risk_driver", F.col("_drivers.secondary"))
        .withColumn("score_delta_30d", F.lit(0.0))
        .withColumn(
            "watch_list_flag",
            F.when((F.col("risk_tier") == "CRITICAL") & (F.col("probability_of_default") > 0.5), "Y").otherwise("N"),
        )
        .withColumn(
            "review_required_flag",
            F.when((F.col("composite_risk_score") >= 60) & (F.col("velocity_ratio") > 2.0), "Y").otherwise("N"),
        )
        .withColumn("model_version", F.lit(config.risk_model_version))
        .withColumn("effective_date", F.lit(config.effective_date))
        .withColumn("load_ts", F.lit(config.run_ts))
    )
    return df


def build_risk_scores(risk: DataFrame, cust360: DataFrame, config: PipelineConfig,
                      audit: PipelineAudit) -> DataFrame:
    risk_raw = extract_risk_raw(risk, cust360)
    features = prepare_features(risk_raw, config)
    scored = score_probability_of_default(features, audit)
    classified = classify(scored, config)
    return classified.select(
        F.col("customer_id").cast("long").alias("customer_id"),
        *[c for c in OUTPUT_COLUMNS if c != "customer_id"],
    )


def run(config: PipelineConfig) -> str:
    audit = PipelineAudit(run_id=config.run_id)
    spark = get_spark(config)
    data = DataLayer(spark, config)

    audit.log_step(step=STEP, status="START", msg=f"Model version {config.risk_model_version}")
    risk = data.read_staging("stg_risk_factors")
    cust360 = data.read_staging("stg_customer_360")

    result_df = build_risk_scores(risk, cust360, config, audit).cache()
    row_count = result_df.count()
    audit.log_step(step=STEP, status="SUCCESS", msg="Risk scores computed", rowcount=row_count)

    validation = validate_table(
        result_df, table="CUSTOMER_RISK_SCORES",
        key_cols=["customer_id"],
        not_null=["customer_id", "composite_risk_score", "risk_tier"],
        min_rows=config.min_rows, audit=audit,
    )
    enforce(validation, "CUSTOMER_RISK_SCORES", audit)

    audit.log_step(step=STEP, status="START", msg="Loading DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES")
    path = data.write_product(result_df, "customer_risk_scores")
    audit.log_step(step=STEP, status="SUCCESS", msg=f"Pipeline complete -> {path}", rowcount=row_count)
    result_df.unpersist()
    return path


if __name__ == "__main__":
    run(PipelineConfig.from_env())
