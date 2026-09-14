import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StructField, StructType
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from ..audit import assert_rows, step
from ..config import RunConfig
from ..dq import validate_table
from ..ml import log_sklearn_model, mlflow_run
from ..tables import write_overwrite

MODEL_VERSION = "RISK_V4.0"
_MODEL_FEATURES = [
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


def _model_scores(source: DataFrame):
    data = source.select("customer_id", *_MODEL_FEATURES, "payment_late_cnt").toPandas()
    features = data[_MODEL_FEATURES].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0)
    target = (data["payment_late_cnt"].astype(float) > 2).astype(int)
    scaler = StandardScaler()
    standardized = scaler.fit_transform(features)
    if target.nunique() < 2:
        return (
            data[["customer_id"]].assign(probability_of_default=0.05),
            None,
            True,
        )
    model = LogisticRegression(max_iter=1000)
    model.fit(standardized, target)
    probabilities = model.predict_proba(standardized)[:, 1]
    return (
        data[["customer_id"]].assign(probability_of_default=probabilities),
        model,
        False,
    )


def _risk_features(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    risk = spark.table(cfg.fqn(cfg.silver_schema, "stg_risk_factors"))
    customers = spark.table(cfg.fqn(cfg.silver_schema, "stg_customer_360")).where(
        F.col("customer_status") == "A"
    )
    joined = risk.join(
        customers.select(
            "customer_id",
            "tenure_months",
            "num_active_accounts",
            "total_balance",
        ),
        "customer_id",
    )
    bureau_score = F.when(
        F.col("external_credit_score").isNull() | (F.col("external_credit_score") <= 0),
        680,
    ).otherwise(F.col("external_credit_score"))
    bureau_norm = (bureau_score - 300) / 550 * 100
    velocity_ratio = F.when(
        F.col("debit_velocity_30d") > 0,
        F.col("debit_velocity_7d") * (30 / 7) / F.col("debit_velocity_30d"),
    ).otherwise(1.0)
    return joined.select(
        "*",
        bureau_norm.alias("bureau_score_norm"),
        velocity_ratio.alias("velocity_ratio"),
    )


def _build_result(spark: SparkSession, cfg: RunConfig):
    source = _risk_features(spark, cfg)
    score_data, model, fallback = _model_scores(source)
    schema = StructType(
        [
            StructField("customer_id", LongType(), False),
            StructField("probability_of_default", DoubleType(), False),
        ]
    )
    probabilities = spark.createDataFrame(score_data.itertuples(index=False, name=None), schema)
    scored = source.join(probabilities, "customer_id")
    credit = F.greatest(
        F.lit(0.0), F.least(F.lit(100.0) - F.col("bureau_score_norm"), F.lit(100.0))
    )
    behavior = F.greatest(
        F.lit(0.0), F.least(F.lit(100.0) - F.col("payment_ontime_pct"), F.lit(100.0))
    )
    velocity = F.greatest(
        F.lit(0.0),
        F.least((F.col("velocity_ratio") - 1) * 50, F.lit(100.0)),
    )
    bureau = F.greatest(F.lit(0.0), F.least(F.col("bureau_score_norm"), F.lit(100.0)))
    payment = F.greatest(F.lit(0.0), F.least(F.col("payment_ontime_pct"), F.lit(100.0)))
    scored = (
        scored.withColumn("credit_risk_component", credit)
        .withColumn("behaviour_risk_component", behavior)
        .withColumn("velocity_risk_component", velocity)
        .withColumn("bureau_score_component", bureau)
        .withColumn("payment_history_component", payment)
        .withColumn(
            "composite_risk_score",
            F.round(
                credit * 0.30
                + behavior * 0.25
                + velocity * 0.15
                + (100 - bureau) * 0.20
                + (100 - payment) * 0.10,
                2,
            ),
        )
    )
    drivers = [
        ("BUREAU_SCORE", 100 - bureau),
        ("TRANSACTION_VELOCITY", velocity),
        ("CREDIT_UTILIZATION", credit),
        ("PAYMENT_BEHAVIOUR", behavior),
    ]
    driver_structs = F.array(
        *[
            F.struct(F.lit(score).alias("label"), expression.alias("score"))
            for score, expression in drivers
        ]
    )
    scored = scored.withColumn(
        "_drivers",
        F.array_sort(
            driver_structs,
            lambda left, right: (
                F.when(left["score"] > right["score"], -1)
                .when(left["score"] < right["score"], 1)
                .otherwise(0)
            ),
        ),
    )
    scored = scored.withColumn("primary_risk_driver", F.col("_drivers")[0]["label"]).withColumn(
        "secondary_risk_driver", F.col("_drivers")[1]["label"]
    )
    result = scored.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("composite_risk_score").cast("decimal(6,2)").alias("composite_risk_score"),
        F.when(F.col("composite_risk_score") < 20, "LOW")
        .when(F.col("composite_risk_score") < 40, "MODERATE")
        .when(F.col("composite_risk_score") < 60, "ELEVATED")
        .when(F.col("composite_risk_score") < 80, "HIGH")
        .otherwise("CRITICAL")
        .alias("risk_tier"),
        F.round(F.col("probability_of_default"), 6)
        .cast("decimal(7,6)")
        .alias("probability_of_default"),
        F.col("credit_risk_component").cast("decimal(5,2)").alias("credit_risk_component"),
        F.col("behaviour_risk_component").cast("decimal(5,2)").alias("behaviour_risk_component"),
        F.col("velocity_risk_component").cast("decimal(5,2)").alias("velocity_risk_component"),
        F.col("bureau_score_component").cast("decimal(5,2)").alias("bureau_score_component"),
        F.col("payment_history_component").cast("decimal(5,2)").alias("payment_history_component"),
        "primary_risk_driver",
        "secondary_risk_driver",
        F.lit(0).cast("decimal(6,2)").alias("score_delta_30d"),
        F.when(
            (F.col("composite_risk_score") >= 80) & (F.col("probability_of_default") > 0.5),
            "Y",
        )
        .otherwise("N")
        .alias("watch_list_flag"),
        F.when(
            (F.col("composite_risk_score") >= 60) & (F.col("velocity_ratio") > 2.0),
            "Y",
        )
        .otherwise("N")
        .alias("review_required_flag"),
        F.lit(MODEL_VERSION).alias("model_version"),
        F.lit(cfg.run_date).cast("date").alias("effective_date"),
        F.current_timestamp().alias("load_ts"),
    )
    return result, model, fallback


def build(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    return _build_result(spark, cfg)[0]


def run(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    fqn = cfg.fqn(cfg.gold_schema, "customer_risk_scores")
    with step(spark, cfg, "03_customer_risk_scores", "FULL_LOAD") as state:
        result, model, fallback = _build_result(spark, cfg)
        state["row_count"] = assert_rows(result, "customer_risk_scores")
        with mlflow_run(cfg, "customer_risk_scores") as active_run:
            if active_run is not None:
                params = {"model_version": MODEL_VERSION, "max_iter": 1000}
                if fallback:
                    params["fallback_constant_pd"] = 0.05
                if model is not None:
                    log_sklearn_model(model, "customer_risk_logistic_regression", params, {})
                else:
                    import mlflow

                    mlflow.log_params(params)
        write_overwrite(result, fqn)
    validate_table(
        spark,
        fqn,
        ["customer_id"],
        ["customer_id", "composite_risk_score", "risk_tier"],
        cfg.dq_min_rows,
    )
    return result
