"""Customer risk scoring job (port of ``sas/03_sas_risk_scoring.sas``).

Reads Delta ``etl_staging.stg_risk_factors`` + ``etl_staging.stg_customer_360``,
trains a Spark MLlib :class:`LogisticRegression` for probability of default
(replacing SAS ``PROC LOGISTIC`` stepwise), builds a weighted composite risk
score with a 5-level tier classification, and writes Delta
``data_products.customer_risk_scores`` (idempotent overwrite).

Legacy construct -> PySpark mapping
-----------------------------------
* ``PROC LOGISTIC ... selection=stepwise``  -> ``LogisticRegression`` over a
  ``VectorAssembler`` feature vector; ``PROB_DEFAULT`` = P(default = 1).
* SAS ``DATA`` step composite score / tiering -> Spark SQL column expressions.
* ``%log_step`` / ``ETL_RUN_LOG``           -> :func:`common.audit.log_step`.
* ``%validate_table`` / ``%ABORT CANCEL``   -> :func:`common.validation.validate_table`.
* ``PROC APPEND`` after ``DELETE``          -> idempotent Delta ``overwrite``.
"""

from __future__ import annotations

import uuid

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common.audit import init_audit, log_step
from common.validation import validate_table

JOB_NAME = "03_risk_scoring"
MODEL_VERSION = "RISK_V4.0"

# LogisticRegression (LBFGS) is deterministic; fixed hyper-parameters + this seed
# keep every run reproducible (Providence rule R4).
RANDOM_SEED = 42

# Numeric predictors fed to the logistic model (SAS PROC LOGISTIC MODEL list).
_FEATURE_COLS = [
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

# Component -> human-readable driver label (SAS _lbl array).
_DRIVER_LABELS = [
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
]

_DRIVER_SCHEMA = StructType(
    [
        StructField("primary", StringType(), True),
        StructField("secondary", StringType(), True),
    ]
)


def _rank_drivers(credit: float, behaviour: float, velocity: float, bureau: float):
    """Return (primary, secondary) drivers, mirroring the SAS top-two loop."""
    comps = [
        (credit, _DRIVER_LABELS[0]),
        (behaviour, _DRIVER_LABELS[1]),
        (velocity, _DRIVER_LABELS[2]),
        (bureau, _DRIVER_LABELS[3]),
    ]
    max1 = max2 = 0.0
    primary = secondary = ""
    for value, label in comps:
        value = float(value or 0.0)
        if value > max1:
            max2 = max1
            secondary = primary
            max1 = value
            primary = label
        elif value > max2:
            max2 = value
            secondary = label
    return (primary, secondary)


def _prepare_features(spark: SparkSession, cfg) -> DataFrame:
    """STEP 1-2: join staging, filter active customers, derive model features."""
    risk = spark.table(cfg.table(cfg.schema_stg, "stg_risk_factors"))
    cust = spark.table(cfg.table(cfg.schema_stg, "stg_customer_360"))

    joined = risk.alias("r").join(
        cust.alias("c").select(
            "customer_id",
            "tenure_months",
            "num_active_accounts",
            "total_balance",
            "customer_status",
        ),
        on="customer_id",
        how="inner",
    ).where(F.col("c.customer_status") == F.lit("A"))

    ecs = F.col("external_credit_score")
    imputed_ecs = F.when((ecs <= 0) | ecs.isNull(), F.lit(680.0)).otherwise(ecs)

    return (
        joined
        .withColumn("external_credit_score", imputed_ecs)
        .withColumn(
            "bureau_score_norm",
            (F.col("external_credit_score") - F.lit(300.0)) / F.lit(550.0) * F.lit(100.0),
        )
        .withColumn(
            "velocity_ratio",
            F.when(
                F.col("debit_velocity_30d") > 0,
                (F.col("debit_velocity_7d") * F.lit(30.0 / 7.0)) / F.col("debit_velocity_30d"),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn("default_flag", (F.col("payment_late_cnt") > F.lit(2)).cast("double"))
    )


def _score_probability_of_default(features: DataFrame) -> DataFrame:
    """STEP 3: logistic regression P(default); robust to single-class training."""
    assembler = VectorAssembler(
        inputCols=_FEATURE_COLS, outputCol="features", handleInvalid="keep"
    )
    assembled = assembler.transform(features)

    distinct_labels = [r[0] for r in assembled.select("default_flag").distinct().collect()]
    if len(distinct_labels) < 2:
        # PROC LOGISTIC cannot separate a single-class target; fall back to the
        # empirical positive rate (deterministic) rather than failing the run.
        positive_rate = float(distinct_labels[0]) if distinct_labels else 0.0
        return assembled.withColumn("prob_default", F.lit(positive_rate))

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="default_flag",
        maxIter=100,
        regParam=0.0,
        elasticNetParam=0.0,
        standardization=True,
        tol=1e-6,
    )
    model = lr.fit(assembled)
    scored = model.transform(assembled)
    extract_p1 = F.udf(lambda v: float(v[1]) if v is not None else 0.0, "double")
    return scored.withColumn("prob_default", extract_p1(F.col("probability")))


def _classify(scored: DataFrame, cfg) -> DataFrame:
    """STEP 4: composite score, tiers, drivers, watch-list / review flags."""
    clamp = lambda c: F.least(F.lit(100.0), F.greatest(F.lit(0.0), c))  # noqa: E731

    credit_risk = clamp(F.lit(100.0) - F.col("bureau_score_norm"))
    behaviour_risk = clamp(F.lit(100.0) - F.col("payment_ontime_pct"))
    velocity_risk = clamp((F.col("velocity_ratio") - F.lit(1.0)) * F.lit(50.0))
    bureau_component = clamp(F.col("bureau_score_norm"))
    payment_history = clamp(F.col("payment_ontime_pct"))

    df = (
        scored
        .withColumn("credit_risk_component", credit_risk)
        .withColumn("behaviour_risk_component", behaviour_risk)
        .withColumn("velocity_risk_component", velocity_risk)
        .withColumn("bureau_score_component", bureau_component)
        .withColumn("payment_history_component", payment_history)
    )

    composite = F.round(
        F.col("credit_risk_component") * F.lit(0.30)
        + F.col("behaviour_risk_component") * F.lit(0.25)
        + F.col("velocity_risk_component") * F.lit(0.15)
        + (F.lit(100.0) - F.col("bureau_score_component")) * F.lit(0.20)
        + (F.lit(100.0) - F.col("payment_history_component")) * F.lit(0.10),
        2,
    )

    df = df.withColumn("composite_risk_score", composite).withColumn(
        "probability_of_default", F.round(F.coalesce(F.col("prob_default"), F.lit(0.0)), 6)
    )

    df = df.withColumn(
        "risk_tier",
        F.when(F.col("composite_risk_score") < 20, F.lit("LOW"))
        .when(F.col("composite_risk_score") < 40, F.lit("MODERATE"))
        .when(F.col("composite_risk_score") < 60, F.lit("ELEVATED"))
        .when(F.col("composite_risk_score") < 80, F.lit("HIGH"))
        .otherwise(F.lit("CRITICAL")),
    )

    driver_udf = F.udf(_rank_drivers, _DRIVER_SCHEMA)
    df = df.withColumn(
        "_drivers",
        driver_udf(
            F.col("credit_risk_component"),
            F.col("behaviour_risk_component"),
            F.col("velocity_risk_component"),
            F.lit(100.0) - F.col("bureau_score_component"),
        ),
    )

    threshold = F.lit(cfg.risk_score_threshold)
    return (
        df
        .withColumn("primary_risk_driver", F.col("_drivers.primary"))
        .withColumn("secondary_risk_driver", F.col("_drivers.secondary"))
        .withColumn("score_delta_30d", F.lit(0.0))
        .withColumn(
            "watch_list_flag",
            F.when(
                (F.col("risk_tier") == F.lit("CRITICAL"))
                & (
                    (F.col("probability_of_default") > F.lit(0.5))
                    | (F.col("external_credit_score") < threshold)
                ),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn(
            "review_required_flag",
            F.when(
                (F.col("composite_risk_score") >= F.lit(60))
                & (F.col("velocity_ratio") > F.lit(2.0)),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("effective_date", F.to_date(F.lit(cfg.run_date)))
        .withColumn("load_ts", F.current_timestamp())
    )


def _select_output(df: DataFrame) -> DataFrame:
    """Cast to the CUSTOMER_RISK_SCORES DDL contract column list / types."""
    return df.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("composite_risk_score").cast("decimal(6,2)").alias("composite_risk_score"),
        F.col("risk_tier").cast("string").alias("risk_tier"),
        F.col("probability_of_default").cast("decimal(7,6)").alias("probability_of_default"),
        F.col("credit_risk_component").cast("decimal(5,2)").alias("credit_risk_component"),
        F.col("behaviour_risk_component").cast("decimal(5,2)").alias("behaviour_risk_component"),
        F.col("velocity_risk_component").cast("decimal(5,2)").alias("velocity_risk_component"),
        F.col("bureau_score_component").cast("decimal(5,2)").alias("bureau_score_component"),
        F.col("payment_history_component").cast("decimal(5,2)").alias("payment_history_component"),
        F.col("primary_risk_driver").cast("string").alias("primary_risk_driver"),
        F.col("secondary_risk_driver").cast("string").alias("secondary_risk_driver"),
        F.col("score_delta_30d").cast("decimal(6,2)").alias("score_delta_30d"),
        F.col("watch_list_flag").cast("string").alias("watch_list_flag"),
        F.col("review_required_flag").cast("string").alias("review_required_flag"),
        F.col("model_version").cast("string").alias("model_version"),
        F.col("effective_date").cast("date").alias("effective_date"),
        F.col("load_ts").cast("timestamp").alias("load_ts"),
    )


def run(spark: SparkSession, cfg) -> DataFrame:
    """Execute the risk-scoring job and return the written DataFrame."""
    run_id = str(uuid.uuid4())
    init_audit(spark, cfg)
    log_step(spark, cfg, run_id, JOB_NAME, "extract", "START",
             message=f"model_version={MODEL_VERSION}")

    features = _prepare_features(spark, cfg)
    scored = _score_probability_of_default(features)
    classified = _classify(scored, cfg)
    output = _select_output(classified)

    validate_table(
        output,
        min_rows=1,
        not_null_cols=["customer_id", "composite_risk_score", "risk_tier"],
        unique_keys=["customer_id"],
    )

    target = cfg.table(cfg.schema_dp, "customer_risk_scores")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_dp}")
    output.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target)

    result = spark.table(target)
    log_step(spark, cfg, run_id, JOB_NAME, "load", "SUCCESS",
             row_count=result.count(), message=f"wrote {target}")
    return result
