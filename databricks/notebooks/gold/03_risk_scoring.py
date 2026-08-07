# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — `CUSTOMER_RISK_SCORES`
# MAGIC
# MAGIC Port of `sas/03_sas_risk_scoring.sas` (model version `RISK_V4.0`).
# MAGIC
# MAGIC | SAS step | Databricks equivalent |
# MAGIC |---|---|
# MAGIC | `proc sql` join of `STG_RISK_FACTORS` + `STG_CUSTOMER_360` | `build_risk_features` |
# MAGIC | `data WORK.RISK_FEATURES` imputation and ratios | same expressions in Spark SQL |
# MAGIC | `proc logistic ... selection=stepwise slentry=0.10 slstay=0.05` | `shared.modeling.stepwise_logistic` (`pyspark.ml.LogisticRegression` + Wald p-value selection) |
# MAGIC | `data WORK.RISK_CLASSIFIED` composite score, tiers, driver array | `classify` — formula, cutoffs and driver fold reproduced exactly |
# MAGIC | `proc sql delete` + `proc append force` | Delta overwrite/merge |
# MAGIC
# MAGIC The weighted composite, the `LOW<20 / MODERATE<40 / ELEVATED<60 / HIGH<80 /
# MAGIC CRITICAL` cutoffs and the top-two driver selection are byte-for-byte the
# MAGIC same logic as the SAS DATA step; only the estimator behind
# MAGIC `PROBABILITY_OF_DEFAULT` changes.
# MAGIC
# MAGIC `risk_score_threshold` mirrors `%let RISK_THRESHOLD = %sysget(RISK_SCORE_THRESHOLD)`:
# MAGIC the SAS program reads it and records it in the run log but no rule consumes
# MAGIC it, so this port keeps it as a logged job parameter rather than inventing a
# MAGIC cutoff the legacy scores never had.

# COMMAND ----------

from __future__ import annotations

import os
import sys


def _bootstrap() -> None:
    here = os.path.dirname(os.path.abspath(globals().get("__file__", os.path.join(os.getcwd(), "nb.py"))))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    if root not in sys.path:
        sys.path.insert(0, root)


_bootstrap()

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from shared import io, schemas  # noqa: E402
from shared.audit import ensure_run_log, step  # noqa: E402
from shared.config import PipelineConfig, exit_if_skipped  # noqa: E402
from shared.logging_utils import get_logger, log_event  # noqa: E402
from shared.modeling import predict_probability, stepwise_logistic  # noqa: E402
from shared.validation import validate_and_log  # noqa: E402

JOB_NAME = "03_risk_scoring"
TARGET_TABLE = "CUSTOMER_RISK_SCORES"
MODEL_VERSION = "RISK_V4.0"
LABEL_COL = "DEFAULT_FLAG"

# `model DEFAULT_FLAG = ...` candidate effects, in SAS declaration order.
MODEL_FEATURES = [
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

# `array _lbl[4]` — driver names in component order.
DRIVER_LABELS = [
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
]

# COMMAND ----------


def build_risk_features(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """`WORK.RISK_FEATURES`: imputed bureau score, ratios and default proxy."""
    return spark.sql(
        f"""
        WITH risk_raw AS (
            SELECT
                r.*,
                c.TENURE_MONTHS,
                c.NUM_ACTIVE_ACCOUNTS,
                c.TOTAL_BALANCE,
                c.CUSTOMER_STATUS
            FROM {cfg.silver('STG_RISK_FACTORS')} r
            INNER JOIN {cfg.silver('STG_CUSTOMER_360')} c
                ON r.CUSTOMER_ID = c.CUSTOMER_ID
            WHERE c.CUSTOMER_STATUS = 'A'
        ),
        imputed AS (
            SELECT
                *,
                /* Impute missing bureau scores with the population median proxy */
                CASE WHEN EXTERNAL_CREDIT_SCORE IS NULL OR EXTERNAL_CREDIT_SCORE <= 0
                     THEN 680 ELSE EXTERNAL_CREDIT_SCORE
                END AS BUREAU_SCORE_IMPUTED
            FROM risk_raw
        )
        SELECT
            *,
            /* Normalise bureau score to a 0-100 scale */
            (BUREAU_SCORE_IMPUTED - 300) / (850 - 300) * 100                    AS BUREAU_SCORE_NORM,
            /* Balance trend: ratio of 30D avg to 90D avg */
            CASE WHEN AVG_DAILY_BALANCE_90D > 0
                 THEN AVG_DAILY_BALANCE_30D / AVG_DAILY_BALANCE_90D
                 ELSE 1 END                                                     AS BALANCE_TREND_RATIO,
            /* Velocity ratio: 7-day annualised against 30-day debit velocity */
            CASE WHEN DEBIT_VELOCITY_30D > 0
                 THEN (DEBIT_VELOCITY_7D * (30.0/7.0)) / DEBIT_VELOCITY_30D
                 ELSE 1 END                                                     AS VELOCITY_RATIO,
            /* Binary target: late payments > 2 is the default proxy */
            CAST(CASE WHEN PAYMENT_LATE_CNT > 2 THEN 1 ELSE 0 END AS DOUBLE)    AS {LABEL_COL}
        FROM imputed
        """
    )


def _driver_expr() -> str:
    """Fold over the four components, mirroring the SAS `do i = 1 to 4` loop."""
    components = [
        ("CREDIT_RISK_COMPONENT", DRIVER_LABELS[0]),
        ("BEHAVIOUR_RISK_COMPONENT", DRIVER_LABELS[1]),
        ("VELOCITY_RISK_COMPONENT", DRIVER_LABELS[2]),
        ("100 - BUREAU_SCORE_COMPONENT", DRIVER_LABELS[3]),
    ]
    items = ", ".join(
        f"struct(CAST({value} AS DOUBLE) AS v, '{label}' AS l)" for value, label in components
    )
    return f"""
        aggregate(
            array({items}),
            struct(CAST(0 AS DOUBLE) AS max1, CAST(0 AS DOUBLE) AS max2,
                   CAST('' AS STRING) AS p, CAST('' AS STRING) AS s),
            (acc, x) -> CASE
                WHEN x.v > acc.max1
                    THEN struct(x.v AS max1, acc.max1 AS max2, x.l AS p, acc.p AS s)
                WHEN x.v > acc.max2
                    THEN struct(acc.max1 AS max1, x.v AS max2, acc.p AS p, x.l AS s)
                ELSE acc
            END
        )
    """


def classify(scored: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """`WORK.RISK_CLASSIFIED`: components, composite score, tier and drivers."""
    components = scored.selectExpr(
        "CUSTOMER_ID",
        "VELOCITY_RATIO",
        "PROB_DEFAULT",
        "greatest(0, least(100, 100 - BUREAU_SCORE_NORM))  AS CREDIT_RISK_COMPONENT",
        "greatest(0, least(100, 100 - PAYMENT_ONTIME_PCT)) AS BEHAVIOUR_RISK_COMPONENT",
        "greatest(0, least(100, (VELOCITY_RATIO - 1) * 50)) AS VELOCITY_RISK_COMPONENT",
        "greatest(0, least(100, BUREAU_SCORE_NORM))        AS BUREAU_SCORE_COMPONENT",
        "greatest(0, least(100, PAYMENT_ONTIME_PCT))       AS PAYMENT_HISTORY_COMPONENT",
    )

    with_score = components.selectExpr(
        "*",
        """round(
               CREDIT_RISK_COMPONENT    * 0.30 +
               BEHAVIOUR_RISK_COMPONENT * 0.25 +
               VELOCITY_RISK_COMPONENT  * 0.15 +
               (100 - BUREAU_SCORE_COMPONENT)    * 0.20 +
               (100 - PAYMENT_HISTORY_COMPONENT) * 0.10
           , 2) AS COMPOSITE_RISK_SCORE""",
        "round(COALESCE(PROB_DEFAULT, 0), 6) AS PROBABILITY_OF_DEFAULT",
        f"{_driver_expr()} AS _drivers",
    )

    return with_score.selectExpr(
        "CUSTOMER_ID",
        "COMPOSITE_RISK_SCORE",
        """CASE
               WHEN COMPOSITE_RISK_SCORE < 20 THEN 'LOW'
               WHEN COMPOSITE_RISK_SCORE < 40 THEN 'MODERATE'
               WHEN COMPOSITE_RISK_SCORE < 60 THEN 'ELEVATED'
               WHEN COMPOSITE_RISK_SCORE < 80 THEN 'HIGH'
               ELSE 'CRITICAL'
           END AS RISK_TIER""",
        "PROBABILITY_OF_DEFAULT",
        "CREDIT_RISK_COMPONENT",
        "BEHAVIOUR_RISK_COMPONENT",
        "VELOCITY_RISK_COMPONENT",
        "BUREAU_SCORE_COMPONENT",
        "PAYMENT_HISTORY_COMPONENT",
        "_drivers.p AS PRIMARY_RISK_DRIVER",
        "_drivers.s AS SECONDARY_RISK_DRIVER",
        "VELOCITY_RATIO",
    ).select(
        "CUSTOMER_ID",
        "COMPOSITE_RISK_SCORE",
        "RISK_TIER",
        "PROBABILITY_OF_DEFAULT",
        "CREDIT_RISK_COMPONENT",
        "BEHAVIOUR_RISK_COMPONENT",
        "VELOCITY_RISK_COMPONENT",
        "BUREAU_SCORE_COMPONENT",
        "PAYMENT_HISTORY_COMPONENT",
        "PRIMARY_RISK_DRIVER",
        "SECONDARY_RISK_DRIVER",
        # Placeholder in the SAS model: would compare against the prior run
        F.lit(0.0).alias("SCORE_DELTA_30D"),
        # Watch list: critical tier plus a high probability of default
        F.when(
            (F.col("RISK_TIER") == "CRITICAL") & (F.col("PROBABILITY_OF_DEFAULT") > 0.5), "Y"
        )
        .otherwise("N")
        .alias("WATCH_LIST_FLAG"),
        # Review required: elevated or above with a recent velocity spike
        F.when(
            (F.col("COMPOSITE_RISK_SCORE") >= 60) & (F.col("VELOCITY_RATIO") > 2.0), "Y"
        )
        .otherwise("N")
        .alias("REVIEW_REQUIRED_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.lit(cfg.run_date).cast("date").alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )


def build_risk_scores(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    features = build_risk_features(spark, cfg).cache()
    fit = stepwise_logistic(features, MODEL_FEATURES, LABEL_COL, slentry=0.10, slstay=0.05)
    scored = predict_probability(features, fit, LABEL_COL)
    return classify(scored, cfg)


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    ensure_run_log(spark, cfg)
    target = cfg.gold(TARGET_TABLE)

    with step(spark, cfg, JOB_NAME, "SCORE_AND_LOAD") as ctx:
        df = schemas.conform(build_risk_scores(spark, cfg), schemas.GOLD_SCHEMAS[TARGET_TABLE])
        ctx["row_count"] = io.write_table(spark, cfg, df, target, merge_keys=["CUSTOMER_ID"])
        ctx["message"] = (
            f"model_version={MODEL_VERSION} risk_score_threshold={cfg.risk_score_threshold}"
        )
        rows = ctx["row_count"]

    validate_and_log(
        spark,
        cfg,
        JOB_NAME,
        target,
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER"],
    )
    return rows


# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = PipelineConfig.from_widgets(spark)
    logger = get_logger()
    log_event(logger, "job_start", run_id=cfg.run_id, job=JOB_NAME, config=cfg.describe())

    if not exit_if_skipped(cfg, "gold", spark):
        log_event(
            logger,
            "job_complete",
            run_id=cfg.run_id,
            job=JOB_NAME,
            table=TARGET_TABLE,
            row_count=run(spark, cfg),
        )
