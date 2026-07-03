"""Data-product job 03 -- CUSTOMER_RISK_SCORES.

Faithful PySpark port of ``sas/03_sas_risk_scoring.sas``: consume the BTEQ-built
``STG_RISK_FACTORS`` (joined to ``STG_CUSTOMER_360`` for baseline attributes),
train a stepwise logistic-regression model for probability of default, build a
deterministic weighted composite risk score, and classify each active customer
into a risk tier with primary/secondary risk drivers and watch/review flags.

SAS -> PySpark mapping:
* ``PROC SQL`` inner join filtered to ``CUSTOMER_STATUS = 'A'`` ->
  :func:`extract_risk_input`.
* ``DATA`` step feature prep (bureau imputation/normalisation, balance-trend and
  velocity ratios, ``DEFAULT_FLAG``) -> :func:`prepare_features`.
* ``PROC LOGISTIC selection=stepwise slentry=0.10 slstay=0.05`` ->
  :mod:`jobs.stepwise_logistic` (Spark has no native stepwise; see
  ``MIGRATION_NOTES.md`` for the LRT p-value approximation).
* Composite score / tiers / drivers / flags ``DATA`` step ->
  :func:`score_and_classify` -- deterministic and matched exactly to the SAS
  arithmetic (component clamping, weighted sum, tier cutoffs, the SAS array-scan
  driver tie-break, and the two flag rules).

Mirrors the reference module ``jobs.staging_customer_360``: pure ``transform``
functions + a thin :func:`run` + a ``main()`` CLI.
"""

from __future__ import annotations

import argparse
import datetime as _dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.dates import load_timestamp
from common.io import DataIO, LocalDataIO
from common.spark import build_spark
from common.validation import abort_on_failure, validate_table
from jobs.stepwise_logistic import StepwiseResult, stepwise_logistic

JOB_NAME = "03_risk_scoring"
TARGET = "CUSTOMER_RISK_SCORES"
MODEL_VERSION = "RISK_V4.0"

# PROC LOGISTIC candidate effects, in the exact order they are offered to
# stepwise selection (snake_case columns matching the staging schema).
CANDIDATE_FEATURES: tuple[str, ...] = (
    "bureau_score_norm",
    "credit_util_ratio",
    "payment_ontime_pct",
    "balance_volatility",
    "velocity_ratio",
    "account_overdraft_cnt",
    "large_withdrawal_cnt",
    "high_risk_merchant_cnt",
    "tenure_months",
)

# Labelled component -> value used for primary/secondary driver selection, in
# the SAS array order (drives the first-max tie-break).
_DRIVER_LABELS: tuple[str, ...] = (
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
)

_BUREAU_IMPUTE = 680.0
_BUREAU_MIN = 300.0
_BUREAU_MAX = 850.0

_PROB_COL = "prob_default"


def _clamp_0_100(col: F.Column) -> F.Column:
    """SAS ``max(0, min(100, x))`` component clamp."""
    return F.least(F.greatest(col, F.lit(0.0)), F.lit(100.0))


def extract_risk_input(risk_factors: DataFrame, customer_360: DataFrame) -> DataFrame:
    """STEP 1 -- inner join STG_RISK_FACTORS to active STG_CUSTOMER_360 rows."""
    baseline = (
        customer_360
        .filter(F.col("customer_status") == "A")
        .select(
            "customer_id",
            "tenure_months",
            "num_active_accounts",
            "total_balance",
            "customer_status",
        )
    )
    return risk_factors.alias("r").join(baseline.alias("c"), "customer_id", "inner")


def prepare_features(df: DataFrame) -> DataFrame:
    """STEP 2 -- impute bureau score, normalise, derive ratios and target."""
    ext = F.col("external_credit_score").cast("double")
    ext_imputed = F.when(ext.isNull() | (ext <= 0), F.lit(_BUREAU_IMPUTE)).otherwise(ext)
    bureau_score_norm = (ext_imputed - F.lit(_BUREAU_MIN)) / (
        F.lit(_BUREAU_MAX) - F.lit(_BUREAU_MIN)
    ) * F.lit(100.0)

    bal_30 = F.col("avg_daily_balance_30d").cast("double")
    bal_90 = F.col("avg_daily_balance_90d").cast("double")
    balance_trend_ratio = F.when(bal_90 > 0, bal_30 / bal_90).otherwise(F.lit(1.0))

    dv_7 = F.col("debit_velocity_7d").cast("double")
    dv_30 = F.col("debit_velocity_30d").cast("double")
    velocity_ratio = F.when(
        dv_30 > 0, (dv_7 * F.lit(30.0 / 7.0)) / dv_30
    ).otherwise(F.lit(1.0))

    default_flag = F.when(F.col("payment_late_cnt") > 2, F.lit(1)).otherwise(F.lit(0))

    return (
        df
        .withColumn("external_credit_score_imp", ext_imputed)
        .withColumn("bureau_score_norm", bureau_score_norm)
        .withColumn("balance_trend_ratio", balance_trend_ratio)
        .withColumn("velocity_ratio", velocity_ratio)
        .withColumn("default_flag", default_flag)
    )


def model_input(features: DataFrame, label_col: str = "default_flag") -> DataFrame:
    """Project the model matrix: key + target + candidate effects as doubles.

    Model covariates are ``coalesce(..., 0)``d so a missing factor never drops a
    customer from scoring (SAS PROC LOGISTIC uses listwise deletion; see
    ``MIGRATION_NOTES.md``).
    """
    cols = [F.col("customer_id"), F.col(label_col)]
    cols += [
        F.coalesce(F.col(name).cast("double"), F.lit(0.0)).alias(name)
        for name in CANDIDATE_FEATURES
    ]
    return features.select(*cols)


def _top_two_drivers(pairs: list[tuple[str, F.Column]]) -> tuple[F.Column, F.Column]:
    """Reproduce the SAS array scan that picks the top-two risk drivers.

    Unrolls ``do i = 1 to 4`` exactly: a strictly-greater value becomes the new
    primary (first-max wins ties); otherwise a value greater than the running
    runner-up becomes the new secondary.
    """
    max1: F.Column = F.lit(0.0)
    max2: F.Column = F.lit(0.0)
    primary: F.Column = F.lit(None).cast("string")
    secondary: F.Column = F.lit(None).cast("string")
    for label, value in pairs:
        is_new_max = value > max1
        is_new_second = (~is_new_max) & (value > max2)
        new_max1 = F.when(is_new_max, value).otherwise(max1)
        new_max2 = F.when(is_new_max, max1).when(is_new_second, value).otherwise(max2)
        new_primary = F.when(is_new_max, F.lit(label)).otherwise(primary)
        new_secondary = (
            F.when(is_new_max, primary)
            .when(is_new_second, F.lit(label))
            .otherwise(secondary)
        )
        max1, max2, primary, secondary = new_max1, new_max2, new_primary, new_secondary
    return primary, secondary


def score_and_classify(
    features: DataFrame,
    prob: DataFrame,
    config: PipelineConfig,
) -> DataFrame:
    """STEP 4 -- composite score, tier, drivers and flags (schema-enforced)."""
    pomt = F.col("payment_ontime_pct").cast("double")

    df = (
        features.join(prob, "customer_id", "left")
        .withColumn("credit_risk_component", _clamp_0_100(F.lit(100.0) - F.col("bureau_score_norm")))
        .withColumn("behaviour_risk_component", _clamp_0_100(F.lit(100.0) - pomt))
        .withColumn(
            "velocity_risk_component",
            _clamp_0_100((F.col("velocity_ratio") - F.lit(1.0)) * F.lit(50.0)),
        )
        .withColumn("bureau_score_component", _clamp_0_100(F.col("bureau_score_norm")))
        .withColumn("payment_history_component", _clamp_0_100(pomt))
    )

    composite = F.round(
        F.col("credit_risk_component") * F.lit(0.30)
        + F.col("behaviour_risk_component") * F.lit(0.25)
        + F.col("velocity_risk_component") * F.lit(0.15)
        + (F.lit(100.0) - F.col("bureau_score_component")) * F.lit(0.20)
        + (F.lit(100.0) - F.col("payment_history_component")) * F.lit(0.10),
        2,
    )
    df = df.withColumn("composite_risk_score", composite)

    df = df.withColumn(
        "probability_of_default",
        F.round(F.coalesce(F.col(_PROB_COL), F.lit(0.0)), 6),
    )

    tier = (
        F.when(F.col("composite_risk_score") < 20, F.lit("LOW"))
        .when(F.col("composite_risk_score") < 40, F.lit("MODERATE"))
        .when(F.col("composite_risk_score") < 60, F.lit("ELEVATED"))
        .when(F.col("composite_risk_score") < 80, F.lit("HIGH"))
        .otherwise(F.lit("CRITICAL"))
    )
    df = df.withColumn("risk_tier", tier)

    primary, secondary = _top_two_drivers([
        ("CREDIT_UTILIZATION", F.col("credit_risk_component")),
        ("PAYMENT_BEHAVIOUR", F.col("behaviour_risk_component")),
        ("TRANSACTION_VELOCITY", F.col("velocity_risk_component")),
        ("BUREAU_SCORE", F.lit(100.0) - F.col("bureau_score_component")),
    ])
    df = (
        df.withColumn("primary_risk_driver", primary)
        .withColumn("secondary_risk_driver", secondary)
        .withColumn("score_delta_30d", F.lit(0.0))
        .withColumn(
            "watch_list_flag",
            F.when(
                (F.col("risk_tier") == "CRITICAL")
                & (F.col("probability_of_default") > 0.5),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn(
            "review_required_flag",
            F.when(
                (F.col("composite_risk_score") >= 60)
                & (F.col("velocity_ratio") > 2.0),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("effective_date", F.lit(config.run_date).cast("date"))
        .withColumn("load_ts", F.lit(load_timestamp()).cast("timestamp"))
    )
    return schemas.enforce_schema(df, schemas.CUSTOMER_RISK_SCORES)


def score_risk(
    risk_factors: DataFrame,
    customer_360: DataFrame,
    config: PipelineConfig,
) -> tuple[DataFrame, StepwiseResult]:
    """Full transform: STEP 1-4. Returns the output DataFrame and model result."""
    joined = extract_risk_input(risk_factors, customer_360)
    features = prepare_features(joined).cache()
    result = stepwise_logistic(
        model_input(features),
        list(CANDIDATE_FEATURES),
        label_col="default_flag",
        slentry=0.10,
        slstay=0.05,
        prob_col=_PROB_COL,
    )
    prob = result.predictions.select("customer_id", _PROB_COL)
    out = score_and_classify(features, prob, config)
    return out, result


def run(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog | None = None,
) -> DataFrame:
    """Read staging, score, validate, and write CUSTOMER_RISK_SCORES."""
    audit = audit or AuditLog(log_level=config.log_level)
    audit.log_step(JOB_NAME, "START", f"Model version {MODEL_VERSION}")

    risk_factors = io.read_staging("STG_RISK_FACTORS")
    customer_360 = io.read_staging("STG_CUSTOMER_360")

    out, result = score_risk(risk_factors, customer_360, config)
    out = out.cache()
    n = out.count()

    if result.fallback:
        audit.log_step(
            JOB_NAME, "WARNING",
            f"Probability-of-default fallback: {result.fallback_reason}",
        )
    else:
        audit.log_step(
            JOB_NAME, "SUCCESS",
            f"Stepwise selected: {', '.join(result.selected_features) or '(none)'}",
        )

    validation = validate_table(
        out, TARGET,
        key_cols=["customer_id"],
        not_null=["customer_id", "composite_risk_score", "risk_tier"],
        min_rows=1, audit=audit,
    )
    abort_on_failure(validation)

    io.write_data_product(out, TARGET)
    audit.run_log_row(JOB_NAME, n)
    audit.log_step(JOB_NAME, "SUCCESS", "Risk scores written", rowcount=n)
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build CUSTOMER_RISK_SCORES")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--lake-dir", required=True)
    parser.add_argument("--run-date", default=None)
    parser.add_argument(
        "--read-products-from-source", action="store_true",
        help="Read staging inputs from the committed CSV fixtures.",
    )
    args = parser.parse_args(argv)

    config = PipelineConfig.from_env().with_overrides(
        **({"run_date": _dt.date.fromisoformat(args.run_date)} if args.run_date else {})
    )
    spark = build_spark(JOB_NAME)
    io = LocalDataIO(
        spark, config, args.source_dir, args.lake_dir,
        read_products_from_source=args.read_products_from_source,
    )
    run(spark, io, config)


if __name__ == "__main__":
    main()
