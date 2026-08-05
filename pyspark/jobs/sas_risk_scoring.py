"""PySpark port of ``sas/03_sas_risk_scoring.sas``.

Builds ``DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES``: a composite risk score, risk tier, component
breakdown, top two risk drivers and review flags for every active customer, plus a probability
of default from a stepwise logistic regression.

The SAS program's step sequence is preserved one-to-one:

===== ============================================ =========================================
Step  Legacy construct                             Port
===== ============================================ =========================================
1     ``proc sql`` extract (inner join, status A)   :func:`transform_risk_raw`
2     ``data WORK.RISK_FEATURES`` feature prep      :func:`transform_risk_features`
3     ``proc logistic ... selection=stepwise``      :func:`select_features_stepwise` +
                                                    :func:`transform_probability_of_default`
4     ``data WORK.RISK_CLASSIFIED`` + rename        :func:`transform_risk_classified`
5     ``%validate_table`` + ``%abort cancel``       :func:`run`
6     ``DELETE FROM`` + ``proc append``             :func:`run` (overwrite write)
===== ============================================ =========================================
"""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig, RiskScoringConstants
from common.functions import clamp_0_100, run_date_col, sas_round, yn
from common.io import DataIO
from common.job import STATUS_SUCCESS, JobResult, job_entry_point
from common.schemas import enforce_schema
from common.validation import abort_on_failure, validate_table

LOGGER = logging.getLogger(__name__)

JOB_NAME = "03_sas_risk_scoring"
STEP_NAME = "FULL_LOAD"
TARGET = schemas.CUSTOMER_RISK_SCORES

#: ``%let MODEL_VERSION = RISK_V4.0;`` - sas/03_sas_risk_scoring.sas:23
MODEL_VERSION = "RISK_V4.0"

#: ``where c.CUSTOMER_STATUS = 'A'`` - sas/03_sas_risk_scoring.sas:42
ACTIVE_CUSTOMER_STATUS = "A"

#: Columns taken from ``STGDB.STG_CUSTOMER_360`` - sas/03_sas_risk_scoring.sas:35-38
CUSTOMER_360_COLUMNS = ("TENURE_MONTHS", "NUM_ACTIVE_ACCOUNTS", "TOTAL_BALANCE", "CUSTOMER_STATUS")

#: ``VELOCITY_RATIO = (DEBIT_VELOCITY_7D * (30/7)) / DEBIT_VELOCITY_30D``
#: - sas/03_sas_risk_scoring.sas:70
VELOCITY_ANNUALISER = 30 / 7

#: ``model DEFAULT_FLAG = ...`` in legacy order - sas/03_sas_risk_scoring.sas:88-97
CANDIDATE_FEATURES: tuple[str, ...] = (
    "BUREAU_SCORE_NORM",
    "CREDIT_UTIL_RATIO",
    "PAYMENT_ONTIME_PCT",
    "BALANCE_VOLATILITY",
    "VELOCITY_RATIO",
    "ACCOUNT_OVERDRAFT_CNT",
    "LARGE_WITHDRAWAL_CNT",
    "HIGH_RISK_MERCHANT_CNT",
    "TENURE_MONTHS",
)

#: ``array _comp[4] ...`` / ``array _lbl[4] ...`` - sas/03_sas_risk_scoring.sas:143-147
DRIVER_LABELS: tuple[str, ...] = (
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
)

#: ``PROBABILITY_OF_DEFAULT = round(coalesce(PROB_DEFAULT, 0), 0.000001);``
#: - sas/03_sas_risk_scoring.sas:131
PROBABILITY_ROUNDING_UNIT = 0.000001

#: ``COMPOSITE_RISK_SCORE = round(..., 0.01);`` - sas/03_sas_risk_scoring.sas:122-128
SCORE_ROUNDING_UNIT = 0.01

PROB_DEFAULT_COLUMN = "PROB_DEFAULT"
DEFAULT_FLAG_COLUMN = "DEFAULT_FLAG"


# ------------------------------------------------------------------------------------------
# Step 1 + 2: extract and feature preparation
# ------------------------------------------------------------------------------------------


def transform_risk_raw(risk_factors: DataFrame, customer_360: DataFrame) -> DataFrame:
    """``WORK.RISK_RAW``: risk factors inner-joined to customer 360, active customers only.

    The legacy ``select r.*, c.TENURE_MONTHS, c.NUM_ACTIVE_ACCOUNTS, c.TOTAL_BALANCE,
    c.CUSTOMER_STATUS`` is reproduced by projecting the customer-360 side onto exactly those
    four columns before the join, so the shuffle only carries what the job uses.
    """

    customers = customer_360.select("CUSTOMER_ID", *CUSTOMER_360_COLUMNS).filter(
        F.col("CUSTOMER_STATUS") == ACTIVE_CUSTOMER_STATUS
    )
    return risk_factors.join(customers, on="CUSTOMER_ID", how="inner")


def transform_risk_features(
    risk_raw: DataFrame, *, risk: RiskScoringConstants | None = None
) -> DataFrame:
    """``WORK.RISK_FEATURES`` - sas/03_sas_risk_scoring.sas:53-76.

    Every derivation is ported verbatim, including the bureau-score imputation that lives here
    rather than in the upstream BTEQ job. SAS arithmetic is IEEE double throughout, so the
    DECIMAL staging columns are widened to ``double`` before the ratios are computed; otherwise
    Spark's decimal division rules would silently truncate the result.
    """

    risk = risk or RiskScoringConstants()
    score = F.col("EXTERNAL_CREDIT_SCORE").cast("double")
    # `if EXTERNAL_CREDIT_SCORE <= 0 or EXTERNAL_CREDIT_SCORE = . then 680` (line 57): in SAS a
    # missing numeric sorts below every value, so `<= 0` already covers it; kept explicit here.
    imputed_score = F.when(
        score.isNull() | (score <= 0), F.lit(float(risk.bureau_score_impute))
    ).otherwise(score)
    bureau_score_norm = (
        (imputed_score - F.lit(float(risk.bureau_score_min)))
        / F.lit(float(risk.bureau_score_max - risk.bureau_score_min))
        * F.lit(100.0)
    )

    balance_30d = F.col("AVG_DAILY_BALANCE_30D").cast("double")
    balance_90d = F.col("AVG_DAILY_BALANCE_90D").cast("double")
    balance_trend_ratio = F.when(balance_90d > 0, balance_30d / balance_90d).otherwise(F.lit(1.0))

    velocity_7d = F.col("DEBIT_VELOCITY_7D").cast("double")
    velocity_30d = F.col("DEBIT_VELOCITY_30D").cast("double")
    velocity_ratio = F.when(
        velocity_30d > 0, (velocity_7d * F.lit(VELOCITY_ANNUALISER)) / velocity_30d
    ).otherwise(F.lit(1.0))

    # `DEFAULT_FLAG = (PAYMENT_LATE_CNT > 2)` (line 75): a SAS boolean is 1/0, and a missing
    # count compares false, i.e. 0.
    default_flag = F.when(
        F.col("PAYMENT_LATE_CNT") > F.lit(risk.default_flag_late_payment_threshold), F.lit(1)
    ).otherwise(F.lit(0))

    return (
        risk_raw.withColumn("EXTERNAL_CREDIT_SCORE", imputed_score)
        .withColumn("BUREAU_SCORE_NORM", bureau_score_norm)
        .withColumn("BALANCE_TREND_RATIO", balance_trend_ratio)
        .withColumn("VELOCITY_RATIO", velocity_ratio)
        .withColumn(DEFAULT_FLAG_COLUMN, default_flag)
    )


# ------------------------------------------------------------------------------------------
# Step 3: PROC LOGISTIC with stepwise selection
# ------------------------------------------------------------------------------------------


def _assemble(df: DataFrame, features: Sequence[str]) -> DataFrame:
    """Design matrix for ``features``, keyed by ``CUSTOMER_ID``.

    Restricted to complete cases: PROC LOGISTIC deletes an observation with a missing covariate
    from both the fit and the ``output`` data set, and the resulting missing prediction is what
    the legacy ``coalesce(PROB_DEFAULT, 0)`` mops up.
    """

    from pyspark.ml.feature import VectorAssembler

    prepared = df.select(
        F.col("CUSTOMER_ID"),
        F.col(DEFAULT_FLAG_COLUMN).cast("double").alias("_label"),
        *[F.col(name).cast("double").alias(f"_f_{name}") for name in features],
    )
    for name in features:
        prepared = prepared.filter(F.col(f"_f_{name}").isNotNull())
    return VectorAssembler(
        inputCols=[f"_f_{name}" for name in features], outputCol="_features"
    ).transform(prepared)


def _fit_logistic(df: DataFrame, features: Sequence[str]):
    """Fit an unregularised logistic regression on ``features`` (``descending``: event = 1)."""

    from pyspark.ml.classification import LogisticRegression

    assembled = _assemble(df, features).persist()
    model = LogisticRegression(
        featuresCol="_features",
        labelCol="_label",
        regParam=0.0,
        elasticNetParam=0.0,
        fitIntercept=True,
        standardization=True,
        maxIter=100,
        tol=1e-6,
    ).fit(assembled)
    return model, assembled


def wald_p_values(model, assembled: DataFrame, features: Sequence[str]) -> dict[str, float]:
    """Two-sided Wald p-values for each fitted coefficient.

    PySpark's ``LogisticRegressionTrainingSummary`` exposes no coefficient standard errors (only
    the linear-regression summary does), so they are derived from the observed Fisher
    information ``X' W X`` with ``W = diag(p(1-p))``. The information matrix is accumulated in a
    single distributed aggregation of ``(k+1)(k+2)/2`` sums and only the small
    ``(k+1) x (k+1)`` matrix reaches the driver, so this scales with the data.

    A singular information matrix (perfect separation, a constant feature) yields ``p = 1.0``
    for the affected coefficient, which keeps such a variable out of the model.
    """

    import numpy as np
    from pyspark.ml.functions import vector_to_array

    scored = model.transform(assembled).withColumn(
        "_w",
        vector_to_array(F.col("probability"))[1]
        * (F.lit(1.0) - vector_to_array(F.col("probability"))[1]),
    )
    design = [F.lit(1.0)] + [F.col(f"_f_{name}") for name in features]
    size = len(design)
    sums = scored.agg(
        *[
            F.sum(F.col("_w") * design[i] * design[j]).alias(f"_i{i}_{j}")
            for i in range(size)
            for j in range(i, size)
        ]
    ).collect()[0]

    information = np.zeros((size, size))
    for i in range(size):
        for j in range(i, size):
            value = sums[f"_i{i}_{j}"]
            information[i, j] = information[j, i] = 0.0 if value is None else float(value)

    try:
        covariance = np.linalg.inv(information)
    except np.linalg.LinAlgError:
        covariance = np.linalg.pinv(information)

    coefficients = model.coefficients.toArray()
    p_values: dict[str, float] = {}
    for index, name in enumerate(features, start=1):
        variance = covariance[index, index]
        if not math.isfinite(variance) or variance <= 0:
            p_values[name] = 1.0
            continue
        z_statistic = abs(float(coefficients[index - 1])) / math.sqrt(variance)
        p_values[name] = 1.0 if not math.isfinite(z_statistic) else math.erfc(z_statistic / 2**0.5)
    return p_values


def has_both_classes(features: DataFrame) -> bool:
    """Whether ``DEFAULT_FLAG`` takes both values, i.e. whether a model can be fitted at all."""

    return features.select(DEFAULT_FLAG_COLUMN).distinct().limit(3).count() > 1


def select_features_stepwise(
    features: DataFrame,
    candidates: Sequence[str] = CANDIDATE_FEATURES,
    *,
    slentry: float = RiskScoringConstants().stepwise_slentry,
    slstay: float = RiskScoringConstants().stepwise_slstay,
) -> tuple[str, ...]:
    """``selection=stepwise slentry=0.10 slstay=0.05`` - sas/03_sas_risk_scoring.sas:98-100.

    Forward selection driven by the Wald p-value of the candidate coefficient, followed after
    each entry by backward elimination of any retained variable whose Wald p-value exceeds
    ``slstay``; as in PROC LOGISTIC, selection stops when the variable that has just entered
    would immediately be removed. Candidates keep the legacy declaration order, which is also
    the deterministic tiebreaker when two candidates share the smallest p-value (SAS leaves
    that case unspecified).

    A single-class ``DEFAULT_FLAG`` - which the upstream BTEQ quirk makes the normal case -
    returns an empty selection: no model can be fitted, and
    :func:`transform_probability_of_default` then falls back to the base rate.
    """

    if not has_both_classes(features):
        LOGGER.warning(
            "%s: DEFAULT_FLAG has a single class; PROC LOGISTIC cannot be reproduced, "
            "falling back to the base rate",
            JOB_NAME,
        )
        return ()

    selected: list[str] = []
    remaining = list(candidates)
    while remaining:
        entry_p: dict[str, float] = {}
        for candidate in remaining:
            trial = [*selected, candidate]
            model, assembled = _fit_logistic(features, trial)
            entry_p[candidate] = wald_p_values(model, assembled, trial)[candidate]
            assembled.unpersist()
        best = min(remaining, key=lambda name: (entry_p[name], candidates.index(name)))
        if entry_p[best] >= slentry:
            break
        selected.append(best)
        remaining.remove(best)

        while len(selected) > 1:
            model, assembled = _fit_logistic(features, selected)
            stay_p = wald_p_values(model, assembled, selected)
            assembled.unpersist()
            worst = max(selected, key=lambda name: (stay_p[name], -candidates.index(name)))
            if stay_p[worst] <= slstay:
                break
            selected.remove(worst)
            remaining.append(worst)
            if worst == best:  # the variable that just entered was removed again: stop
                return tuple(selected)
    return tuple(selected)


def transform_probability_of_default(
    features: DataFrame, selected: Sequence[str] = CANDIDATE_FEATURES
) -> DataFrame:
    """``output out=WORK.RISK_SCORED predicted=PROB_DEFAULT`` - sas/03_sas_risk_scoring.sas:102.

    ``descending`` makes ``DEFAULT_FLAG = 1`` the modelled event, so the prediction is the
    second element of Spark's probability vector.

    With no selected feature - either because stepwise selection retained none or because the
    target has a single class - the model degenerates to an intercept, whose maximum-likelihood
    prediction is the base rate of the event. That is the deterministic fallback used here; for
    the all-zero target produced by the upstream BTEQ payment logic it yields ``0.0``, which is
    also what the legacy ``coalesce(PROB_DEFAULT, 0)`` produces when PROC LOGISTIC declines to
    fit a single-level response.
    """

    if not selected:
        base_rate = features.agg(F.avg(F.col(DEFAULT_FLAG_COLUMN).cast("double"))).collect()[0][0]
        return features.withColumn(
            PROB_DEFAULT_COLUMN, F.lit(float(base_rate or 0.0)).cast("double")
        )

    from pyspark.ml.functions import vector_to_array

    model, assembled = _fit_logistic(features, selected)
    predictions = model.transform(assembled).select(
        F.col("CUSTOMER_ID"),
        vector_to_array(F.col("probability"))[1].alias(PROB_DEFAULT_COLUMN),
    )
    return features.join(predictions, on="CUSTOMER_ID", how="left")


# ------------------------------------------------------------------------------------------
# Step 4: composite score, tier, drivers and flags
# ------------------------------------------------------------------------------------------


def transform_risk_drivers(df: DataFrame) -> DataFrame:
    """``do i = 1 to 4`` top-two driver loop - sas/03_sas_risk_scoring.sas:143-162.

    The loop is unrolled into four column projections - one per array element, not one per row -
    each updating the ``_max1``/``_max2``/driver state simultaneously, which is exactly the
    semantics of the SAS statements inside the ``do`` block.

    The loop's quirks are preserved:

    * ``_max1``/``_max2`` start at ``0`` and the comparisons are strictly ``>``, so a component
      equal to ``0`` never becomes a driver and an all-zero row keeps both drivers blank;
    * a tie keeps the earlier array index;
    * on a new maximum the previous *primary label* is demoted to secondary (the previous
      secondary is dropped);
    * element 4 is the expression ``100 - BUREAU_SCORE_COMPONENT``, which is numerically the
      same as element 1 whenever ``BUREAU_SCORE_NORM`` is inside ``[0, 100]``, so with strict
      ``>`` it can only ever become the *secondary* driver.
    """

    components = (
        F.col("CREDIT_RISK_COMPONENT"),
        F.col("BEHAVIOUR_RISK_COMPONENT"),
        F.col("VELOCITY_RISK_COMPONENT"),
        F.lit(100.0) - F.col("BUREAU_SCORE_COMPONENT"),
    )
    state = (
        df.withColumn("_max1", F.lit(0.0))
        .withColumn("_max2", F.lit(0.0))
        .withColumn("PRIMARY_RISK_DRIVER", F.lit(""))
        .withColumn("SECONDARY_RISK_DRIVER", F.lit(""))
    )
    for component, label in zip(components, DRIVER_LABELS, strict=True):
        new_primary = component > F.col("_max1")
        new_secondary = ~new_primary & (component > F.col("_max2"))
        state = state.withColumns(
            {
                "_max2": F.when(new_primary, F.col("_max1"))
                .when(new_secondary, component)
                .otherwise(F.col("_max2")),
                "SECONDARY_RISK_DRIVER": F.when(new_primary, F.col("PRIMARY_RISK_DRIVER"))
                .when(new_secondary, F.lit(label))
                .otherwise(F.col("SECONDARY_RISK_DRIVER")),
                "_max1": F.when(new_primary, component).otherwise(F.col("_max1")),
                "PRIMARY_RISK_DRIVER": F.when(new_primary, F.lit(label)).otherwise(
                    F.col("PRIMARY_RISK_DRIVER")
                ),
            }
        )
    return state.drop("_max1", "_max2")


def transform_risk_classified(
    scored: DataFrame,
    *,
    run_date: date,
    model_version: str = MODEL_VERSION,
    risk: RiskScoringConstants | None = None,
    load_ts: Column | None = None,
) -> DataFrame:
    """``WORK.RISK_CLASSIFIED`` + the ``PAYMENT_HISTORY_COMP`` rename.

    - sas/03_sas_risk_scoring.sas:111-188.

    The composite deliberately double-counts: ``100 - BUREAU_SCORE_COMPONENT`` duplicates
    ``CREDIT_RISK_COMPONENT`` and ``100 - PAYMENT_HISTORY_COMP`` duplicates
    ``BEHAVIOUR_RISK_COMPONENT``, so the bureau score carries ``0.30 + 0.20`` and the payment
    history ``0.25 + 0.10``, and nothing in the score reflects credit utilisation despite the
    ``CREDIT_UTILIZATION`` driver label. Ported exactly as written.
    """

    risk = risk or RiskScoringConstants()
    load_ts = F.current_timestamp() if load_ts is None else load_ts

    bureau_score_norm = F.col("BUREAU_SCORE_NORM").cast("double")
    payment_ontime_pct = F.col("PAYMENT_ONTIME_PCT").cast("double")
    velocity_ratio = F.col("VELOCITY_RATIO").cast("double")

    components = scored.select(
        "CUSTOMER_ID",
        velocity_ratio.alias("VELOCITY_RATIO"),
        clamp_0_100(F.lit(100.0) - bureau_score_norm).alias("CREDIT_RISK_COMPONENT"),
        clamp_0_100(F.lit(100.0) - payment_ontime_pct).alias("BEHAVIOUR_RISK_COMPONENT"),
        clamp_0_100((velocity_ratio - F.lit(1.0)) * F.lit(50.0)).alias("VELOCITY_RISK_COMPONENT"),
        clamp_0_100(bureau_score_norm).alias("BUREAU_SCORE_COMPONENT"),
        clamp_0_100(payment_ontime_pct).alias("PAYMENT_HISTORY_COMP"),
        sas_round(
            F.coalesce(F.col(PROB_DEFAULT_COLUMN).cast("double"), F.lit(0.0)),
            PROBABILITY_ROUNDING_UNIT,
        ).alias("PROBABILITY_OF_DEFAULT"),
    )

    composite = sas_round(
        F.col("CREDIT_RISK_COMPONENT") * F.lit(risk.credit_risk_weight)
        + F.col("BEHAVIOUR_RISK_COMPONENT") * F.lit(risk.behaviour_risk_weight)
        + F.col("VELOCITY_RISK_COMPONENT") * F.lit(risk.velocity_risk_weight)
        + (F.lit(100.0) - F.col("BUREAU_SCORE_COMPONENT")) * F.lit(risk.bureau_score_weight)
        + (F.lit(100.0) - F.col("PAYMENT_HISTORY_COMP")) * F.lit(risk.payment_history_weight),
        SCORE_ROUNDING_UNIT,
    )
    scored_with_composite = components.withColumn("COMPOSITE_RISK_SCORE", composite)

    tier = (
        F.when(F.col("COMPOSITE_RISK_SCORE") < F.lit(risk.tier_low_max), F.lit("LOW"))
        .when(F.col("COMPOSITE_RISK_SCORE") < F.lit(risk.tier_moderate_max), F.lit("MODERATE"))
        .when(F.col("COMPOSITE_RISK_SCORE") < F.lit(risk.tier_elevated_max), F.lit("ELEVATED"))
        .when(F.col("COMPOSITE_RISK_SCORE") < F.lit(risk.tier_high_max), F.lit("HIGH"))
        .otherwise(F.lit("CRITICAL"))
    )
    classified = transform_risk_drivers(scored_with_composite.withColumn("RISK_TIER", tier))

    projected = classified.select(
        F.col("CUSTOMER_ID"),
        F.col("COMPOSITE_RISK_SCORE"),
        F.col("RISK_TIER"),
        F.col("PROBABILITY_OF_DEFAULT"),
        F.col("CREDIT_RISK_COMPONENT"),
        F.col("BEHAVIOUR_RISK_COMPONENT"),
        F.col("VELOCITY_RISK_COMPONENT"),
        F.col("BUREAU_SCORE_COMPONENT"),
        # `rename=(PAYMENT_HISTORY_COMP=PAYMENT_HISTORY_COMPONENT)` (line 187)
        F.col("PAYMENT_HISTORY_COMP").alias("PAYMENT_HISTORY_COMPONENT"),
        F.col("PRIMARY_RISK_DRIVER"),
        F.col("SECONDARY_RISK_DRIVER"),
        # `SCORE_DELTA_30D = 0;` placeholder (line 165)
        F.lit(0.0).alias("SCORE_DELTA_30D"),
        yn(
            (F.col("RISK_TIER") == F.lit("CRITICAL"))
            & (F.col("PROBABILITY_OF_DEFAULT") > F.lit(risk.watch_list_pd_threshold))
        ).alias("WATCH_LIST_FLAG"),
        yn(
            (F.col("COMPOSITE_RISK_SCORE") >= F.lit(risk.review_required_score_threshold))
            & (F.col("VELOCITY_RATIO") > F.lit(risk.review_required_velocity_threshold))
        ).alias("REVIEW_REQUIRED_FLAG"),
        F.lit(model_version).alias("MODEL_VERSION"),
        run_date_col(run_date).alias("EFFECTIVE_DATE"),
        load_ts.alias("LOAD_TS"),
    )
    return enforce_schema(projected, TARGET)


def transform_customer_risk_scores(
    risk_factors: DataFrame,
    customer_360: DataFrame,
    *,
    run_date: date,
    model_version: str = MODEL_VERSION,
    risk: RiskScoringConstants | None = None,
    selected_features: Sequence[str] | None = None,
    load_ts: Column | None = None,
) -> DataFrame:
    """Compose steps 1-4 into ``DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES``.

    ``selected_features`` short-circuits the stepwise search (used by the tests to keep the
    model fitting out of a parity comparison); ``None`` runs the full selection.
    """

    risk = risk or RiskScoringConstants()
    features = transform_risk_features(transform_risk_raw(risk_factors, customer_360), risk=risk)
    if selected_features is None:
        selected_features = select_features_stepwise(
            features, slentry=risk.stepwise_slentry, slstay=risk.stepwise_slstay
        )
    scored = transform_probability_of_default(features, selected_features)
    return transform_risk_classified(
        scored, run_date=run_date, model_version=model_version, risk=risk, load_ts=load_ts
    )


def run(spark: SparkSession, io: DataIO, config: PipelineConfig, audit: AuditLog) -> JobResult:
    """Execute the job end to end, mirroring the SAS program's step sequence."""

    result = JobResult(job_name=JOB_NAME, target_table=TARGET.qualified_name)
    # `%let RISK_THRESHOLD = %sysget(RISK_SCORE_THRESHOLD);` (line 24) is set by the legacy
    # program and then never referenced; it is read here and recorded in the audit trail.
    audit.log_step(
        JOB_NAME,
        "START",
        f"Model version {MODEL_VERSION} "
        f"(RISK_SCORE_THRESHOLD={config.risk_score_threshold}, unused by the legacy model)",
    )

    risk_factors = io.read_spec(schemas.STG_RISK_FACTORS)
    customer_360 = io.read_spec(schemas.STG_CUSTOMER_360)

    features = transform_risk_features(
        transform_risk_raw(risk_factors, customer_360), risk=config.risk
    ).persist()
    audit.log_step(JOB_NAME, "SUCCESS", "Extracted risk factors", rowcount=features.count())

    audit.log_step(JOB_NAME, "START", "Training logistic regression model")
    selected = select_features_stepwise(
        features, slentry=config.risk.stepwise_slentry, slstay=config.risk.stepwise_slstay
    )
    audit.log_step(
        JOB_NAME,
        "SUCCESS",
        f"Stepwise selection retained {len(selected)} of {len(CANDIDATE_FEATURES)} features"
        f"{': ' + ', '.join(selected) if selected else ' (intercept-only base rate)'}",
    )

    audit.log_step(JOB_NAME, "START", "Computing composite scores")
    output = transform_risk_classified(
        transform_probability_of_default(features, selected),
        run_date=config.run_date,
        risk=config.risk,
    ).persist()

    validation = validate_table(
        output,
        table=TARGET.qualified_name,
        key_cols=TARGET.primary_index,
        not_null=("CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER"),
        min_rows=config.min_rows,
    )
    result.validation = validation
    if validation.passed:
        audit.log_step(JOB_NAME, "SUCCESS", "Risk scores computed", rowcount=validation.row_count)
    else:
        audit.log_step(JOB_NAME, "ERROR", "Validation failed")
    abort_on_failure(validation)

    # `DELETE FROM DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES` + `proc append` = full refresh.
    row_count = io.write_spec(output, TARGET, mode="overwrite")
    output.unpersist()
    features.unpersist()

    result.row_count = row_count
    result.status = STATUS_SUCCESS
    result.end_ts = datetime.now()
    audit.log_step(JOB_NAME, "SUCCESS", "Pipeline complete", rowcount=row_count)
    audit.log_run(JOB_NAME, STEP_NAME, "SUCCESS", row_count, result.start_ts, result.end_ts)
    return result


main = job_entry_point(run, JOB_NAME)


if __name__ == "__main__":
    main()
