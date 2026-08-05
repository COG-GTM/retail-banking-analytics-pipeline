"""STEP 3 — probability of default (``PROC LOGISTIC`` replacement).

Ports this block of ``sas/03_sas_risk_scoring.sas``::

    proc logistic data=WORK.RISK_FEATURES outmodel=WORK.RISK_MODEL
                  descending noprint;
        model DEFAULT_FLAG = <9 predictors>
        / selection=stepwise slentry=0.10 slstay=0.05 lackfit;
        output out=WORK.RISK_SCORED predicted=PROB_DEFAULT;
    run;

What is faithful
----------------
* ``descending`` — SAS models ``P(DEFAULT_FLAG = 1)``, so ``PROB_DEFAULT`` is
  element 1 of the MLlib probability vector, extracted with ``vector_to_array``.
* No hold-out set: the model is fitted and scored on the same rows, every run.
  Nothing is cached between runs.
* Unpenalised MLE: ``regParam=0.0``, ``elasticNetParam=0.0``. ``PROC LOGISTIC``
  applies no shrinkage, so neither do we.
* Missing predictors: ``PROC LOGISTIC`` silently drops observations with a
  missing model variable from the fit, but ``output out=`` still emits them with
  a missing predicted value. Every input row therefore survives into
  ``ModelResult.scored``; unscorable rows carry ``PROB_DEFAULT = NULL``, which
  STEP 4 turns into ``0`` via ``round(coalesce(PROB_DEFAULT, 0), 0.000001)``.
* ``selection=stepwise`` with ``slentry=0.10`` / ``slstay=0.05`` is implemented
  manually (see :func:`_stepwise_select`) — MLlib has no variable selection.

Where PySpark cannot match SAS
------------------------------
* **Optimiser.** ``PROC LOGISTIC`` uses Fisher scoring (IRLS); MLlib uses LBFGS.
  Converged coefficients agree only to optimiser tolerance, and on
  near-separable or ill-conditioned data they can differ materially. Combined
  with the stepwise path below, ``PROBABILITY_OF_DEFAULT`` is comparable to the
  SAS oracle **at the distribution level only, never row by row**.
* **Stepwise path.** SAS enters variables on a *score* test computed without
  refitting; the entry test here is a true score test built from the observed
  information matrix, but the ordering of near-tied candidates, and hence the
  final variable set, can still diverge from SAS. A different selected set means
  different coefficients and different probabilities.
* **Wald standard errors.** PySpark's ``LogisticRegressionSummary`` exposes no
  ``coefficientStandardErrors`` (unlike ``LinearRegressionSummary`` /
  ``GeneralizedLinearRegressionSummary``), so the Wald statistics that drive
  ``slstay`` are computed here from the observed information matrix
  ``X' W X`` — the same quantity SAS inverts. This is exact for an unpenalised
  fit, but it is our arithmetic rather than the library's.
* **Chi-square tail.** ``scipy`` is not a dependency; the 1-df survival function
  is evaluated as ``erfc(sqrt(x / 2))`` from :mod:`math`.
* **Degenerate target.** ``DEFAULT_FLAG = (PAYMENT_LATE_CNT > 2)`` is **all-zero
  on the committed extract** (0 of 478 staging rows, 0 of the 407 joined,
  active-customer rows). The model is then unidentifiable: no variable can be
  significant against a constant response, stepwise enters nothing, and the fit
  collapses to the intercept-only MLE, i.e. ``PROB_DEFAULT = mean(DEFAULT_FLAG)
  = 0.0`` for every scorable row (intercept ``-inf``). That is the honest
  degenerate answer, and it is logged as a warning rather than raised. The SAS
  oracle emits a constant ``0.05`` for the same rows; that constant is *not*
  reproduced here, because nothing in the SAS program computes it. See the PR
  description.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field

import numpy as np
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import PipelineConfig
from .schemas import MODEL_PREDICTORS, MODEL_TARGET, PROB_DEFAULT

LOGGER = logging.getLogger(__name__)

# Internal column names. Prefixed so they cannot collide with a contract column;
# all of them are dropped before ``ModelResult.scored`` is returned.
_PREFIX = "_RS_"
_LABEL = _PREFIX + "LABEL"
_FEATURES = _PREFIX + "FEATURES"
_PROBABILITY = _PREFIX + "PROBABILITY"
_RAW_PREDICTION = _PREFIX + "RAW_PREDICTION"
_PREDICTION = _PREFIX + "PREDICTION"
_SCORABLE = _PREFIX + "SCORABLE"
_FITTABLE = _PREFIX + "FITTABLE"
_FITTED_PROB = _PREFIX + "FITTED_PROB"


def _feature_col(name: str) -> str:
    return f"{_PREFIX}F_{name}"


@dataclass(frozen=True)
class ModelOptions:
    """Tunables for the STEP 3 fit.

    Pinned to explicit values so runs are reproducible. Overridable by the
    caller (the integrator's driver), but never read from the environment —
    module code takes its configuration from :class:`PipelineConfig` and its
    arguments only.
    """

    #: SAS ``slentry`` — score-test p-value below which a variable enters.
    slentry: float = 0.10
    #: SAS ``slstay`` — Wald p-value above which a variable is removed.
    slstay: float = 0.05
    #: ``selection=stepwise``. Set ``False`` to fit the full 9-predictor model.
    stepwise: bool = True
    max_iter: int = 100
    tol: float = 1e-6
    #: Cycle guard for the entry/removal loop.
    max_steps: int = 4 * len(MODEL_PREDICTORS)


@dataclass
class ModelResult:
    """Outcome of STEP 3, consumed by ``scoring.classify_risk``."""

    scored: DataFrame
    selected_features: list[str] = field(default_factory=list)
    coefficients: dict[str, float] = field(default_factory=dict)
    intercept: float = 0.0
    steps: list[str] = field(default_factory=list)


def train_and_score(
    risk_features: DataFrame,
    config: PipelineConfig,
    *,
    options: ModelOptions | None = None,
) -> ModelResult:
    """Fit the default model on ``risk_features`` and score the same rows.

    ``ModelResult.scored`` is ``risk_features`` plus a ``PROB_DEFAULT`` double.
    Its row count always equals that of ``risk_features``: rows that could not be
    scored carry ``NULL``, mirroring the missing values SAS leaves in
    ``WORK.RISK_SCORED``.
    """
    options = options or ModelOptions()
    _require_columns(risk_features)

    prepared = _prepare(risk_features).cache()
    try:
        fit_rows = prepared.filter(F.col(_FITTABLE))
        n_fit, n_positive = _target_balance(fit_rows)
        n_total = prepared.count()
        LOGGER.info(
            "step=03_RISK_SCORING model=fit rows_in=%d rows_fittable=%d "
            "events=%d event_rate=%s",
            n_total,
            n_fit,
            n_positive,
            f"{n_positive / n_fit:.6f}" if n_fit else "n/a",
        )

        if n_fit == 0:
            return _unfittable_result(risk_features, prepared, n_total)

        event_rate = n_positive / n_fit
        if n_positive == 0 or n_positive == n_fit:
            return _degenerate_result(
                risk_features, prepared, event_rate=event_rate, n_fit=n_fit
            )

        fit_rows = fit_rows.cache()
        try:
            if options.stepwise:
                selected, steps = _stepwise_select(fit_rows, event_rate, options)
            else:
                selected = list(MODEL_PREDICTORS)
                steps = ["stepwise disabled: full MODEL statement fitted"]

            if not selected:
                steps.append(
                    "no predictors entered: falling back to the intercept-only model"
                )
                LOGGER.warning(
                    "step=03_RISK_SCORING model=stepwise selected=none "
                    "msg=no predictor met slentry=%s; intercept-only model",
                    options.slentry,
                )
                return _degenerate_result(
                    risk_features,
                    prepared,
                    event_rate=event_rate,
                    n_fit=n_fit,
                    steps=steps,
                )

            model = _fit(fit_rows, selected, options)
        finally:
            fit_rows.unpersist()

        coefficients = {
            name: float(value)
            for name, value in zip(selected, model.coefficients.toArray())
        }
        intercept = float(model.intercept)
        LOGGER.info(
            "step=03_RISK_SCORING model=fitted selected=%s intercept=%.6f "
            "coefficients=%s",
            ",".join(selected) or "none",
            intercept,
            {k: round(v, 6) for k, v in coefficients.items()},
        )
        for line in steps:
            LOGGER.info("step=03_RISK_SCORING model=stepwise %s", line)

        scored = _score_with_model(risk_features, prepared, model, selected)
        return ModelResult(
            scored=scored,
            selected_features=selected,
            coefficients=coefficients,
            intercept=intercept,
            steps=steps,
        )
    finally:
        prepared.unpersist()


# --------------------------------------------------------------------------- #
# Preparation                                                                  #
# --------------------------------------------------------------------------- #


def _require_columns(df: DataFrame) -> None:
    missing = [c for c in (*MODEL_PREDICTORS, MODEL_TARGET) if c not in df.columns]
    if missing:
        raise ValueError(
            f"risk_features is missing model columns: {', '.join(missing)}"
        )


def _is_present(column: str) -> Column:
    col = F.col(column)
    return col.isNotNull() & ~F.isnan(col)


def _prepare(df: DataFrame) -> DataFrame:
    """Add the double-cast model columns and the fit/score eligibility flags.

    SAS holds every numeric as a 64-bit float, so the predictors are cast to
    ``double`` in *shadow* columns — the caller's columns and their types are
    left untouched, keeping the transform pure.
    """
    widened = df.select(
        "*",
        *[F.col(c).cast("double").alias(_feature_col(c)) for c in MODEL_PREDICTORS],
        F.col(MODEL_TARGET).cast("double").alias(_LABEL),
    )
    scorable = F.lit(True)
    for name in MODEL_PREDICTORS:
        scorable = scorable & _is_present(_feature_col(name))
    return widened.withColumn(_SCORABLE, scorable).withColumn(
        # PROC LOGISTIC excludes observations with a missing response from the
        # fit, but still scores them through the OUTPUT statement.
        _FITTABLE,
        F.col(_SCORABLE) & _is_present(_LABEL),
    )


def _target_balance(fit_rows: DataFrame) -> tuple[int, int]:
    row = fit_rows.select(
        F.count(F.lit(1)).alias("N"),
        F.coalesce(F.sum(F.col(_LABEL)), F.lit(0.0)).alias("EVENTS"),
    ).first()
    if row is None:
        return 0, 0
    return int(row["N"]), int(row["EVENTS"])


# --------------------------------------------------------------------------- #
# Fitting and scoring                                                          #
# --------------------------------------------------------------------------- #


def _assemble(df: DataFrame, features: list[str]) -> DataFrame:
    assembler = VectorAssembler(
        inputCols=[_feature_col(f) for f in features],
        outputCol=_FEATURES,
        handleInvalid="error",
    )
    return assembler.transform(df)


def _fit(
    fit_rows: DataFrame, features: list[str], options: ModelOptions
) -> LogisticRegressionModel:
    """Unpenalised binomial MLE — the closest MLlib analogue of PROC LOGISTIC."""
    estimator = LogisticRegression(
        featuresCol=_FEATURES,
        labelCol=_LABEL,
        predictionCol=_PREDICTION,
        probabilityCol=_PROBABILITY,
        rawPredictionCol=_RAW_PREDICTION,
        family="binomial",
        fitIntercept=True,
        regParam=0.0,
        elasticNetParam=0.0,
        standardization=True,
        maxIter=options.max_iter,
        tol=options.tol,
    )
    return estimator.fit(_assemble(fit_rows, features))


def _probability_of_event() -> Column:
    """``P(DEFAULT_FLAG = 1)`` — element 1 of the MLlib probability vector.

    SAS ``descending`` makes 1 the modelled level. Extracted through
    ``vector_to_array`` (a VectorUDT-aware expression) rather than a Python UDF.
    """
    return vector_to_array(F.col(_PROBABILITY))[1]


def _score_with_model(
    original: DataFrame,
    prepared: DataFrame,
    model: LogisticRegressionModel,
    features: list[str],
) -> DataFrame:
    """Attach ``PROB_DEFAULT`` to every prepared row, NULL where unscorable.

    Unscorable rows are neutralised (predictors coalesced to 0) so the assembler
    accepts them, then masked back to NULL — no filter, no join, so the row count
    is preserved by construction.
    """
    neutralised = prepared
    for name in features:
        column = _feature_col(name)
        neutralised = neutralised.withColumn(
            column,
            F.when(F.col(_SCORABLE), F.col(column)).otherwise(F.lit(0.0)),
        )

    transformed = model.transform(_assemble(neutralised, features))
    return transformed.select(
        *original.columns,
        F.when(F.col(_SCORABLE), _probability_of_event())
        .otherwise(F.lit(None))
        .cast("double")
        .alias(PROB_DEFAULT),
    )


def _score_constant(
    original: DataFrame, prepared: DataFrame, probability: float | None
) -> DataFrame:
    value = F.lit(None) if probability is None else F.lit(float(probability))
    return prepared.select(
        *original.columns,
        F.when(F.col(_SCORABLE), value).otherwise(F.lit(None)).cast("double").alias(
            PROB_DEFAULT
        ),
    )


def _degenerate_result(
    original: DataFrame,
    prepared: DataFrame,
    *,
    event_rate: float,
    n_fit: int,
    steps: list[str] | None = None,
) -> ModelResult:
    """Intercept-only MLE: ``P(y = 1) = mean(y)`` for every scorable row.

    Reached when the response is constant (the model is unidentifiable — MLlib
    would drive the intercept to ±infinity) or when stepwise entered nothing.
    Both are documented, non-fatal outcomes: every row still gets a probability.
    """
    steps = list(steps or [])
    if event_rate in (0.0, 1.0):
        steps.append(
            f"target DEFAULT_FLAG is constant ({event_rate:.0f}) over {n_fit} "
            "fittable rows: model unidentifiable, no predictors entered"
        )
        LOGGER.warning(
            "step=03_RISK_SCORING model=degenerate rows=%d event_rate=%.6f "
            "msg=DEFAULT_FLAG has a single class; the logistic model is "
            "unidentifiable. Emitting the intercept-only MLE "
            "PROB_DEFAULT=%.6f for every scorable row.",
            n_fit,
            event_rate,
            event_rate,
        )
        intercept = math.inf if event_rate == 1.0 else -math.inf
    else:
        intercept = math.log(event_rate / (1.0 - event_rate))
        LOGGER.warning(
            "step=03_RISK_SCORING model=intercept_only rows=%d event_rate=%.6f "
            "msg=no predictor entered the model; PROB_DEFAULT is constant.",
            n_fit,
            event_rate,
        )
    return ModelResult(
        scored=_score_constant(original, prepared, event_rate),
        selected_features=[],
        coefficients={},
        intercept=intercept,
        steps=steps,
    )


def _unfittable_result(
    original: DataFrame, prepared: DataFrame, n_total: int
) -> ModelResult:
    LOGGER.error(
        "step=03_RISK_SCORING model=unfittable rows=%d msg=no row has a complete "
        "set of model variables; PROB_DEFAULT is NULL for every row.",
        n_total,
    )
    return ModelResult(
        scored=_score_constant(original, prepared, None),
        selected_features=[],
        coefficients={},
        intercept=float("nan"),
        steps=["no fittable observations: no model was estimated"],
    )


# --------------------------------------------------------------------------- #
# Stepwise selection (SAS: selection=stepwise slentry=0.10 slstay=0.05)        #
# --------------------------------------------------------------------------- #


def _chisq_sf_1df(statistic: float) -> float:
    """Upper tail of the chi-square distribution with 1 degree of freedom.

    ``P(X > x) = erfc(sqrt(x / 2))`` for 1 df. Avoids a ``scipy`` dependency,
    which is not guaranteed to be installed on the cluster.
    """
    if not math.isfinite(statistic) or statistic <= 0.0:
        return 1.0
    return math.erfc(math.sqrt(statistic / 2.0))


def _information_and_score(
    fit_rows: DataFrame, probability: Column
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(X'WX, X'(y - p))`` over ``[intercept, *MODEL_PREDICTORS]``.

    ``W = diag(p (1 - p))`` at the current fitted probabilities. One distributed
    aggregation serves both the score tests (which need the full design, so an
    excluded candidate can be tested without refitting) and the Wald standard
    errors (which need the submatrix of the selected columns).
    """
    columns = [_feature_col(name) for name in MODEL_PREDICTORS]
    width = len(columns) + 1
    rows = fit_rows.select(
        *columns, F.col(_LABEL).alias("Y"), probability.alias("P")
    ).rdd

    def seq_op(
        acc: tuple[np.ndarray, np.ndarray], row
    ) -> tuple[np.ndarray, np.ndarray]:
        information, score = acc
        design = np.empty(width, dtype=float)
        design[0] = 1.0
        for index, column in enumerate(columns):
            design[index + 1] = float(row[column])
        prob = float(row["P"])
        weight = prob * (1.0 - prob)
        residual = float(row["Y"]) - prob
        return (
            information + weight * np.outer(design, design),
            score + residual * design,
        )

    def comb_op(
        left: tuple[np.ndarray, np.ndarray], right: tuple[np.ndarray, np.ndarray]
    ) -> tuple[np.ndarray, np.ndarray]:
        return (left[0] + right[0], left[1] + right[1])

    zero = (np.zeros((width, width)), np.zeros(width))
    return rows.treeAggregate(zero, seq_op, comb_op)


def _covariance(information: np.ndarray, indices: list[int]) -> np.ndarray | None:
    block = information[np.ix_(indices, indices)]
    if not np.all(np.isfinite(block)):
        return None
    try:
        return np.linalg.pinv(block, rcond=1e-12, hermitian=True)
    except np.linalg.LinAlgError:  # pragma: no cover - pinv on SVD failure
        return None


def _score_test(
    information: np.ndarray, score: np.ndarray, selected: list[int], candidate: int
) -> float:
    """1-df score-test p-value for adding ``candidate`` to the current model.

    At the MLE of the current model the score is zero on the fitted columns, so
    the statistic reduces to ``U_c^2 * [I^-1]_cc`` on the augmented design.
    """
    indices = [0, *selected, candidate]
    covariance = _covariance(information, indices)
    if covariance is None:
        return 1.0
    variance = float(covariance[-1, -1])
    if variance <= 0.0:
        return 1.0
    statistic = float(score[candidate]) ** 2 * variance
    return _chisq_sf_1df(statistic)


def _wald_pvalues(
    information: np.ndarray, selected: list[str], coefficients: dict[str, float]
) -> dict[str, float]:
    """Wald p-values from the observed information matrix.

    ``LogisticRegressionSummary`` has no ``coefficientStandardErrors`` in
    PySpark, so the covariance matrix ``(X'WX)^-1`` is inverted here. For an
    unpenalised fit this is exactly what SAS reports.
    """
    positions = [MODEL_PREDICTORS.index(name) + 1 for name in selected]
    covariance = _covariance(information, [0, *positions])
    pvalues: dict[str, float] = {}
    for offset, name in enumerate(selected, start=1):
        if covariance is None:
            pvalues[name] = 1.0
            continue
        variance = float(covariance[offset, offset])
        if variance <= 0.0 or not math.isfinite(variance):
            pvalues[name] = 1.0
            continue
        statistic = (coefficients[name] ** 2) / variance
        pvalues[name] = _chisq_sf_1df(statistic)
    return pvalues


def _fitted_probability(
    fit_rows: DataFrame, model: LogisticRegressionModel, features: list[str]
) -> DataFrame:
    return model.transform(_assemble(fit_rows, features)).withColumn(
        _FITTED_PROB, _probability_of_event()
    )


def _stepwise_select(
    fit_rows: DataFrame, event_rate: float, options: ModelOptions
) -> tuple[list[str], list[str]]:
    """SAS ``selection=stepwise``: forward entry on the score test, backward
    elimination on the Wald test, until neither moves.

    Mirrors SAS in three details that matter: the null model is intercept-only,
    the variable that has just entered is not eligible for removal in the same
    step (SAS's cycle guard), and ties keep the earlier MODEL-statement variable
    because candidates are scanned in MODEL order with a strict comparison.
    """
    steps: list[str] = []
    selected: list[str] = []
    current = fit_rows.withColumn(_FITTED_PROB, F.lit(event_rate))
    model: LogisticRegressionModel | None = None
    step_number = 0

    while step_number < options.max_steps:
        step_number += 1
        remaining = [name for name in MODEL_PREDICTORS if name not in selected]
        if not remaining:
            steps.append(f"step {step_number}: every predictor is in the model")
            break

        information, score = _information_and_score(current, F.col(_FITTED_PROB))
        positions = [MODEL_PREDICTORS.index(name) + 1 for name in selected]
        pvalues = {
            name: _score_test(
                information, score, positions, MODEL_PREDICTORS.index(name) + 1
            )
            for name in remaining
        }
        entrant = min(remaining, key=lambda name: (pvalues[name],))
        if pvalues[entrant] >= options.slentry:
            steps.append(
                f"step {step_number}: no variable met slentry={options.slentry} "
                f"(best {entrant} p={pvalues[entrant]:.4f}); selection stopped"
            )
            break

        selected.append(entrant)
        steps.append(
            f"step {step_number}: {entrant} entered "
            f"(score test p={pvalues[entrant]:.4f} < slentry={options.slentry})"
        )
        model = _fit(fit_rows, selected, options)
        current = _fitted_probability(fit_rows, model, selected)

        removed = _backward_pass(
            fit_rows, selected, model, current, entrant, options, steps, step_number
        )
        if removed is not None:
            selected, model, current = removed

    else:  # pragma: no cover - guard, unreachable for 9 predictors
        steps.append(
            f"step limit {options.max_steps} reached; selection stopped early"
        )

    return selected, steps


def _backward_pass(
    fit_rows: DataFrame,
    selected: list[str],
    model: LogisticRegressionModel,
    current: DataFrame,
    just_entered: str,
    options: ModelOptions,
    steps: list[str],
    step_number: int,
) -> tuple[list[str], LogisticRegressionModel, DataFrame] | None:
    """Remove variables whose Wald p-value exceeds ``slstay``, worst first."""
    changed = False
    while len(selected) > 1:
        coefficients = dict(
            zip(selected, (float(v) for v in model.coefficients.toArray()))
        )
        information, _ = _information_and_score(current, F.col(_FITTED_PROB))
        pvalues = _wald_pvalues(information, selected, coefficients)
        eligible = [name for name in selected if name != just_entered]
        if not eligible:
            break
        worst = max(eligible, key=lambda name: (pvalues[name],))
        if pvalues[worst] <= options.slstay:
            break
        selected = [name for name in selected if name != worst]
        steps.append(
            f"step {step_number}: {worst} removed "
            f"(Wald p={pvalues[worst]:.4f} > slstay={options.slstay})"
        )
        model = _fit(fit_rows, selected, options)
        current = _fitted_probability(fit_rows, model, selected)
        changed = True
    return (selected, model, current) if changed else None
