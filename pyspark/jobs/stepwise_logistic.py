"""Stepwise logistic-regression selection wrapper.

PySpark analog of SAS ``PROC LOGISTIC ... selection=stepwise slentry= slstay=``.

Spark ML has no native stepwise selection, so this module implements the
classic forward/backward stepwise procedure on top of
:class:`pyspark.ml.classification.LogisticRegression`:

* **Forward step** -- among the not-yet-included candidates, tentatively add each
  one, refit, and keep the candidate whose entry most improves the fit; it is
  included only if its significance ``p < slentry``.
* **Backward step** -- after an entry, any already-included variable whose
  significance ``p > slstay`` is dropped (the least-significant first). The
  variable that just entered is never removed in the same pass, which guarantees
  termination (matching PROC LOGISTIC behaviour).

**Significance test (fidelity note).** ``PROC LOGISTIC`` uses Wald chi-square
statistics derived from coefficient standard errors. Spark ML's
:class:`LogisticRegressionSummary` does **not** expose
``coefficientStandardErrors`` for the classifier (unlike GLM), so per the task
the fallback is a **likelihood-ratio test (LRT)**: for a single added/removed
term the statistic ``G^2 = 2 * (loglik_full - loglik_reduced)`` is chi-square
with 1 df, and ``p = P(chi2_1 > G^2)``. Only :mod:`math` is used (no SciPy):
``P(chi2_1 > x) = erfc(sqrt(x/2))`` (equivalently ``2*(1 - Phi(sqrt(x)))``).

The output ``prob_default`` column is the model's predicted ``P(target = 1)``.
Degenerate cases (single-class target, empty selection, or a fit failure) fall
back to ``prob_default = coalesce(mean(target), 0)`` and are flagged on the
result. See ``MIGRATION_NOTES.md``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

from common.audit import get_logger

_LOG = get_logger("rbap.stepwise")

# Clamp for log-likelihood so log(0) never occurs.
_EPS = 1e-15

_ASSEMBLED_COL = "_stepwise_features"


def chi2_1df_sf(stat: float) -> float:
    """Upper-tail probability ``P(chi2_1 > stat)`` via the error function.

    For a chi-square with 1 df, ``P(X > x) = erfc(sqrt(x/2))`` which equals the
    two-sided normal tail ``2 * (1 - Phi(sqrt(x)))`` used for a Wald z. Uses only
    :mod:`math` so no SciPy dependency is introduced.
    """
    if stat <= 0.0:
        return 1.0
    return math.erfc(math.sqrt(stat / 2.0))


@dataclass
class StepwiseResult:
    """Outcome of :func:`stepwise_logistic`."""

    selected_features: list[str]
    predictions: DataFrame
    prob_col: str
    fallback: bool = False
    fallback_reason: str | None = None
    history: list[tuple[str, str, float]] = field(default_factory=list)
    model: object | None = None


def _positive_prob(vector) -> float | None:
    """Second element of a Spark ML probability vector: ``P(label = 1)``."""
    return None if vector is None else float(vector[1])


_positive_prob_udf = F.udf(_positive_prob, DoubleType())


def _prob_of_positive(prob_vector_col: str) -> F.Column:
    """Extract ``P(label = 1)`` (index 1) from a Spark ML probability vector."""
    return _positive_prob_udf(F.col(prob_vector_col))


def _fit(df: DataFrame, features: list[str], label_col: str):
    """Fit an unregularised binary logistic regression on ``features``."""
    assembler = VectorAssembler(
        inputCols=features, outputCol=_ASSEMBLED_COL, handleInvalid="keep"
    )
    assembled = assembler.transform(df)
    lr = LogisticRegression(
        featuresCol=_ASSEMBLED_COL,
        labelCol=label_col,
        regParam=0.0,
        elasticNetParam=0.0,
        fitIntercept=True,
        standardization=True,
        maxIter=100,
        tol=1e-6,
    )
    model = lr.fit(assembled)
    preds = model.transform(assembled)
    return model, preds


def _log_likelihood(preds: DataFrame, label_col: str, prob_vector_col: str = "probability") -> float:
    """Binomial log-likelihood of ``preds`` under the fitted probabilities."""
    p = _prob_of_positive(prob_vector_col)
    p = F.least(F.greatest(p, F.lit(_EPS)), F.lit(1.0 - _EPS))
    ll = F.sum(
        F.when(F.col(label_col) == 1, F.log(p)).otherwise(F.log(F.lit(1.0) - p))
    ).alias("ll")
    value = preds.select(ll).first()["ll"]
    return float(value) if value is not None else 0.0


def _null_log_likelihood(mean_y: float, n: int, n_pos: int) -> float:
    """Log-likelihood of the intercept-only model (predicts the base rate)."""
    p = min(max(mean_y, _EPS), 1.0 - _EPS)
    return n_pos * math.log(p) + (n - n_pos) * math.log(1.0 - p)


def stepwise_logistic(
    df: DataFrame,
    features: list[str],
    label_col: str,
    slentry: float = 0.10,
    slstay: float = 0.05,
    prob_col: str = "prob_default",
) -> StepwiseResult:
    """Forward/backward stepwise logistic regression.

    ``features`` are offered to selection in the given order (the order only
    affects deterministic tie-breaks). Returns a :class:`StepwiseResult` whose
    ``predictions`` DataFrame is ``df`` plus a ``prob_col`` column of
    ``P(label = 1)``.
    """
    df = df.cache()
    stats = df.select(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col(label_col).cast("double")).alias("s"),
        F.countDistinct(F.col(label_col)).alias("k"),
    ).first()
    n = int(stats["n"])
    n_pos = int(stats["s"] or 0)
    n_classes = int(stats["k"] or 0)
    mean_y = (n_pos / n) if n else 0.0

    def _fallback(reason: str) -> StepwiseResult:
        _LOG.info("stepwise fallback: %s (prob_default=%.6f)", reason, mean_y)
        preds = df.withColumn(prob_col, F.lit(float(mean_y)))
        return StepwiseResult([], preds, prob_col, fallback=True, fallback_reason=reason)

    # Guard: a single-class target cannot train a logistic model.
    if n_classes < 2:
        return _fallback("target has a single class")

    included: list[str] = []
    history: list[tuple[str, str, float]] = []
    ll_current = _null_log_likelihood(mean_y, n, n_pos)
    max_steps = 4 * len(features) + 4

    try:
        for _ in range(max_steps):
            remaining = [f for f in features if f not in included]

            # -- forward: best entry candidate by LRT p-value ---------------- #
            best_feature: str | None = None
            best_p = 1.0
            best_ll = ll_current
            for cand in remaining:
                _, preds = _fit(df, included + [cand], label_col)
                ll = _log_likelihood(preds, label_col)
                p = chi2_1df_sf(2.0 * (ll - ll_current))
                if best_feature is None or p < best_p:
                    best_feature, best_p, best_ll = cand, p, ll

            just_added: str | None = None
            if best_feature is not None and best_p < slentry:
                included.append(best_feature)
                ll_current = best_ll
                just_added = best_feature
                history.append(("add", best_feature, best_p))

            # -- backward: drop the least-significant staying variable ------- #
            removed = False
            if included:
                worst_feature: str | None = None
                worst_p = 0.0
                worst_ll = ll_current
                for feat in included:
                    if feat == just_added:
                        continue
                    reduced = [f for f in included if f != feat]
                    if reduced:
                        _, preds_r = _fit(df, reduced, label_col)
                        ll_r = _log_likelihood(preds_r, label_col)
                    else:
                        ll_r = _null_log_likelihood(mean_y, n, n_pos)
                    p = chi2_1df_sf(2.0 * (ll_current - ll_r))
                    if worst_feature is None or p > worst_p:
                        worst_feature, worst_p, worst_ll = feat, p, ll_r
                if worst_feature is not None and worst_p > slstay:
                    included.remove(worst_feature)
                    ll_current = worst_ll
                    removed = True
                    history.append(("remove", worst_feature, worst_p))

            if just_added is None and not removed:
                break

        if not included:
            return _fallback("no feature met slentry")

        model, preds = _fit(df, included, label_col)
        out = preds.withColumn(prob_col, _prob_of_positive("probability")).drop(
            _ASSEMBLED_COL, "rawPrediction", "probability", "prediction"
        )
        _LOG.info("stepwise selected features: %s", included)
        return StepwiseResult(
            selected_features=list(included),
            predictions=out,
            prob_col=prob_col,
            history=history,
            model=model,
        )
    except Exception as exc:  # pragma: no cover - defensive convergence guard
        return _fallback(f"model fit failed: {type(exc).__name__}: {exc}")
