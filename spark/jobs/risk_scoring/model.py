"""Logistic regression with stepwise selection - port of PROC LOGISTIC (SAS STEP 3).

SAS fits the model with `selection=stepwise slentry=0.10 slstay=0.05`. Spark MLlib has no
stepwise selection and exposes no coefficient p-values for logistic regression, so the model is
fitted on the driver with statsmodels (maximum likelihood, same as PROC LOGISTIC) over the
collected reconciliation cohort, and the resulting coefficients are applied back to the full
DataFrame as a plain Spark expression so scoring stays distributed.

Difference against SAS that is intentional and documented: PROC LOGISTIC uses the score
chi-square test to decide entry and the Wald test to decide removal, while this implementation
uses the Wald test for both. On well-conditioned data the two agree; where they do not, the
selected variable set is recorded per run in the model audit output.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
import statsmodels.api as sm
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import RiskScoringParams

LOGGER = logging.getLogger(__name__)

INTERCEPT = "INTERCEPT"


@dataclass(frozen=True)
class FittedModel:
    """Everything needed to score, audit and reproduce a scoring run."""

    selected_features: tuple[str, ...]
    intercept: float
    coefficients: dict[str, float]
    std_errors: dict[str, float]
    p_values: dict[str, float]
    n_observations: int
    n_events: int
    converged: bool
    log_likelihood: float
    selection_log: tuple[str, ...] = field(default=())

    def as_rows(self, model_version: str) -> list[dict[str, object]]:
        """Flatten to one row per term for the coefficient audit table."""
        rows = [
            {
                "MODEL_VERSION": model_version,
                "TERM": INTERCEPT,
                "COEFFICIENT": self.intercept,
                "STD_ERROR": self.std_errors.get(INTERCEPT),
                "P_VALUE": self.p_values.get(INTERCEPT),
                "SELECTED": True,
            }
        ]
        for term in self.selected_features:
            rows.append(
                {
                    "MODEL_VERSION": model_version,
                    "TERM": term,
                    "COEFFICIENT": self.coefficients[term],
                    "STD_ERROR": self.std_errors.get(term),
                    "P_VALUE": self.p_values.get(term),
                    "SELECTED": True,
                }
            )
        return rows


def _intercept_only(
    frame: pd.DataFrame,
    target: str,
    params: RiskScoringParams,
    reason: str,
) -> FittedModel:
    """Degenerate-cohort fallback: predict the observed event rate for every customer.

    PROC LOGISTIC aborts the step in this situation. Aborting the Synapse job would block the
    downstream publish even though the composite score - which carries the SAS business logic -
    does not depend on the model, so the run instead continues with an intercept-only model and
    records the reason in the model audit output.
    """
    if not params.allow_intercept_only_model:
        raise ValueError(f"Risk model cannot be fitted: {reason}")
    rate = float(frame[target].mean())
    bounded = min(max(rate, 1e-12), 1 - 1e-12)
    LOGGER.warning("Falling back to an intercept-only model (%s); PD=%.6f", reason, rate)
    return FittedModel(
        selected_features=(),
        intercept=float(np.log(bounded / (1 - bounded))),
        coefficients={},
        std_errors={},
        p_values={},
        n_observations=int(frame.shape[0]),
        n_events=int(frame[target].sum()),
        converged=True,
        log_likelihood=float("nan"),
        selection_log=(f"INTERCEPT_ONLY ({reason})",),
    )


def _fit(frame: pd.DataFrame, target: str, features: Sequence[str], max_iter: int):
    design = sm.add_constant(frame[list(features)], has_constant="add")
    return sm.Logit(frame[target], design).fit(disp=0, maxiter=max_iter)


def _p_values(
    frame: pd.DataFrame, target: str, features: Sequence[str], max_iter: int
) -> pd.Series:
    return _fit(frame, target, features, max_iter).pvalues


def _usable_candidates(frame: pd.DataFrame, candidates: Iterable[str]) -> list[str]:
    usable = []
    for name in candidates:
        if name not in frame.columns:
            LOGGER.warning("Candidate feature %s is absent from the input; skipped", name)
            continue
        column = frame[name]
        if column.notna().sum() == 0 or float(np.nanstd(column.to_numpy(dtype=float))) == 0.0:
            LOGGER.warning("Candidate feature %s is constant or all-null; skipped", name)
            continue
        usable.append(name)
    return usable


def stepwise_logistic(
    frame: pd.DataFrame,
    params: RiskScoringParams,
    target: str = "DEFAULT_FLAG",
) -> FittedModel:
    """Forward-selection / backward-elimination stepwise logistic regression."""
    candidates = _usable_candidates(frame, params.candidate_features)
    model_frame = frame[[target, *candidates]].dropna().astype(float)
    if model_frame.empty:
        raise ValueError("No complete observations available to fit the risk model")
    if model_frame[target].nunique() < 2:
        return _intercept_only(
            model_frame,
            target,
            params,
            f"target {target} has a single level",
        )

    selected: list[str] = []
    selection_log: list[str] = []
    remaining = list(candidates)

    while True:
        entered = None
        best_p = params.slentry
        for candidate in remaining:
            try:
                pvals = _p_values(model_frame, target, [*selected, candidate], params.max_iter)
            except Exception as exc:  # noqa: BLE001 - separation / singularity: candidate unusable
                LOGGER.warning("Skipping candidate %s: %s", candidate, exc)
                continue
            p_value = float(pvals[candidate])
            if p_value < best_p:
                best_p, entered = p_value, candidate
        if entered is None:
            break
        selected.append(entered)
        remaining.remove(entered)
        selection_log.append(f"ENTER {entered} (p={best_p:.6f} < slentry={params.slentry})")

        # Backward elimination of terms that lost significance after the entry.
        while len(selected) > 1:
            pvals = _p_values(model_frame, target, selected, params.max_iter)
            worst = max(selected, key=lambda term: float(pvals[term]))
            worst_p = float(pvals[worst])
            if worst_p <= params.slstay:
                break
            selected.remove(worst)
            remaining.append(worst)
            selection_log.append(f"REMOVE {worst} (p={worst_p:.6f} > slstay={params.slstay})")
            if worst == entered:
                break

    if not selected:
        return _intercept_only(
            model_frame,
            target,
            params,
            f"no candidate reached slentry={params.slentry}",
        )

    result = _fit(model_frame, target, selected, params.max_iter)
    coefficients = {term: float(result.params[term]) for term in selected}
    std_errors = {term: float(result.bse[term]) for term in [*selected, "const"]}
    p_values = {term: float(result.pvalues[term]) for term in [*selected, "const"]}
    std_errors[INTERCEPT] = std_errors.pop("const")
    p_values[INTERCEPT] = p_values.pop("const")

    return FittedModel(
        selected_features=tuple(selected),
        intercept=float(result.params["const"]),
        coefficients=coefficients,
        std_errors=std_errors,
        p_values=p_values,
        n_observations=int(model_frame.shape[0]),
        n_events=int(model_frame[target].sum()),
        converged=bool(result.mle_retvals.get("converged", False)),
        log_likelihood=float(result.llf),
        selection_log=tuple(selection_log),
    )


def fit_model(features: DataFrame, params: RiskScoringParams) -> FittedModel:
    """Collect the modelling columns to the driver and fit the stepwise logistic model."""
    columns = [c for c in ("DEFAULT_FLAG", *params.candidate_features) if c in features.columns]
    pdf = features.select(*columns).toPandas()
    return stepwise_logistic(pdf, params)


def probability_column(model: FittedModel) -> Column:
    """Predicted probability of default, NULL where a selected feature is missing.

    PROC LOGISTIC drops observations with missing covariates and emits a missing predicted
    value for them; SAS then applies `coalesce(PROB_DEFAULT, 0)`.
    """
    linear = F.lit(float(model.intercept))
    complete = F.lit(True)
    for term, coefficient in model.coefficients.items():
        column = F.col(term).cast("double")
        linear = linear + F.lit(float(coefficient)) * column
        complete = complete & column.isNotNull()
    return F.when(complete, F.lit(1.0) / (F.lit(1.0) + F.exp(-linear)))
