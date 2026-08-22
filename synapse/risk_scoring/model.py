"""Logistic regression with explicit stepwise selection.

PROC LOGISTIC ... / selection=stepwise slentry=0.10 slstay=0.05 has no direct
equivalent in Spark MLlib, so the selection loop is implemented here on top of
``pyspark.ml.classification.LogisticRegression`` (unregularised, to match the SAS
maximum-likelihood parameterisation). MLlib does not expose standard errors for
logistic models, so the Wald statistics that drive the selection are computed
from the observed information matrix X'WX (see ``wald_p_values``).

Each iteration:
  1. forward step - fit ``selected + [candidate]`` for every remaining candidate
     and enter the candidate with the smallest Wald p-value if it is < slentry;
  2. backward step - drop any already-selected variable whose p-value has risen
     to >= slstay.
The loop stops when neither step changes the variable set, exactly like the SAS
stepwise stopping rule.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)

LABEL_COL = "DEFAULT_FLAG"
FEATURES_COL = "_features"
PROBABILITY_COL = "_probability"


@dataclass
class StepwiseResult:
    """Everything needed to score new rows and to audit the fitted model."""

    selected_features: list[str]
    coefficients: dict[str, float]
    intercept: float
    p_values: dict[str, float]
    steps: list[dict[str, object]] = field(default_factory=list)
    base_rate: float = 0.0
    model: LogisticRegressionModel | None = None

    def as_audit_record(self, model_version: str) -> dict[str, object]:
        return {
            "model_version": model_version,
            "selected_features": self.selected_features,
            "coefficients": self.coefficients,
            "intercept": self.intercept,
            "p_values": self.p_values,
            "base_rate": self.base_rate,
            "selection": {"method": "stepwise", "steps": self.steps},
        }

    def to_json(self, model_version: str) -> str:
        return json.dumps(self.as_audit_record(model_version), indent=2, sort_keys=True)


def wald_p_values(
    df: DataFrame,
    features: Sequence[str],
    coefficients: Sequence[float],
    intercept: float,
) -> list[float]:
    """Two-sided Wald p-values for each coefficient (intercept excluded).

    Standard errors come from the inverse observed information matrix
    ``(X' W X)^-1`` with ``W = diag(p_i (1 - p_i))``, which is the same quantity
    PROC LOGISTIC reports as ``Standard Error``. The matrix is (k+1)x(k+1) for a
    handful of candidate variables, so it is aggregated in Spark and inverted on
    the driver.
    """

    theta = np.append(np.asarray(coefficients, dtype=float), float(intercept))
    columns = list(features)

    def seq_op(acc: np.ndarray, row) -> np.ndarray:
        x = np.append(np.asarray([row[c] for c in columns], dtype=float), 1.0)
        if not np.all(np.isfinite(x)):
            return acc
        p = 1.0 / (1.0 + math.exp(-float(np.dot(x, theta))))
        return acc + (p * (1.0 - p)) * np.outer(x, x)

    size = len(columns) + 1
    information = df.select(*columns).rdd.treeAggregate(
        np.zeros((size, size)), seq_op, lambda a, b: a + b
    )
    covariance = np.linalg.pinv(information)
    variances = np.diag(covariance)

    p_values: list[float] = []
    for index in range(len(columns)):
        variance = variances[index]
        if not np.isfinite(variance) or variance <= 0.0:
            p_values.append(1.0)
            continue
        z = theta[index] / math.sqrt(variance)
        p_values.append(math.erfc(abs(z) / math.sqrt(2.0)))
    return p_values


def _fit(df: DataFrame, features: Sequence[str]) -> tuple[LogisticRegressionModel, list[float]]:
    assembler = VectorAssembler(
        inputCols=list(features), outputCol=FEATURES_COL, handleInvalid="skip"
    )
    assembled = assembler.transform(df).select(FEATURES_COL, LABEL_COL)
    lr = LogisticRegression(
        featuresCol=FEATURES_COL,
        labelCol=LABEL_COL,
        probabilityCol=PROBABILITY_COL,
        regParam=0.0,
        elasticNetParam=0.0,
        fitIntercept=True,
        standardization=True,
        maxIter=100,
    )
    model = lr.fit(assembled)
    p_values = wald_p_values(df, features, list(model.coefficients), model.intercept)
    return model, p_values


def _candidate_p_value(df: DataFrame, features: Sequence[str], candidate: str) -> float | None:
    try:
        _, p_values = _fit(df, list(features) + [candidate])
    except Exception as exc:  # noqa: BLE001 - a degenerate candidate must not abort selection
        LOGGER.warning("Candidate %s could not be evaluated: %s", candidate, exc)
        return None
    return p_values[len(features)]


def fit_stepwise(
    df: DataFrame,
    candidate_features: Sequence[str],
    slentry: float = 0.10,
    slstay: float = 0.05,
    max_iterations: int = 50,
) -> StepwiseResult:
    """Fit the model, entering/removing variables per the SAS stepwise rule."""

    label_values = {row[0] for row in df.select(LABEL_COL).distinct().limit(3).collect()}
    base_rate = df.select(F.avg(F.col(LABEL_COL))).first()[0] or 0.0
    if len(label_values) < 2:
        return StepwiseResult(
            selected_features=[],
            coefficients={},
            intercept=0.0,
            p_values={},
            steps=[{"action": "abort", "reason": "target has a single class"}],
            base_rate=float(base_rate),
        )

    selected: list[str] = []
    remaining = [c for c in candidate_features if c not in selected]
    steps: list[dict[str, object]] = []

    for _ in range(max_iterations):
        changed = False

        scored = {
            candidate: p
            for candidate in remaining
            if (p := _candidate_p_value(df, selected, candidate)) is not None
        }
        if scored:
            best, best_p = min(scored.items(), key=lambda kv: kv[1])
            if best_p < slentry:
                selected.append(best)
                remaining.remove(best)
                steps.append({"action": "enter", "variable": best, "p_value": best_p})
                changed = True

        if selected:
            _, p_values = _fit(df, selected)
            current = dict(zip(selected, p_values))
            worst, worst_p = max(current.items(), key=lambda kv: kv[1])
            if worst_p >= slstay:
                selected.remove(worst)
                remaining.append(worst)
                steps.append({"action": "remove", "variable": worst, "p_value": worst_p})
                changed = True

        if not changed:
            break

    if not selected:
        return StepwiseResult(
            selected_features=[],
            coefficients={},
            intercept=0.0,
            p_values={},
            steps=steps or [{"action": "abort", "reason": "no variable met slentry"}],
            base_rate=float(base_rate),
        )

    model, p_values = _fit(df, selected)
    return StepwiseResult(
        selected_features=list(selected),
        coefficients=dict(zip(selected, [float(c) for c in model.coefficients])),
        intercept=float(model.intercept),
        p_values=dict(zip(selected, [float(p) for p in p_values])),
        steps=steps,
        base_rate=float(base_rate),
        model=model,
    )


def score_probability(
    df: DataFrame,
    result: StepwiseResult,
    output_col: str = "PROB_DEFAULT",
) -> DataFrame:
    """Attach the predicted probability of default to every row.

    When stepwise selection retained no variable the SAS model degenerates to the
    intercept-only model, i.e. the observed event rate.
    """

    if result.model is None:
        return df.withColumn(output_col, F.lit(float(result.base_rate)))

    assembler = VectorAssembler(
        inputCols=result.selected_features, outputCol=FEATURES_COL, handleInvalid="keep"
    )
    scored = result.model.transform(assembler.transform(df))
    extract_positive = F.udf(lambda v: float(v[1]), "double")
    probability = extract_positive(F.col(PROBABILITY_COL))
    return scored.withColumn(
        output_col,
        F.when(F.isnan(probability) | probability.isNull(), F.lit(0.0)).otherwise(probability),
    ).drop(FEATURES_COL, PROBABILITY_COL, "rawPrediction", "prediction")
