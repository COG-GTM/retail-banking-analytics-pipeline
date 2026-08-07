"""Model helpers replacing SAS/STAT procedures with ``pyspark.ml``.

============================================  ==================================
SAS/STAT                                      pyspark.ml
============================================  ==================================
``PROC STDIZE METHOD=STD``                    ``StandardScaler(withMean, withStd)``
``PROC FASTCLUS MAXCLUSTERS= MAXITER=``       ``KMeans(k, maxIter, tol)``
``PROC LOGISTIC ... SELECTION=STEPWISE``      :func:`stepwise_logistic`
============================================  ==================================
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURES_COL = "_features"
SCALED_COL = "_scaled_features"


def standardize(df: DataFrame, feature_cols: list[str]) -> DataFrame:
    """``PROC STDIZE METHOD=STD``: centre and scale to unit sample std-dev.

    Adds a ``_scaled_features`` vector column; the raw feature columns are left
    untouched (unlike SAS, which overwrites them in place).
    """
    filled = df.select(
        "*",
        *[
            F.coalesce(F.col(c).cast("double"), F.lit(0.0)).alias(f"_num_{c}")
            for c in feature_cols
        ],
    )
    assembler = VectorAssembler(
        inputCols=[f"_num_{c}" for c in feature_cols], outputCol=FEATURES_COL
    )
    assembled = assembler.transform(filled)
    scaler = StandardScaler(
        inputCol=FEATURES_COL, outputCol=SCALED_COL, withMean=True, withStd=True
    )
    scaled = scaler.fit(assembled).transform(assembled)
    return scaled.drop(*[f"_num_{c}" for c in feature_cols])


def kmeans_cluster(
    df: DataFrame,
    k: int = 5,
    max_iter: int = 50,
    tol: float = 0.001,
    seed: int = 42,
    prediction_col: str = "CLUSTER",
) -> DataFrame:
    """``PROC FASTCLUS``: assign each row to one of ``k`` clusters."""
    model = KMeans(
        featuresCol=SCALED_COL,
        predictionCol=prediction_col,
        k=k,
        maxIter=max_iter,
        tol=tol,
        seed=seed,
    ).fit(df)
    return model.transform(df)


@dataclass
class LogisticFit:
    """Result of :func:`stepwise_logistic`."""

    selected_features: list[str]
    model: LogisticRegressionModel | None
    converged: bool
    note: str = ""


def _fit_lr(df: DataFrame, label_col: str) -> LogisticRegressionModel:
    return LogisticRegression(
        featuresCol=SCALED_COL,
        labelCol=label_col,
        maxIter=200,
        regParam=0.0,
        elasticNetParam=0.0,
        standardization=False,
        family="binomial",
    ).fit(df)


def _p_values(model: LogisticRegressionModel) -> list[float] | None:
    """Wald p-values per coefficient, or ``None`` when unavailable."""
    try:
        pvals = list(model.summary.pValues)
    except Exception:  # noqa: BLE001 - not exposed for regularized fits
        return None
    return pvals[:-1] if len(pvals) == len(model.coefficients) + 1 else pvals


def stepwise_logistic(
    df: DataFrame,
    feature_cols: list[str],
    label_col: str,
    slentry: float = 0.10,
    slstay: float = 0.05,
    max_steps: int = 20,
) -> LogisticFit:
    """``PROC LOGISTIC ... SELECTION=STEPWISE SLENTRY= SLSTAY=``.

    Forward selection with backward elimination on Wald p-values, mirroring the
    SAS defaults used in ``sas/03_sas_risk_scoring.sas``. Returns an unfitted
    result when the label has a single class (the demo dataset can produce
    that), so callers apply the same heuristic fallback the reference
    implementation uses.
    """
    classes = [r[0] for r in df.select(label_col).distinct().limit(3).collect()]
    if len(classes) < 2:
        return LogisticFit([], None, converged=False, note="single-class label")

    selected: list[str] = []
    remaining = list(feature_cols)

    for _ in range(max_steps):
        best_feature, best_p = None, None
        for candidate in remaining:
            trial = standardize(df, selected + [candidate])
            pvals = _p_values(_fit_lr(trial, label_col))
            if pvals is None:
                return _full_model(df, feature_cols, label_col, "p-values unavailable")
            p_candidate = pvals[len(selected)]
            if best_p is None or p_candidate < best_p:
                best_feature, best_p = candidate, p_candidate
        if best_feature is None or best_p is None or best_p >= slentry:
            break

        selected.append(best_feature)
        remaining.remove(best_feature)

        # Backward elimination of terms that lost significance.
        while len(selected) > 1:
            pvals = _p_values(_fit_lr(standardize(df, selected), label_col))
            if pvals is None:
                break
            worst_idx = max(range(len(selected)), key=lambda i: pvals[i])
            if pvals[worst_idx] <= slstay:
                break
            remaining.append(selected.pop(worst_idx))

    if not selected:
        return _full_model(df, feature_cols, label_col, "no feature met slentry")

    model = _fit_lr(standardize(df, selected), label_col)
    return LogisticFit(selected, model, converged=True)


def _full_model(
    df: DataFrame, feature_cols: list[str], label_col: str, note: str
) -> LogisticFit:
    """Fallback: fit on every candidate feature (SAS ``SELECTION=NONE``)."""
    model = _fit_lr(standardize(df, feature_cols), label_col)
    return LogisticFit(list(feature_cols), model, converged=True, note=note)


def predict_probability(
    df: DataFrame, fit: LogisticFit, label_col: str, output_col: str = "PROB_DEFAULT"
) -> DataFrame:
    """Score ``df`` with the fitted model, or apply the SAS-free fallback."""
    if fit.model is None:
        # Mirrors the reference implementation when the model cannot be fitted.
        return df.withColumn(output_col, F.col(label_col).cast("double") * 0.8 + 0.05)

    scored = fit.model.transform(standardize(df, fit.selected_features))
    return scored.withColumn(
        output_col, vector_to_array(F.col("probability"))[1]
    ).drop(FEATURES_COL, SCALED_COL, "rawPrediction", "probability", "prediction")
