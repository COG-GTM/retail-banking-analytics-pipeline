"""Unit tests for the stepwise logistic-regression wrapper."""

from __future__ import annotations

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)

from jobs import stepwise_logistic as sw

pytestmark = pytest.mark.unit

_SCHEMA = StructType([
    StructField("x1", DoubleType(), False),
    StructField("x2", DoubleType(), False),
    StructField("label", IntegerType(), False),
])


def _separable(spark):
    """x1 clearly predicts label; x2 is pure noise (same in both classes)."""
    rows = []
    for i in range(12):
        rows.append((float(1 + (i % 4)), float(i % 5), 0))       # low x1 -> 0
    for i in range(12):
        rows.append((float(15 + (i % 4)), float(i % 5), 1))      # high x1 -> 1
    return spark.createDataFrame(rows, schema=_SCHEMA)


def test_chi2_1df_sf_reference_values():
    assert sw.chi2_1df_sf(0.0) == 1.0
    assert sw.chi2_1df_sf(-1.0) == 1.0
    # chi-square 1df 95th percentile is 3.841459 -> upper-tail p ~= 0.05
    assert abs(sw.chi2_1df_sf(3.841459) - 0.05) < 1e-4


def test_selects_predictive_feature(spark):
    res = sw.stepwise_logistic(_separable(spark), ["x1", "x2"], "label")
    assert not res.fallback
    assert "x1" in res.selected_features


def test_probabilities_in_unit_interval(spark):
    res = sw.stepwise_logistic(_separable(spark), ["x1", "x2"], "label")
    probs = [r.prob_default for r in res.predictions.select("prob_default").collect()]
    assert len(probs) == 24
    assert all(0.0 <= p <= 1.0 for p in probs)


def test_single_class_falls_back_to_prior_mean(spark):
    rows = [(1.0, 0.0, 0), (2.0, 1.0, 0), (3.0, 2.0, 0)]
    df = spark.createDataFrame(rows, schema=_SCHEMA)
    res = sw.stepwise_logistic(df, ["x1", "x2"], "label")
    assert res.fallback
    assert res.selected_features == []
    probs = {r.prob_default for r in res.predictions.select("prob_default").collect()}
    assert probs == {0.0}   # prior mean of an all-zero target


def test_single_class_all_positive_prior_mean_one(spark):
    rows = [(1.0, 0.0, 1), (2.0, 1.0, 1)]
    df = spark.createDataFrame(rows, schema=_SCHEMA)
    res = sw.stepwise_logistic(df, ["x1", "x2"], "label")
    assert res.fallback
    probs = {r.prob_default for r in res.predictions.select("prob_default").collect()}
    assert probs == {1.0}


def test_noise_only_features_fall_back(spark):
    # Every feature value appears once with label 0 and once with label 1, so
    # the features carry zero information -> nothing meets slentry, selection is
    # empty, and prob_default is the (balanced) prior mean 0.5.
    rows = []
    for i in range(12):
        rows.append((float(i), float(i % 4), 0))
        rows.append((float(i), float(i % 4), 1))
    df = spark.createDataFrame(rows, schema=_SCHEMA)
    res = sw.stepwise_logistic(df, ["x1", "x2"], "label", slentry=0.10, slstay=0.05)
    assert res.fallback
    assert res.fallback_reason == "no feature met slentry"
    probs = {round(r.prob_default, 6) for r in res.predictions.select("prob_default").collect()}
    assert probs == {0.5}
