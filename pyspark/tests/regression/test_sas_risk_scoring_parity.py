"""Parity of the ported risk scoring job against the legacy reference extract.

Two columns cannot match the reference extract, both for reasons outside the SAS source:

``PROBABILITY_OF_DEFAULT``
    ``PAYMENT_LATE_CNT`` is 0 for every customer in the committed extract, so ``DEFAULT_FLAG``
    has a single class and PROC LOGISTIC has nothing to fit. This port falls back to the
    intercept-only prediction, i.e. the base rate, which is exactly ``0.000000`` here. The
    reference engine emitted the constant ``0.050000`` for all 407 rows - a placeholder, not a
    fitted probability. The divergence is therefore a flat 0.05 on every row.

``PRIMARY_RISK_DRIVER`` / ``SECONDARY_RISK_DRIVER``
    The SAS loop initialises ``_max1``/``_max2`` to ``0`` and compares strictly (``>``); the
    reference engine used non-strict comparisons seeded below zero, which lets the fourth array
    element (``100 - BUREAU_SCORE_COMPONENT``, numerically identical to the first) overwrite the
    tie and pushes a zero-valued component into a driver slot. We follow the SAS source, and
    :func:`test_driver_divergence_is_exactly_the_reference_engines_comparison_rule` proves the
    difference is nothing but that comparison rule.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import schemas
from jobs.sas_risk_scoring import DRIVER_LABELS, transform_customer_risk_scores
from tests.regression.harness import compare_to_reference

pytestmark = pytest.mark.regression

#: Flat offset between the reference placeholder (0.05) and the base-rate fallback (0.0).
TOLERANCES = {"PROBABILITY_OF_DEFAULT": 0.05}

#: String columns, so the harness cannot express a tolerance for them; see the module docstring.
DRIVER_COLUMNS = ("PRIMARY_RISK_DRIVER", "SECONDARY_RISK_DRIVER")


def actual_output(spark, run_date) -> DataFrame:
    from orchestration.sample_data import reference_output_io

    reference = reference_output_io(spark)
    return transform_customer_risk_scores(
        reference.read_spec(schemas.STG_RISK_FACTORS),
        reference.read_spec(schemas.STG_CUSTOMER_360),
        run_date=run_date,
    )


def expected_output(spark) -> DataFrame:
    from orchestration.sample_data import reference_output_io

    return reference_output_io(spark).read_spec(schemas.CUSTOMER_RISK_SCORES)


@pytest.fixture(scope="module")
def parity(spark, run_date):
    return compare_to_reference(
        actual_output(spark, run_date),
        expected_output(spark),
        schemas.CUSTOMER_RISK_SCORES,
        tolerances=TOLERANCES,
        ignore=("LOAD_TS", *DRIVER_COLUMNS),
    )


@pytest.fixture(scope="module")
def strict_parity(spark, run_date):
    return compare_to_reference(
        actual_output(spark, run_date), expected_output(spark), schemas.CUSTOMER_RISK_SCORES
    )


def test_row_population_matches_exactly(parity):
    assert parity.expected_rows == 407
    assert parity.actual_rows == parity.expected_rows
    assert (parity.missing_keys, parity.extra_keys) == (0, 0), parity.summary()


def test_every_column_matches_within_its_declared_tolerance(parity):
    parity.assert_parity()


def test_divergence_is_confined_to_the_three_documented_columns(strict_parity):
    """No other column is allowed to hide behind the tolerance."""

    assert {diff.column for diff in strict_parity.column_diffs} == {
        "PROBABILITY_OF_DEFAULT",
        *DRIVER_COLUMNS,
    }


def test_probability_divergence_is_a_flat_five_hundredths(strict_parity):
    diff = next(d for d in strict_parity.column_diffs if d.column == "PROBABILITY_OF_DEFAULT")

    assert diff.mismatches == strict_parity.expected_rows
    assert diff.max_abs_diff == pytest.approx(0.05), strict_parity.summary()


def reference_engine_drivers(df: DataFrame) -> DataFrame:
    """The reference engine's driver rule: non-strict ``>=`` seeded below zero.

    Identical to :func:`jobs.sas_risk_scoring.transform_risk_drivers` except for the comparison
    operator and the seeds, so that the parity gap can be attributed to exactly that. The seed
    is any value below the clamp floor of ``0``; the arithmetic stays in DECIMAL because this
    replays the rule on the already-rounded output columns, where ``100 - BUREAU_SCORE_COMPONENT``
    must tie exactly with ``CREDIT_RISK_COMPONENT`` as it does on the unrounded doubles inside
    the job.
    """

    seed = F.lit(Decimal("-1.00"))
    components = (
        F.col("CREDIT_RISK_COMPONENT"),
        F.col("BEHAVIOUR_RISK_COMPONENT"),
        F.col("VELOCITY_RISK_COMPONENT"),
        F.lit(Decimal("100.00")) - F.col("BUREAU_SCORE_COMPONENT"),
    )
    state = (
        df.withColumn("_max1", seed)
        .withColumn("_max2", seed)
        .withColumn("PRIMARY_RISK_DRIVER", F.lit(""))
        .withColumn("SECONDARY_RISK_DRIVER", F.lit(""))
    )
    for component, label in zip(components, DRIVER_LABELS, strict=True):
        new_primary = component >= F.col("_max1")
        new_secondary = ~new_primary & (component >= F.col("_max2"))
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


def test_driver_divergence_is_exactly_the_reference_engines_comparison_rule(spark, run_date):
    """Re-deriving the drivers with ``>=`` reproduces the reference extract row for row.

    That pins the gap on the comparison rule alone: every other input to the driver loop (the
    four components, in the legacy array order) is already byte-identical.
    """

    expected = expected_output(spark)
    replayed = reference_engine_drivers(
        actual_output(spark, run_date).drop(*DRIVER_COLUMNS)
    ).select("CUSTOMER_ID", *DRIVER_COLUMNS)

    joined = replayed.alias("a").join(
        expected.select("CUSTOMER_ID", *DRIVER_COLUMNS).alias("e"), on="CUSTOMER_ID"
    )
    mismatches = joined.filter(
        (F.col("a.PRIMARY_RISK_DRIVER") != F.col("e.PRIMARY_RISK_DRIVER"))
        | (F.col("a.SECONDARY_RISK_DRIVER") != F.col("e.SECONDARY_RISK_DRIVER"))
    )

    assert joined.count() == 407
    assert mismatches.count() == 0, mismatches.take(3)


def test_ported_drivers_only_ever_relabel_within_the_legacy_vocabulary(spark, run_date):
    """The divergence is a relabelling, never a lost row or an invented driver."""

    actual = actual_output(spark, run_date)
    labels = {
        row["PRIMARY_RISK_DRIVER"]
        for row in actual.select("PRIMARY_RISK_DRIVER").distinct().collect()
    } | {
        row["SECONDARY_RISK_DRIVER"]
        for row in actual.select("SECONDARY_RISK_DRIVER").distinct().collect()
    }

    assert labels <= set(DRIVER_LABELS) | {""}
