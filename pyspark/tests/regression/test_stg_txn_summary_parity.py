"""Parity of the ported job against the legacy reference extract.

This job derives nothing from a date of birth or a customer tenure, so the calendar-difference
divergence documented for ``stg_customer_360`` does not apply here and **no tolerance is
declared**: every column must match the reference output exactly.
"""

from __future__ import annotations

import pytest

from common import schemas
from jobs.stg_txn_summary import transform_txn_summary
from orchestration.sample_data import reference_output_io, sample_source_io
from tests.regression.harness import compare_to_reference

pytestmark = pytest.mark.regression

#: No column is allowed to diverge from the reference extract.
TOLERANCES: dict[str, float] = {}


def _actual(spark, run_date, lookback_months):
    source = sample_source_io(spark)
    return transform_txn_summary(
        source.read_spec(schemas.TRANSACTIONS),
        source.read_spec(schemas.TRANSACTION_TYPES),
        source.read_spec(schemas.ACCOUNTS),
        run_date=run_date,
        lookback_months=lookback_months,
    )


@pytest.fixture
def parity(spark, run_date, config):
    return compare_to_reference(
        _actual(spark, run_date, config.lookback_months),
        reference_output_io(spark).read_spec(schemas.STG_TXN_SUMMARY),
        schemas.STG_TXN_SUMMARY,
        tolerances=TOLERANCES,
    )


def test_row_population_matches_exactly(parity):
    assert parity.expected_rows == 1251
    assert parity.actual_rows == parity.expected_rows
    assert (parity.missing_keys, parity.extra_keys) == (0, 0), parity.summary()


def test_every_column_matches_within_its_declared_tolerance(parity):
    parity.assert_parity()


def test_the_divergence_is_confined_to_the_declared_tolerances(spark, run_date, config):
    """The tolerance set is empty, so a strict comparison must be clean as well."""

    strict = compare_to_reference(
        _actual(spark, run_date, config.lookback_months),
        reference_output_io(spark).read_spec(schemas.STG_TXN_SUMMARY),
        schemas.STG_TXN_SUMMARY,
    )

    assert {diff.column for diff in strict.column_diffs} == set(TOLERANCES), strict.summary()
