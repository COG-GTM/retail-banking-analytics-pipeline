"""Parity of the ported job against the legacy reference extract.

Divergences and their tolerances are justified in ``MIGRATION_NOTES.md``.
"""

from __future__ import annotations

import pytest

from common import schemas
from jobs.stg_customer_360 import transform_customer_360
from tests.regression.harness import compare_to_reference

pytestmark = pytest.mark.regression

#: ``AGE`` and ``TENURE_MONTHS`` are the only columns allowed to differ, by at most 1.
#: The BTEQ computes ``(CURRENT_DATE - DATE_OF_BIRTH)/365.25`` and ``MONTHS_BETWEEN``, both
#: truncated by the CAST; the reference extract was produced with calendar-difference
#: arithmetic (verified: every reference AGE equals the calendar year difference and every
#: reference TENURE_MONTHS equals the calendar month difference).
TOLERANCES = {"AGE": 1.0, "TENURE_MONTHS": 1.0}


@pytest.fixture(scope="module")
def parity(spark, run_date):
    from orchestration.sample_data import reference_output_io, sample_source_io

    source = sample_source_io(spark)
    reference = reference_output_io(spark)
    actual = transform_customer_360(
        source.read_spec(schemas.CUSTOMERS),
        source.read_spec(schemas.ADDRESSES),
        source.read_spec(schemas.ACCOUNTS),
        run_date=run_date,
    )
    return compare_to_reference(
        actual,
        reference.read_spec(schemas.STG_CUSTOMER_360),
        schemas.STG_CUSTOMER_360,
        tolerances=TOLERANCES,
    )


def test_row_population_matches_exactly(parity):
    assert parity.expected_rows == 478
    assert parity.actual_rows == parity.expected_rows
    assert (parity.missing_keys, parity.extra_keys) == (0, 0), parity.summary()


def test_every_column_matches_within_its_declared_tolerance(parity):
    parity.assert_parity()


def test_date_arithmetic_divergence_is_bounded_at_one(spark, run_date):
    """The tolerance above is only legitimate if the divergence really is +/-1 and only there."""

    from orchestration.sample_data import reference_output_io, sample_source_io

    source = sample_source_io(spark)
    strict = compare_to_reference(
        transform_customer_360(
            source.read_spec(schemas.CUSTOMERS),
            source.read_spec(schemas.ADDRESSES),
            source.read_spec(schemas.ACCOUNTS),
            run_date=run_date,
        ),
        reference_output_io(spark).read_spec(schemas.STG_CUSTOMER_360),
        schemas.STG_CUSTOMER_360,
    )
    assert {diff.column for diff in strict.column_diffs} == set(TOLERANCES)
    assert all(diff.max_abs_diff == 1.0 for diff in strict.column_diffs), strict.summary()
