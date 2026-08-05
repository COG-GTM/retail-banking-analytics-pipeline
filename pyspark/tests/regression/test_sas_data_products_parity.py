"""Parity of the ported golden record against the legacy reference extract."""

from __future__ import annotations

import pytest

from common import schemas
from jobs.sas_data_products import transform_master_profile
from tests.regression.harness import compare_to_reference

pytestmark = pytest.mark.regression

#: No column needs a tolerance. Every column of ``CUSTOMER_MASTER_PROFILE`` is a projection,
#: rename or default of an upstream value, and the upstream values fed in here are the committed
#: reference products, so the derived columns are byte-identical. In particular the +/-1
#: age/tenure divergence of the reference extract lives in ``STG_CUSTOMER_360`` and is carried
#: through unchanged by this job, so it cannot show up in this comparison.
TOLERANCES: dict[str, float] = {}


def _actual(spark, run_date):
    from orchestration.sample_data import reference_output_io

    reference = reference_output_io(spark)
    return transform_master_profile(
        reference.read_spec(schemas.STG_CUSTOMER_360),
        reference.read_spec(schemas.CUSTOMER_SEGMENTS),
        reference.read_spec(schemas.TRANSACTION_ANALYTICS),
        reference.read_spec(schemas.CUSTOMER_RISK_SCORES),
        run_date=run_date,
    )


@pytest.fixture(scope="module")
def parity(spark, run_date):
    from orchestration.sample_data import reference_output_io

    return compare_to_reference(
        _actual(spark, run_date),
        reference_output_io(spark).read_spec(schemas.CUSTOMER_MASTER_PROFILE),
        schemas.CUSTOMER_MASTER_PROFILE,
        tolerances=TOLERANCES,
    )


def test_row_population_matches_exactly(parity):
    assert parity.expected_rows == 407
    assert parity.actual_rows == parity.expected_rows
    assert (parity.missing_keys, parity.extra_keys) == (0, 0), parity.summary()


def test_every_column_matches_within_its_declared_tolerance(parity):
    parity.assert_parity()


def test_no_column_diverges_at_all_without_a_tolerance(spark, run_date):
    """The empty tolerance map is only legitimate if a strict comparison also passes."""

    from orchestration.sample_data import reference_output_io

    strict = compare_to_reference(
        _actual(spark, run_date),
        reference_output_io(spark).read_spec(schemas.CUSTOMER_MASTER_PROFILE),
        schemas.CUSTOMER_MASTER_PROFILE,
    )

    assert {diff.column for diff in strict.column_diffs} == set(TOLERANCES), strict.summary()
