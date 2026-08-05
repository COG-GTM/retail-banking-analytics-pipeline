"""Parity of ``03_stg_risk_factors`` against the legacy reference extract.

Two columns cannot match the reference extract while staying faithful to the BTEQ; both
divergences are proved to be bounded and one-directional by
``test_declared_divergences_are_bounded_and_confined`` and
``test_reference_new_merchant_count_ignores_the_correlated_not_in`` below. Every other column of
all 478 rows matches exactly.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from common import schemas
from jobs.stg_risk_factors import NEW_MERCHANT_WINDOW_DAYS, transform_merchant_risk
from tests.regression.harness import compare_to_reference

pytestmark = pytest.mark.regression

#: ``MONTHS_SINCE_LAST_LATE``: the BTEQ takes ``CAST(MONTHS_BETWEEN(...) AS INTEGER)``, which
#: Teradata truncates, while the reference extract was produced with calendar month differences
#: (the same divergence the reference job declares for ``TENURE_MONTHS``). Because the always-true
#: on-time test means the value is always "months since the account opened", the port is exactly
#: one month lower than the reference whenever the day-of-month has not been reached yet.
#:
#: ``NEW_MERCHANT_CNT_30D``: the reference extract did not apply the correlated
#: ``NOT IN (SELECT DISTINCT MERCHANT_NAME ...)`` predicate at all - its value equals a plain
#: ``COUNT(DISTINCT MERCHANT_NAME)`` over the 30-day window for all 478 rows (asserted below).
#: The port implements the predicate as specified, so it is never higher than the reference; the
#: gap is the number of merchants the account had already used before the cut-off (at most 5 in
#: this extract).
TOLERANCES = {"MONTHS_SINCE_LAST_LATE": 1.0, "NEW_MERCHANT_CNT_30D": 5.0}


def _actual(spark, run_date):
    from jobs.stg_risk_factors import transform_risk_factors
    from orchestration.sample_data import sample_source_io

    source = sample_source_io(spark)
    return transform_risk_factors(
        source.read_spec(schemas.CUSTOMERS),
        source.read_spec(schemas.ACCOUNTS),
        source.read_spec(schemas.CUSTOMER_BUREAU_SCORES),
        source.read_spec(schemas.TRANSACTIONS),
        source.read_spec(schemas.TRANSACTION_TYPES),
        run_date=run_date,
    )


@pytest.fixture(scope="module")
def parity(spark, run_date):
    from orchestration.sample_data import reference_output_io

    return compare_to_reference(
        _actual(spark, run_date),
        reference_output_io(spark).read_spec(schemas.STG_RISK_FACTORS),
        schemas.STG_RISK_FACTORS,
        tolerances=TOLERANCES,
    )


def test_row_population_matches_exactly(parity):
    assert parity.expected_rows == 478
    assert parity.actual_rows == parity.expected_rows
    assert (parity.missing_keys, parity.extra_keys) == (0, 0), parity.summary()


def test_every_column_matches_within_its_declared_tolerance(parity):
    parity.assert_parity()


def test_declared_divergences_are_bounded_and_confined(spark, run_date):
    """The tolerances are only legitimate if nothing else differs and the gap is one-directional."""

    from orchestration.sample_data import reference_output_io

    actual = _actual(spark, run_date)
    expected = reference_output_io(spark).read_spec(schemas.STG_RISK_FACTORS)
    strict = compare_to_reference(actual, expected, schemas.STG_RISK_FACTORS)

    assert {diff.column for diff in strict.column_diffs} == set(TOLERANCES), strict.summary()

    joined = actual.alias("a").join(expected.alias("e"), "CUSTOMER_ID")
    bounds = joined.select(
        F.min(F.col("a.MONTHS_SINCE_LAST_LATE") - F.col("e.MONTHS_SINCE_LAST_LATE")).alias(
            "min_months"
        ),
        F.max(F.col("a.MONTHS_SINCE_LAST_LATE") - F.col("e.MONTHS_SINCE_LAST_LATE")).alias(
            "max_months"
        ),
        F.min(F.col("a.NEW_MERCHANT_CNT_30D") - F.col("e.NEW_MERCHANT_CNT_30D")).alias(
            "min_merchants"
        ),
        F.max(F.col("a.NEW_MERCHANT_CNT_30D") - F.col("e.NEW_MERCHANT_CNT_30D")).alias(
            "max_merchants"
        ),
    ).collect()[0]

    # truncation can only lose a month, never gain one
    assert (bounds["min_months"], bounds["max_months"]) == (-1, 0)
    # the extra NOT IN predicate can only remove merchants, never add them
    assert bounds["max_merchants"] == 0
    assert bounds["min_merchants"] >= -TOLERANCES["NEW_MERCHANT_CNT_30D"]


def test_reference_new_merchant_count_ignores_the_correlated_not_in(spark, run_date):
    """Proof for the ``NEW_MERCHANT_CNT_30D`` tolerance.

    Re-running the merchant aggregate with an empty "seen before" set - i.e. dropping the
    correlated ``NOT IN`` exactly as the reference engine did - reproduces the reference column
    for every one of the 478 rows.
    """

    from orchestration.sample_data import reference_output_io, sample_source_io

    source = sample_source_io(spark)
    transactions = source.read_spec(schemas.TRANSACTIONS)
    accounts = source.read_spec(schemas.ACCOUNTS)
    expected = reference_output_io(spark).read_spec(schemas.STG_RISK_FACTORS)

    no_predicate = transform_merchant_risk(
        transactions.filter(
            F.col("TRANSACTION_DATE") >= F.date_sub(F.lit(run_date), NEW_MERCHANT_WINDOW_DAYS)
        ),
        accounts,
        run_date=run_date,
    ).select("CUSTOMER_ID", "NEW_MERCH_30D")

    mismatches = (
        expected.select("CUSTOMER_ID", "NEW_MERCHANT_CNT_30D")
        .join(no_predicate, "CUSTOMER_ID", "left")
        .filter(F.coalesce(F.col("NEW_MERCH_30D"), F.lit(0)) != F.col("NEW_MERCHANT_CNT_30D"))
    )

    assert mismatches.count() == 0
