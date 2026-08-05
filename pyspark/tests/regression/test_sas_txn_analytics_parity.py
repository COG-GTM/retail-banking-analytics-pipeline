"""Parity of the ported transaction analytics job against the legacy reference extract.

Row population and 12 of the 18 compared columns match the reference exactly. The three
divergences below were each traced back to the reference generator before a tolerance was
declared; none of them is a difference in the ported business logic.

``SPEND_PERCENTILE`` (tolerance 1.0, all 500 rows differ)
    ``PROC RANK groups=100`` emits a *group number* in ``[0, 99]``:
    ``FLOOR(mean_rank * 100 / (n + 1))``. Every reference value instead equals
    ``rank / n * 100`` exactly (0.2, 0.4, ... 100.0 for the 500 distinct debit totals), i.e.
    the reference generator produced a continuous percentile, not a SAS rank group -- it even
    emits 100.0, which ``groups=100`` can never produce. The two scales differ by less than
    one group on every row (max |diff| = 1.0), and the ordering is identical: the reference
    percentile is monotonically non-decreasing in the group number. The SAS semantics are what
    is ported; see :func:`jobs.sas_txn_analytics.transform_spend_percentile`.

``TOP_SPEND_CATEGORY`` (excluded from the comparison, 243 of 500 rows differ)
    The SAS is ``max(TOP_MERCHANT_CATEGORY)``, the lexicographic maximum over the customer's
    accounts. Every reference value is instead the category of the customer's *first* staging
    row (verified: 0 of 500 mismatches against that rule), which is what an unordered
    "any value" aggregate returns. A string column cannot be given a numeric tolerance, so it
    is excluded here and :func:`test_top_spend_category_divergence_is_confined_to_the_reference`
    proves the divergence is exactly the ``MAX`` vs "first row" difference.

``AVG_TRANSACTION_SIZE`` / ``DIGITAL_TXN_PCT`` / ``INTEREST_INCOME`` / ``REVENUE_CONTRIBUTION``
(tolerance one cent, 2 + 4 + 3 + 3 of 500 rows, i.e. 12 of ~9,000 compared values)
    Half-cent values, where the last cent depends on floating-point noise that the reference
    generator happens to have and this job deliberately does not. Customer 283's average
    transaction size is ``16850.56 / 128``, exactly 131.645: rounded half-up by the DDL's
    ``DECIMAL(15,2)`` that is 131.65, while the reference accumulated its sum in floating point
    (16850.559999999998) and landed on 131.64. The job sums in exact decimal, because a
    parallel double summation would make the last cent depend on the order in which partitions
    are merged and the output would stop being reproducible; the derived arithmetic then
    continues in double the way SAS does.

    There is also no rounding rule that reproduces the reference: customer 19's interest is
    ``871.75 * 0.02`` = 17.435 and the reference rounded it *down* to 17.43, while customer
    102's ``6322.25 * 0.02`` = 126.445, whose nearest double is *above* the half-cent, was
    also rounded down, to 126.44. Whatever the missing generator did, it is not a rule the
    port can reproduce; the divergence is bounded by one cent.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from common import schemas
from jobs.sas_txn_analytics import transform_transaction_analytics
from orchestration.sample_data import reference_output_io
from tests.regression.harness import compare_to_reference

pytestmark = pytest.mark.regression

#: One cent, with slack for the comparison itself: the harness measures the difference of two
#: doubles, and ``131.65 - 131.64`` is 0.010000000000019327.
ONE_CENT = 0.0101

#: See the module docstring for the justification of every entry.
TOLERANCES = {
    "SPEND_PERCENTILE": 1.0,
    "AVG_TRANSACTION_SIZE": ONE_CENT,
    "DIGITAL_TXN_PCT": ONE_CENT,
    "INTEREST_INCOME": ONE_CENT,
    "REVENUE_CONTRIBUTION": ONE_CENT,
}
#: ``LOAD_TS`` is the run timestamp; ``TOP_SPEND_CATEGORY`` is a string column, so the harness
#: cannot express its bounded divergence as a tolerance.
IGNORED = ("LOAD_TS", "TOP_SPEND_CATEGORY")

EXPECTED_ROWS = 500
#: Rows differing per column when nothing is tolerated (see the module docstring).
EXPECTED_STRICT_MISMATCHES = {
    "SPEND_PERCENTILE": 500,
    "AVG_TRANSACTION_SIZE": 2,
    "DIGITAL_TXN_PCT": 4,
    "INTEREST_INCOME": 3,
    "REVENUE_CONTRIBUTION": 3,
    "TOP_SPEND_CATEGORY": 243,
}


@pytest.fixture(scope="module")
def actual(spark, run_date):
    reference = reference_output_io(spark)
    return transform_transaction_analytics(
        reference.read_spec(schemas.STG_TXN_SUMMARY), run_date=run_date
    ).persist()


@pytest.fixture(scope="module")
def expected(spark):
    return reference_output_io(spark).read_spec(schemas.TRANSACTION_ANALYTICS)


@pytest.fixture(scope="module")
def parity(actual, expected):
    return compare_to_reference(
        actual,
        expected,
        schemas.TRANSACTION_ANALYTICS,
        tolerances=TOLERANCES,
        ignore=IGNORED,
    )


def test_row_population_matches_exactly(parity):
    assert parity.expected_rows == EXPECTED_ROWS
    assert parity.actual_rows == parity.expected_rows
    assert (parity.missing_keys, parity.extra_keys) == (0, 0), parity.summary()


def test_every_column_matches_within_its_declared_tolerance(parity):
    parity.assert_parity()


def test_divergence_is_bounded_and_confined_to_the_declared_columns(actual, expected):
    """The tolerances are only legitimate if nothing else differs and the bounds are tight."""

    strict = compare_to_reference(
        actual, expected, schemas.TRANSACTION_ANALYTICS, ignore=("LOAD_TS",)
    )
    diffs = {diff.column: diff for diff in strict.column_diffs}
    assert set(diffs) == set(EXPECTED_STRICT_MISMATCHES), strict.summary()
    assert {
        column: diff.mismatches for column, diff in diffs.items()
    } == EXPECTED_STRICT_MISMATCHES, strict.summary()
    assert diffs["SPEND_PERCENTILE"].max_abs_diff == pytest.approx(1.0), strict.summary()
    for column in TOLERANCES.keys() - {"SPEND_PERCENTILE"}:
        assert diffs[column].max_abs_diff == pytest.approx(0.01), strict.summary()


def test_spend_percentile_preserves_the_reference_ordering(actual, expected):
    """The rank groups are a coarser scale of the same ranking, never a reordering."""

    joined = actual.alias("a").join(expected.alias("e"), "CUSTOMER_ID")
    ordered = joined.select(
        F.col("a.SPEND_PERCENTILE").alias("group"), F.col("e.SPEND_PERCENTILE").alias("ref")
    ).orderBy("ref")
    groups = [row["group"] for row in ordered.collect()]
    assert groups == sorted(groups)
    assert (min(groups), max(groups)) == (0, 99)


def test_top_spend_category_divergence_is_confined_to_the_reference(spark, actual, expected):
    """Ours is always the lexicographic max of the customer's categories; the reference is not."""

    reference = reference_output_io(spark)
    staging = reference.read_spec(schemas.STG_TXN_SUMMARY)
    categories = staging.groupBy("CUSTOMER_ID").agg(
        F.max("TOP_MERCHANT_CATEGORY").alias("LEXICOGRAPHIC_MAX"),
        F.collect_set("TOP_MERCHANT_CATEGORY").alias("ALL_CATEGORIES"),
    )
    joined = (
        actual.select("CUSTOMER_ID", F.col("TOP_SPEND_CATEGORY").alias("ACTUAL"))
        .join(
            expected.select("CUSTOMER_ID", F.col("TOP_SPEND_CATEGORY").alias("EXPECTED")),
            "CUSTOMER_ID",
        )
        .join(categories, "CUSTOMER_ID")
    )

    assert joined.filter(F.col("ACTUAL") != F.col("LEXICOGRAPHIC_MAX")).count() == 0
    mismatched = joined.filter(F.col("ACTUAL") != F.col("EXPECTED"))
    assert mismatched.count() == 243
    # Every reference value is one of the customer's own categories, and always a smaller one:
    # the reference kept an arbitrary row instead of the maximum.
    assert mismatched.filter(~F.array_contains("ALL_CATEGORIES", F.col("EXPECTED"))).count() == 0
    assert mismatched.filter(F.col("EXPECTED") >= F.col("ACTUAL")).count() == 0
