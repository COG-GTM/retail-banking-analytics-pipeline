"""Parity of the ported segmentation job against the legacy reference extract.

Three groups of columns cannot be compared exactly, each for a documented reason:

``SEGMENT_ID`` / ``SEGMENT_NAME``
    ``PROC FASTCLUS`` (``replace=full``, least-squares) and Spark's ``k-means||`` are different
    algorithms, so the cluster numbering - and therefore which customer lands in which of the
    five labels - is not comparable row for row. The *stable* properties of the labelling are
    asserted instead (see :func:`test_every_customer_is_labelled` and friends).

``CHANNEL_PREFERENCE``
    The legacy program writes the empty string (``'' as CHANNEL_PREFERENCE length=10``) and the
    port writes the empty string too, but a CSV cannot distinguish an empty field from NULL, so
    the reference column reads back as NULL. Asserted exactly in the functional tier instead.

``TENURE_GROUP`` / ``AGE_GROUP`` / ``UPSELL_FLAG``
    The SAS buckets are strict (``if TENURE_MONTHS < 12``, ``if AGE < 25`` ...) and are ported
    verbatim; the engine that produced the reference CSVs put a value that sits *exactly on* a
    boundary in the lower bucket instead (i.e. it compared with ``<=``). The divergence is
    therefore confined to the 5 customers whose ``TENURE_MONTHS`` is exactly 12/36/84 and the 23
    whose ``AGE`` is exactly 25/41/57/76, and is exactly one adjacent bucket - proved by
    :func:`test_bucket_divergence_is_confined_to_boundary_values`. That the *values* agree (only
    the comparison differs) is proved by ``LIFETIME_VALUE_SCORE`` matching exactly on those same
    rows, since it is computed from ``TENURE_MONTHS``.

Every other column matches exactly, with no tolerance.
"""

from __future__ import annotations

import math

import pytest
from pyspark.sql import functions as F

from common import schemas
from jobs.sas_customer_segments import (
    AGE_GROUP_DEFAULT,
    AGE_GROUPS,
    NUM_CLUSTERS,
    SEGMENT_LABELS,
    TENURE_GROUP_DEFAULT,
    TENURE_GROUPS,
    transform_customer_segments,
    transform_features,
)
from tests.regression.harness import compare_to_reference

pytestmark = pytest.mark.regression

#: Columns excluded from the exact comparison; see the module docstring for the reason of each.
EXCLUDED = (
    "LOAD_TS",
    "SEGMENT_ID",
    "SEGMENT_NAME",
    "CHANNEL_PREFERENCE",
    "TENURE_GROUP",
    "AGE_GROUP",
    "UPSELL_FLAG",
)
#: No column needs a numeric tolerance: the scores are reproduced exactly.
TOLERANCES: dict[str, float] = {}

EXPECTED_ROWS = 407
TENURE_BOUNDARIES = {threshold for threshold, _ in TENURE_GROUPS}
AGE_BOUNDARIES = {threshold for threshold, _ in AGE_GROUPS}
TENURE_GROUP_ORDER = [label for _, label in TENURE_GROUPS] + [TENURE_GROUP_DEFAULT]
AGE_GROUP_ORDER = [label for _, label in AGE_GROUPS] + [AGE_GROUP_DEFAULT]


@pytest.fixture(scope="module")
def staging(spark):
    from orchestration.sample_data import reference_output_io

    return reference_output_io(spark).read_spec(schemas.STG_CUSTOMER_360).persist()


@pytest.fixture(scope="module")
def expected(spark):
    from orchestration.sample_data import reference_output_io

    return reference_output_io(spark).read_spec(schemas.CUSTOMER_SEGMENTS).persist()


@pytest.fixture(scope="module")
def actual(spark, staging):
    from datetime import date

    return transform_customer_segments(
        transform_features(staging), run_date=date(2026, 4, 10)
    ).persist()


@pytest.fixture(scope="module")
def parity(actual, expected):
    return compare_to_reference(
        actual,
        expected,
        schemas.CUSTOMER_SEGMENTS,
        tolerances=TOLERANCES,
        ignore=EXCLUDED,
    )


@pytest.fixture(scope="module")
def strict(actual, expected):
    return compare_to_reference(actual, expected, schemas.CUSTOMER_SEGMENTS, ignore=("LOAD_TS",))


def test_row_population_matches_exactly(parity):
    assert parity.expected_rows == EXPECTED_ROWS
    assert parity.actual_rows == parity.expected_rows
    assert (parity.missing_keys, parity.extra_keys) == (0, 0), parity.summary()


def test_every_compared_column_matches_exactly(parity):
    parity.assert_parity()


def test_no_column_outside_the_documented_exclusions_diverges(strict):
    assert {diff.column for diff in strict.column_diffs} <= set(EXCLUDED), strict.summary()


def test_bucket_divergence_is_confined_to_boundary_values(actual, expected, staging):
    """The excluded bucket columns differ only where the value sits exactly on a boundary."""

    joined = (
        actual.select(
            "CUSTOMER_ID",
            F.col("TENURE_GROUP").alias("a_tenure_group"),
            F.col("AGE_GROUP").alias("a_age_group"),
            F.col("UPSELL_FLAG").alias("a_upsell"),
        )
        .join(
            expected.select(
                "CUSTOMER_ID",
                F.col("TENURE_GROUP").alias("e_tenure_group"),
                F.col("AGE_GROUP").alias("e_age_group"),
                F.col("UPSELL_FLAG").alias("e_upsell"),
            ),
            on="CUSTOMER_ID",
        )
        .join(staging.select("CUSTOMER_ID", "AGE", "TENURE_MONTHS"), on="CUSTOMER_ID")
        .persist()
    )

    tenure_diffs = joined.filter(F.col("a_tenure_group") != F.col("e_tenure_group")).collect()
    age_diffs = joined.filter(F.col("a_age_group") != F.col("e_age_group")).collect()
    upsell_diffs = joined.filter(F.col("a_upsell") != F.col("e_upsell")).collect()

    assert len(tenure_diffs) == 5
    assert len(age_diffs) == 23
    assert len(upsell_diffs) == 2

    for row in tenure_diffs:
        assert row["TENURE_MONTHS"] in TENURE_BOUNDARIES
        actual_rank = TENURE_GROUP_ORDER.index(row["a_tenure_group"])
        assert TENURE_GROUP_ORDER.index(row["e_tenure_group"]) == actual_rank - 1
    for row in age_diffs:
        assert row["AGE"] in AGE_BOUNDARIES
        actual_rank = AGE_GROUP_ORDER.index(row["a_age_group"])
        assert AGE_GROUP_ORDER.index(row["e_age_group"]) == actual_rank - 1
    for row in upsell_diffs:
        # 'NEW (<1yr)' is the only tenure group excluded from the upsell rule.
        assert row["TENURE_MONTHS"] == 12
        assert (row["a_upsell"], row["e_upsell"]) == ("Y", "N")

    joined.unpersist()


def test_every_customer_is_labelled_and_the_label_set_is_complete(actual):
    labels = actual.groupBy("SEGMENT_NAME").agg(F.count(F.lit(1)).alias("N")).collect()

    assert {row["SEGMENT_NAME"] for row in labels} == set(SEGMENT_LABELS)
    assert sum(row["N"] for row in labels) == EXPECTED_ROWS
    assert actual.filter(F.col("SEGMENT_NAME").isNull()).count() == 0


def test_cluster_sizes_are_in_a_sane_range(actual):
    sizes = [
        row["N"] for row in actual.groupBy("SEGMENT_ID").agg(F.count(F.lit(1)).alias("N")).collect()
    ]

    assert len(sizes) == NUM_CLUSTERS
    assert min(sizes) >= 0.02 * EXPECTED_ROWS
    assert max(sizes) <= 0.60 * EXPECTED_ROWS


def test_labels_are_ordered_by_descending_average_balance(actual, staging):
    log_balance = F.log(F.greatest(F.col("TOTAL_BALANCE").cast("double"), F.lit(1.0)))
    profile = (
        actual.select("CUSTOMER_ID", "SEGMENT_NAME")
        .join(staging.select("CUSTOMER_ID", log_balance.alias("LOG_BALANCE")), on="CUSTOMER_ID")
        .groupBy("SEGMENT_NAME")
        .agg(F.avg("LOG_BALANCE").alias("AVG_BALANCE"))
        .collect()
    )
    averages = {row["SEGMENT_NAME"]: row["AVG_BALANCE"] for row in profile}
    ordered = [averages[label] for label in SEGMENT_LABELS]

    assert ordered == sorted(ordered, reverse=True), averages


def test_chained_date_arithmetic_divergence_is_bounded(spark, expected, run_date):
    """Running on this migration's own staging output instead of the committed extract.

    The BTEQ port computes ``AGE`` and ``TENURE_MONTHS`` with truncated ``/365.25`` and
    ``MONTHS_BETWEEN`` arithmetic, which is +/-1 away from the calendar arithmetic used to
    produce the committed staging CSV. That +/-1 propagates here into ``LIFETIME_VALUE_SCORE``
    (linear in ``TENURE_MONTHS``) and into the two bucket columns; nothing else moves, and the
    score moves by at most one month of tenure.
    """

    from jobs.stg_customer_360 import transform_customer_360
    from orchestration.sample_data import sample_source_io

    source = sample_source_io(spark)
    chained_staging = transform_customer_360(
        source.read_spec(schemas.CUSTOMERS),
        source.read_spec(schemas.ADDRESSES),
        source.read_spec(schemas.ACCOUNTS),
        run_date=run_date,
    ).persist()
    chained = transform_customer_segments(transform_features(chained_staging), run_date=run_date)
    report = compare_to_reference(chained, expected, schemas.CUSTOMER_SEGMENTS, ignore=("LOAD_TS",))

    assert (report.missing_keys, report.extra_keys) == (0, 0), report.summary()
    assert {diff.column for diff in report.column_diffs} <= {
        *EXCLUDED,
        "LIFETIME_VALUE_SCORE",
    }, report.summary()

    # |dLTV| = 10 * LOG_BALANCE * PRODUCT_BREADTH * |dTENURE| <= 10 * ln(max balance) * 1
    max_balance = chained_staging.agg(F.max("TOTAL_BALANCE")).collect()[0][0]
    bound = 10 * math.log(max(float(max_balance), 1.0))
    for diff in report.column_diffs:
        if diff.column == "LIFETIME_VALUE_SCORE":
            assert diff.max_abs_diff is not None and diff.max_abs_diff <= bound, report.summary()

    chained_staging.unpersist()
