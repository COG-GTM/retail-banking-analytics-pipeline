"""Unit tests for the pure transforms of ``jobs/sas_customer_segments.py``."""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from common import schemas
from jobs.sas_customer_segments import (
    KMEANS_SEED,
    SEGMENT_LABELS,
    transform_cluster_labels,
    transform_cluster_profiles,
    transform_clusters,
    transform_customer_segments,
    transform_features,
)

pytestmark = pytest.mark.unit

RUN_DATE = date(2026, 4, 10)


def customer(customer_id: int, **overrides: object) -> dict[str, object]:
    """A staging row with every column the job reads populated with a benign default."""

    row: dict[str, object] = {
        "CUSTOMER_ID": customer_id,
        "AGE": 30,
        "TENURE_MONTHS": 24,
        "CUSTOMER_STATUS": "A",
        "SEGMENT_CODE": "MASS",
        "STATE_CODE": "TX",
        "NUM_ACCOUNTS": 2,
        "NUM_ACTIVE_ACCOUNTS": 2,
        "HAS_CHECKING": "Y",
        "HAS_SAVINGS": "N",
        "HAS_CREDIT": "N",
        "HAS_LOAN": "N",
        "TOTAL_BALANCE": 5000.00,
        "TOTAL_CREDIT_LIMIT": 0.00,
        "CREDIT_UTILIZATION_PCT": 0.00,
    }
    row.update(overrides)
    return row


def staging(make_df, rows: list[dict[str, object]]):
    return make_df(schemas.STG_CUSTOMER_360, rows)


def features_by_id(make_df, rows: list[dict[str, object]]) -> dict[int, Row]:
    result = transform_features(staging(make_df, rows)).collect()
    return {row["CUSTOMER_ID"]: row for row in result}


def segments_by_id(make_df, rows: list[dict[str, object]], **kwargs) -> dict[int, Row]:
    features = transform_features(staging(make_df, rows))
    output = transform_customer_segments(features, run_date=RUN_DATE, **kwargs).collect()
    return {row["CUSTOMER_ID"]: row for row in output}


# -- STEP 1: extract -----------------------------------------------------------------------


def test_only_active_customers_are_segmented(make_df):
    rows = [
        customer(1, CUSTOMER_STATUS="A"),
        customer(2, CUSTOMER_STATUS="I"),
        customer(3, CUSTOMER_STATUS="C"),
        customer(4, CUSTOMER_STATUS=None),
    ]
    assert set(features_by_id(make_df, rows)) == {1}


# -- STEP 2: feature engineering ------------------------------------------------------------


@pytest.mark.parametrize(
    ("flags", "expected_breadth"),
    [
        (("N", "N", "N", "N"), 0.00),
        (("Y", "N", "N", "N"), 0.25),
        (("Y", "Y", "N", "N"), 0.50),
        (("Y", "Y", "Y", "N"), 0.75),
        (("Y", "Y", "Y", "Y"), 1.00),
    ],
)
def test_product_breadth_is_the_mean_of_the_four_product_flags(make_df, flags, expected_breadth):
    checking, savings, credit, loan = flags
    rows = [
        customer(1, HAS_CHECKING=checking, HAS_SAVINGS=savings, HAS_CREDIT=credit, HAS_LOAN=loan)
    ]
    assert features_by_id(make_df, rows)[1]["PRODUCT_BREADTH"] == pytest.approx(expected_breadth)


def test_a_null_product_flag_counts_as_not_held(make_df):
    """``(HAS_CHECKING = 'Y')`` is 0, never missing, for a blank SAS character value."""

    rows = [customer(1, HAS_CHECKING=None, HAS_SAVINGS="Y", HAS_CREDIT=None, HAS_LOAN=None)]
    assert features_by_id(make_df, rows)[1]["PRODUCT_BREADTH"] == pytest.approx(0.25)


@pytest.mark.parametrize(
    ("tenure", "expected"),
    [
        (0, "NEW (<1yr)"),
        (11, "NEW (<1yr)"),
        (12, "DEVELOPING (1-3yr)"),
        (35, "DEVELOPING (1-3yr)"),
        (36, "ESTABLISHED (3-7yr)"),
        (83, "ESTABLISHED (3-7yr)"),
        (84, "LOYAL (7yr+)"),
        (500, "LOYAL (7yr+)"),
        (None, "NEW (<1yr)"),
    ],
)
def test_tenure_group_boundaries(make_df, tenure, expected):
    rows = [customer(1, TENURE_MONTHS=tenure)]
    assert features_by_id(make_df, rows)[1]["TENURE_GROUP"] == expected


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (18, "GEN_Z"),
        (24, "GEN_Z"),
        (25, "MILLENNIAL"),
        (40, "MILLENNIAL"),
        (41, "GEN_X"),
        (56, "GEN_X"),
        (57, "BOOMER"),
        (75, "BOOMER"),
        (76, "SILENT"),
        (None, "GEN_Z"),
    ],
)
def test_age_group_boundaries(make_df, age, expected):
    rows = [customer(1, AGE=age)]
    assert features_by_id(make_df, rows)[1]["AGE_GROUP"] == expected


@pytest.mark.parametrize(
    ("balance", "expected"),
    [
        (0.00, "LOW"),
        (999.99, "LOW"),
        (1000.00, "MODERATE"),
        (9999.99, "MODERATE"),
        (10000.00, "AFFLUENT"),
        (99999.99, "AFFLUENT"),
        (100000.00, "HIGH_NET_WORTH"),
        (None, "LOW"),
    ],
)
def test_balance_tier_boundaries(make_df, balance, expected):
    rows = [customer(1, TOTAL_BALANCE=balance)]
    assert features_by_id(make_df, rows)[1]["BALANCE_TIER"] == expected


@pytest.mark.parametrize(
    ("balance", "expected"),
    [(None, 0.0), (0.00, 0.0), (0.50, 0.0), (1.00, 0.0), (100.00, 4.605170186)],
)
def test_log_balance_is_the_natural_log_of_max_balance_1(make_df, balance, expected):
    rows = [customer(1, TOTAL_BALANCE=balance)]
    assert features_by_id(make_df, rows)[1]["LOG_BALANCE"] == pytest.approx(expected)


@pytest.mark.parametrize(
    ("num_accounts", "num_active", "expected"),
    [
        (4, 3, 0.75),
        (0, 0, 0.0),
        (None, 2, 2.0),
        (3, None, None),
        (None, None, None),
    ],
)
def test_acct_ratio_reproduces_sas_missing_semantics(make_df, num_accounts, num_active, expected):
    rows = [customer(1, NUM_ACCOUNTS=num_accounts, NUM_ACTIVE_ACCOUNTS=num_active)]
    value = features_by_id(make_df, rows)[1]["ACCT_RATIO"]
    if expected is None:
        assert value is None
    else:
        assert value == pytest.approx(expected)


def test_digital_adoption_score_is_a_hardcoded_placeholder(make_df):
    assert features_by_id(make_df, [customer(1)])[1]["DIGITAL_ADOPTION_SCORE"] == 0


# -- STEP 3 + 4: standardise and cluster ----------------------------------------------------


@pytest.fixture
def spread_features(make_df):
    rows = [
        customer(
            index,
            AGE=20 + index * 3,
            TENURE_MONTHS=index * 9,
            TOTAL_BALANCE=100.00 * (index + 1) ** 2,
            NUM_ACCOUNTS=4,
            NUM_ACTIVE_ACCOUNTS=index % 5,
            CREDIT_UTILIZATION_PCT=index * 5.0,
            HAS_CHECKING="Y" if index % 2 else "N",
            HAS_SAVINGS="Y" if index % 3 else "N",
        )
        for index in range(1, 21)
    ]
    return transform_features(staging(make_df, rows))


@pytest.fixture
def clustered(spread_features):
    return transform_clusters(spread_features)


def test_clustering_assigns_every_customer_to_one_of_five_clusters(clustered):
    assignments = {row["CUSTOMER_ID"]: row["CLUSTER"] for row in clustered.collect()}
    assert len(assignments) == 20
    assert set(assignments.values()) <= set(range(5))
    assert len(set(assignments.values())) == 5


def test_standardised_features_are_centred_and_scaled(clustered):
    stats = clustered.agg(
        F.avg("STD_LOG_BALANCE").alias("mean"), F.stddev_samp("STD_LOG_BALANCE").alias("std")
    ).collect()[0]
    assert stats["mean"] == pytest.approx(0.0, abs=1e-9)
    assert stats["std"] == pytest.approx(1.0, abs=1e-9)


def test_clustering_is_reproducible_for_a_pinned_seed(spread_features, clustered):
    rerun = transform_clusters(spread_features, seed=KMEANS_SEED)
    first = {row["CUSTOMER_ID"]: row["CLUSTER"] for row in clustered.collect()}
    second = {row["CUSTOMER_ID"]: row["CLUSTER"] for row in rerun.collect()}
    assert first == second


def test_a_customer_with_a_missing_feature_is_still_clustered(make_df):
    rows = [
        customer(index, TOTAL_BALANCE=1000.00 * index, NUM_ACTIVE_ACCOUNTS=index % 3)
        for index in range(1, 11)
    ]
    rows.append(customer(99, NUM_ACTIVE_ACCOUNTS=None, TOTAL_BALANCE=None))
    clustered = transform_clusters(transform_features(staging(make_df, rows)))

    incomplete = clustered.filter(F.col("CUSTOMER_ID") == 99).collect()
    assert len(incomplete) == 1
    assert incomplete[0]["CLUSTER"] in set(range(5))
    assert incomplete[0]["STD_ACCT_RATIO"] == pytest.approx(0.0, abs=1e-9)


# -- STEP 5: cluster profiles and labels ----------------------------------------------------


CLUSTERED_SCHEMA = (
    "CUSTOMER_ID long, CLUSTER int, STD_LOG_BALANCE double, STD_TENURE_MONTHS double, "
    "STD_PRODUCT_BREADTH double, STD_CREDIT_UTILIZATION_PCT double"
)


@pytest.fixture
def profile_frame(spark):
    """A ``CUST_CLUSTERED``-shaped frame: three clusters with known standardised averages."""

    return spark.createDataFrame(
        [
            (1, 0, 1.0, 0.0, 0.0, 0.0),
            (2, 0, 3.0, 2.0, 1.0, 1.0),
            (3, 1, -1.0, 0.0, 0.0, 0.0),
            (4, 2, 0.5, 1.0, 1.0, 1.0),
            (5, 2, 0.5, 1.0, 1.0, 1.0),
        ],
        schema=CLUSTERED_SCHEMA,
    )


def test_cluster_profiles_aggregate_the_standardised_features(profile_frame):
    profiles = {row["CLUSTER"]: row for row in transform_cluster_profiles(profile_frame).collect()}
    assert profiles[0]["N"] == 2
    assert profiles[0]["AVG_BALANCE"] == pytest.approx(2.0)
    assert profiles[0]["AVG_TENURE"] == pytest.approx(1.0)
    assert profiles[2]["AVG_BREADTH"] == pytest.approx(1.0)
    assert profiles[1]["AVG_CREDIT_UTIL"] == pytest.approx(0.0)


def test_labels_follow_descending_average_balance(profile_frame):
    labels = {
        row["CLUSTER"]: row["SEGMENT_NAME"]
        for row in transform_cluster_labels(profile_frame).collect()
    }
    assert labels == {0: "PREMIUM_WEALTH", 2: "ENGAGED_MAINSTREAM", 1: "GROWING_DIGITAL"}


def test_subsegment_id_is_always_the_literal_zero(profile_frame):
    assert {row["SUBSEGMENT_ID"] for row in transform_cluster_labels(profile_frame).collect()} == {
        0
    }


def test_tied_average_balances_are_broken_by_the_lowest_cluster_number(spark):
    tied = spark.createDataFrame(
        [
            (1, 7, 2.0, 0.0, 0.0, 0.0),
            (2, 3, 2.0, 0.0, 0.0, 0.0),
            (3, 5, 1.0, 0.0, 0.0, 0.0),
        ],
        schema=CLUSTERED_SCHEMA,
    )
    labels = {
        row["CLUSTER"]: row["SEGMENT_NAME"] for row in transform_cluster_labels(tied).collect()
    }
    assert labels == {
        3: "PREMIUM_WEALTH",
        7: "ENGAGED_MAINSTREAM",
        5: "GROWING_DIGITAL",
    }


def test_a_sixth_cluster_would_fall_into_the_sas_else_branch(spark):
    many = spark.createDataFrame(
        [(index, index, float(-index), 0.0, 0.0, 0.0) for index in range(6)],
        schema=CLUSTERED_SCHEMA,
    )
    labels = [
        row["SEGMENT_NAME"] for row in transform_cluster_labels(many).orderBy("CLUSTER").collect()
    ]
    assert labels == [*SEGMENT_LABELS, "VALUE_BASIC"]


# -- STEP 6: scores, flags and placeholders --------------------------------------------------


def scored_row(make_df, **overrides) -> Row:
    rows = [customer(index, TOTAL_BALANCE=100.00 * index) for index in range(2, 12)]
    rows.append(customer(1, **overrides))
    return segments_by_id(make_df, rows)[1]


def test_lifetime_value_and_engagement_scores(make_df):
    row = scored_row(
        make_df,
        TOTAL_BALANCE=17603.62,
        TENURE_MONTHS=157,
        NUM_ACCOUNTS=2,
        NUM_ACTIVE_ACCOUNTS=2,
        HAS_CHECKING="Y",
        HAS_LOAN="Y",
    )
    assert float(row["LIFETIME_VALUE_SCORE"]) == pytest.approx(7674.05)
    assert float(row["ENGAGEMENT_SCORE"]) == pytest.approx(100.00)
    assert float(row["PRODUCT_BREADTH_INDEX"]) == pytest.approx(50.00)


def test_scores_are_rounded_to_two_decimals(make_df):
    row = scored_row(make_df, NUM_ACCOUNTS=3, NUM_ACTIVE_ACCOUNTS=2)
    assert float(row["ENGAGEMENT_SCORE"]) == pytest.approx(66.67)


def test_engagement_score_is_null_when_acct_ratio_is_missing(make_df):
    row = scored_row(make_df, NUM_ACTIVE_ACCOUNTS=None)
    assert row["ENGAGEMENT_SCORE"] is None


@pytest.mark.parametrize(
    ("breadth_flags", "num_accounts", "num_active", "expected"),
    [
        (("Y", "N", "N", "N"), 4, 3, "Y"),
        (("Y", "N", "N", "N"), 4, 4, "Y"),
        (("Y", "Y", "N", "N"), 4, 4, "N"),
        (("Y", "N", "N", "N"), 4, 2, "N"),
        (("Y", "N", "N", "N"), 4, None, "N"),
    ],
)
def test_cross_sell_flag(make_df, breadth_flags, num_accounts, num_active, expected):
    checking, savings, credit, loan = breadth_flags
    row = scored_row(
        make_df,
        HAS_CHECKING=checking,
        HAS_SAVINGS=savings,
        HAS_CREDIT=credit,
        HAS_LOAN=loan,
        NUM_ACCOUNTS=num_accounts,
        NUM_ACTIVE_ACCOUNTS=num_active,
    )
    assert row["CROSS_SELL_FLAG"] == expected


@pytest.mark.parametrize(
    ("balance", "tenure", "expected"),
    [
        (5000.00, 24, "Y"),
        (5000.00, 12, "Y"),
        (5000.00, 11, "N"),
        (999.99, 24, "N"),
        (10000.00, 24, "N"),
    ],
)
def test_upsell_flag(make_df, balance, tenure, expected):
    row = scored_row(make_df, TOTAL_BALANCE=balance, TENURE_MONTHS=tenure)
    assert row["UPSELL_FLAG"] == expected


@pytest.mark.parametrize(
    ("num_accounts", "num_active", "tenure", "expected"),
    [
        (4, 1, 60, "Y"),
        (4, 1, 59, "N"),
        (4, 2, 60, "N"),
        (4, None, 60, "Y"),
        (4, None, 59, "N"),
        (4, 1, None, "N"),
    ],
)
def test_retention_risk_flag(make_df, num_accounts, num_active, tenure, expected):
    row = scored_row(
        make_df, NUM_ACCOUNTS=num_accounts, NUM_ACTIVE_ACCOUNTS=num_active, TENURE_MONTHS=tenure
    )
    assert row["RETENTION_RISK_FLAG"] == expected


def test_placeholder_columns_stay_placeholders(make_df):
    row = scored_row(make_df)
    assert row["CHANNEL_PREFERENCE"] == ""
    assert float(row["DIGITAL_ADOPTION_SCORE"]) == 0.00
    assert row["SUBSEGMENT_ID"] == 0
    assert row["MODEL_VERSION"] == "SEG_V3.2"


def test_effective_date_is_the_pinned_run_date(make_df):
    assert scored_row(make_df)["EFFECTIVE_DATE"] == RUN_DATE


def test_output_matches_the_ddl_contract(make_df):
    rows = [customer(index, TOTAL_BALANCE=1000.00 * index) for index in range(1, 11)]
    features = transform_features(staging(make_df, rows))
    output = transform_customer_segments(features, run_date=RUN_DATE)
    schemas.assert_schema(output, schemas.CUSTOMER_SEGMENTS)
    assert output.count() == 10


def test_load_ts_can_be_pinned(make_df, load_ts):
    rows = [customer(index) for index in range(1, 11)]
    features = transform_features(staging(make_df, rows))
    output = transform_customer_segments(features, run_date=RUN_DATE, load_ts=load_ts)
    stamps = {row["LOAD_TS"] for row in output.select("LOAD_TS").distinct().collect()}
    assert len(stamps) == 1
