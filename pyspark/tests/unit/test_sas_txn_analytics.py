"""Unit tests for the pure transforms of ``jobs/sas_txn_analytics.py``.

Every CASE branch, every default and every threshold boundary of
``sas/02_sas_txn_analytics.sas`` is exercised on small in-memory DataFrames.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import schemas
from common.schemas import assert_schema
from jobs.sas_txn_analytics import (
    MODEL_VERSION,
    RANK_GROUPS,
    reporting_period,
    transform_anomaly_flag,
    transform_customer_aggregates,
    transform_population_stats,
    transform_spend_percentile,
    transform_spend_trend,
    transform_transaction_analytics,
)

pytestmark = pytest.mark.unit

STAGING_DEFAULTS: dict[str, object] = {
    "ACCOUNT_TYPE": "CHECKING",
    "SUMMARY_PERIOD_START": "2025-04-10",
    "SUMMARY_PERIOD_END": "2026-04-10",
    "TXN_COUNT_TOTAL": 10,
    "AMT_TOTAL_DEBIT": 100.00,
    "AMT_TOTAL_CREDIT": 50.00,
    "AMT_TOTAL_FEES": 5.00,
    "TOP_MERCHANT_CATEGORY": "GROCERY",
    "PCT_WEB": 10.00,
    "PCT_MOBILE": 20.00,
    "DAYS_SINCE_LAST_TXN": 1,
}


@pytest.fixture
def staging(make_df):
    """Build ``STG_TXN_SUMMARY`` rows from overrides on top of a benign default row."""

    def _staging(*rows: dict[str, object]):
        return make_df(schemas.STG_TXN_SUMMARY, [{**STAGING_DEFAULTS, **row} for row in rows])

    return _staging


def _aggregate(staging_df) -> dict[int, dict[str, object]]:
    return {
        row["CUSTOMER_ID"]: row.asDict()
        for row in transform_customer_aggregates(staging_df).collect()
    }


def _customers(spark, rows: list[tuple[int, float | None]]):
    """A minimal customer-level frame for the ranking / anomaly transforms."""

    return spark.createDataFrame(rows, "CUSTOMER_ID long, TOTAL_DEBIT_AMT double")


# -- STEP 2: customer-level aggregation ------------------------------------------------


def test_total_accounts_counts_distinct_account_ids(staging):
    aggregated = _aggregate(
        staging(
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 10},
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 10},
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 11},
        )
    )
    assert aggregated[1]["TOTAL_ACCOUNTS"] == 2


def test_active_accounts_counts_accounts_at_or_below_the_30_day_threshold(staging):
    aggregated = _aggregate(
        staging(
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 10, "DAYS_SINCE_LAST_TXN": 30},
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 11, "DAYS_SINCE_LAST_TXN": 31},
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 12, "DAYS_SINCE_LAST_TXN": 0},
        )
    )
    assert aggregated[1]["TOTAL_ACCOUNTS"] == 3
    assert aggregated[1]["ACTIVE_ACCOUNTS"] == 2


def test_active_accounts_treats_a_missing_days_since_last_txn_as_active(staging):
    """SAS orders a missing numeric below every value, so ``missing <= 30`` is true."""

    aggregated = _aggregate(
        staging(
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 10, "DAYS_SINCE_LAST_TXN": None},
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 11, "DAYS_SINCE_LAST_TXN": 90},
        )
    )
    assert aggregated[1]["ACTIVE_ACCOUNTS"] == 1


def test_totals_and_net_cash_flow_are_sums_of_the_account_rows(staging):
    aggregated = _aggregate(
        staging(
            {
                "CUSTOMER_ID": 1,
                "ACCOUNT_ID": 10,
                "TXN_COUNT_TOTAL": 4,
                "AMT_TOTAL_DEBIT": 100.00,
                "AMT_TOTAL_CREDIT": 20.00,
                "AMT_TOTAL_FEES": 1.50,
            },
            {
                "CUSTOMER_ID": 1,
                "ACCOUNT_ID": 11,
                "TXN_COUNT_TOTAL": 6,
                "AMT_TOTAL_DEBIT": 200.00,
                "AMT_TOTAL_CREDIT": 380.00,
                "AMT_TOTAL_FEES": 2.50,
            },
        )
    )
    row = aggregated[1]
    assert row["TOTAL_TRANSACTIONS"] == 10
    assert row["TOTAL_DEBIT_AMT"] == pytest.approx(300.00)
    assert row["TOTAL_CREDIT_AMT"] == pytest.approx(400.00)
    assert row["NET_CASH_FLOW"] == pytest.approx(100.00)
    assert row["TOTAL_FEES"] == pytest.approx(4.00)
    # (100 + 20 + 200 + 380) / 10
    assert row["AVG_TRANSACTION_SIZE"] == pytest.approx(70.00)


def test_avg_transaction_size_and_digital_pct_fall_back_to_zero_without_transactions(staging):
    aggregated = _aggregate(staging({"CUSTOMER_ID": 1, "ACCOUNT_ID": 10, "TXN_COUNT_TOTAL": 0}))
    assert aggregated[1]["AVG_TRANSACTION_SIZE"] == 0
    assert aggregated[1]["DIGITAL_TXN_PCT"] == 0


def test_avg_transaction_size_drops_rows_where_either_amount_is_missing(staging):
    """``sum(AMT_TOTAL_DEBIT + AMT_TOTAL_CREDIT)``: the ``+`` propagates the missing."""

    aggregated = _aggregate(
        staging(
            {
                "CUSTOMER_ID": 1,
                "ACCOUNT_ID": 10,
                "TXN_COUNT_TOTAL": 4,
                "AMT_TOTAL_DEBIT": 100.00,
                "AMT_TOTAL_CREDIT": 60.00,
            },
            {
                "CUSTOMER_ID": 1,
                "ACCOUNT_ID": 11,
                "TXN_COUNT_TOTAL": 6,
                "AMT_TOTAL_DEBIT": 500.00,
                "AMT_TOTAL_CREDIT": None,
            },
        )
    )
    row = aggregated[1]
    # The second account contributes to TOTAL_DEBIT_AMT and to the divisor, but not to the
    # numerator: (100 + 60) / (4 + 6).
    assert row["TOTAL_DEBIT_AMT"] == pytest.approx(600.00)
    assert row["AVG_TRANSACTION_SIZE"] == pytest.approx(16.00)


def test_digital_txn_pct_is_weighted_by_transaction_count(staging):
    aggregated = _aggregate(
        staging(
            {
                "CUSTOMER_ID": 1,
                "ACCOUNT_ID": 10,
                "TXN_COUNT_TOTAL": 90,
                "PCT_WEB": 10.00,
                "PCT_MOBILE": 0.00,
            },
            {
                "CUSTOMER_ID": 1,
                "ACCOUNT_ID": 11,
                "TXN_COUNT_TOTAL": 10,
                "PCT_WEB": 50.00,
                "PCT_MOBILE": 50.00,
            },
        )
    )
    # (90 * 10 + 10 * 100) / 100 = 19, not the unweighted mean of 55.
    assert aggregated[1]["DIGITAL_TXN_PCT"] == pytest.approx(19.00)


def test_top_spend_category_is_the_lexicographic_max_not_the_largest_spend(staging):
    aggregated = _aggregate(
        staging(
            {
                "CUSTOMER_ID": 1,
                "ACCOUNT_ID": 10,
                "AMT_TOTAL_DEBIT": 10_000.00,
                "TOP_MERCHANT_CATEGORY": "AIRLINE",
            },
            {
                "CUSTOMER_ID": 1,
                "ACCOUNT_ID": 11,
                "AMT_TOTAL_DEBIT": 1.00,
                "TOP_MERCHANT_CATEGORY": "UTILITIES",
            },
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 12, "TOP_MERCHANT_CATEGORY": None},
        )
    )
    assert aggregated[1]["TOP_SPEND_CATEGORY"] == "UTILITIES"


def test_each_customer_is_aggregated_independently(staging):
    aggregated = _aggregate(
        staging(
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 10},
            {"CUSTOMER_ID": 2, "ACCOUNT_ID": 20},
        )
    )
    assert set(aggregated) == {1, 2}


# -- STEP 3: trend, revenue components and the initialised anomaly flag ----------------


@pytest.mark.parametrize(
    ("net_cash_flow", "expected"),
    [
        (100.01, "UP"),
        (100.00, "STABLE"),
        (0.00, "STABLE"),
        (-100.00, "STABLE"),
        (-100.01, "DOWN"),
    ],
)
def test_monthly_spend_trend_thresholds_are_strict(spark, net_cash_flow, expected):
    frame = spark.createDataFrame(
        [(1, net_cash_flow, 20.00, 0.00, 0.00)],
        "CUSTOMER_ID long, NET_CASH_FLOW double, AVG_TRANSACTION_SIZE double, "
        "TOTAL_FEES double, TOTAL_DEBIT_AMT double",
    )
    assert transform_spend_trend(frame).collect()[0]["MONTHLY_SPEND_TREND"] == expected


def test_revenue_components_and_initialised_anomaly_flag(spark):
    frame = spark.createDataFrame(
        [(1, 0.00, 0.00, 40.00, 1_000.00)],
        "CUSTOMER_ID long, NET_CASH_FLOW double, AVG_TRANSACTION_SIZE double, "
        "TOTAL_FEES double, TOTAL_DEBIT_AMT double",
    )
    row = transform_spend_trend(frame).collect()[0]
    assert row["FEE_INCOME"] == pytest.approx(40.00)
    assert row["INTEREST_INCOME"] == pytest.approx(20.00)
    assert row["REVENUE_CONTRIBUTION"] == pytest.approx(60.00)
    assert row["ANOMALY_FLAG"] == "N"


# -- STEP 4: PROC RANK groups=100 ------------------------------------------------------


def test_spend_percentile_uses_the_proc_rank_group_formula(spark):
    frame = _customers(spark, [(i, float(i)) for i in range(1, 6)])
    ranked = {
        row["CUSTOMER_ID"]: row["SPEND_PERCENTILE"]
        for row in transform_spend_percentile(frame, groups=4).collect()
    }
    # floor(rank * 4 / (5 + 1)) for ranks 1..5
    assert ranked == {1: 0, 2: 1, 3: 2, 4: 2, 5: 3}


def test_spend_percentile_spans_zero_to_groups_minus_one(spark):
    frame = _customers(spark, [(i, float(i)) for i in range(1, 201)])
    values = [
        row["SPEND_PERCENTILE"]
        for row in transform_spend_percentile(frame).orderBy("TOTAL_DEBIT_AMT").collect()
    ]
    assert (values[0], values[-1]) == (0, RANK_GROUPS - 1)
    assert values == sorted(values)


def test_tied_spend_values_share_one_group(spark):
    """``TIES=MEAN``: tied observations get the average rank, hence the same group."""

    frame = _customers(spark, [(1, 10.0), (2, 10.0), (3, 10.0), (4, 20.0), (5, 30.0)])
    ranked = {
        row["CUSTOMER_ID"]: row["SPEND_PERCENTILE"]
        for row in transform_spend_percentile(frame, groups=4).collect()
    }
    # mean rank of the tie is (1+2+3)/3 = 2 -> floor(2*4/6) = 1
    assert ranked[1] == ranked[2] == ranked[3] == 1
    assert ranked[4] == 2
    assert ranked[5] == 3


def test_spend_percentile_differs_from_ntile_which_splits_ties(spark):
    """Documents the divergence from the ``ntile(100) - 1`` shorthand."""

    frame = _customers(spark, [(i, float(i)) for i in range(1, 6)])
    ranked = transform_spend_percentile(frame, groups=4).withColumn(
        "NTILE", F.ntile(4).over(Window.orderBy("TOTAL_DEBIT_AMT")) - F.lit(1)
    )
    pairs = {
        row["CUSTOMER_ID"]: (row["SPEND_PERCENTILE"], row["NTILE"]) for row in ranked.collect()
    }
    assert pairs == {1: (0, 0), 2: (1, 0), 3: (2, 1), 4: (2, 2), 5: (3, 3)}


def test_missing_spend_gets_a_missing_group_and_is_excluded_from_the_population(spark):
    frame = _customers(spark, [(1, None), (2, 10.0), (3, 20.0)])
    ranked = {
        row["CUSTOMER_ID"]: row["SPEND_PERCENTILE"]
        for row in transform_spend_percentile(frame, groups=4).collect()
    }
    # n = 2 non-missing values: floor(1*4/3) = 1 and floor(2*4/3) = 2
    assert ranked == {1: None, 2: 1, 3: 2}


# -- STEP 5: PROC MEANS + IQR anomaly flag ---------------------------------------------


def test_population_stats_report_the_median_and_the_interquartile_range(spark):
    frame = _customers(spark, [(i, float(i)) for i in range(1, 6)])
    stats = transform_population_stats(frame).collect()[0]
    assert stats["_MEDIAN"] == pytest.approx(3.0)
    assert stats["_IQR"] == pytest.approx(2.0)
    assert stats["_N_NONMISSING"] == 5


def _flagged(spark, rows):
    frame = spark.createDataFrame(
        rows, "CUSTOMER_ID long, TOTAL_DEBIT_AMT double, ANOMALY_FLAG string"
    )
    stats = transform_population_stats(frame)
    return {
        row["CUSTOMER_ID"]: row["ANOMALY_FLAG"]
        for row in transform_anomaly_flag(frame, stats).collect()
    }


def test_anomaly_flag_is_set_strictly_above_median_plus_three_iqr(spark):
    # sorted values 1..6, 16, 16.01 -> median 4, IQR 6 - 2 = 4, cut-off 4 + 3 * 4 = 16
    flags = _flagged(
        spark,
        [(i, float(i), "N") for i in range(1, 7)] + [(7, 16.0, "N"), (8, 16.01, "N")],
    )
    assert flags[8] == "Y"
    assert flags[7] == "N"
    assert all(flags[i] == "N" for i in range(1, 7))


def test_anomaly_flag_stays_n_when_the_iqr_is_zero(spark):
    flags = _flagged(spark, [(1, 5.0, "N"), (2, 5.0, "N"), (3, 5.0, "N"), (4, 500.0, "N")])
    assert set(flags.values()) == {"N"}


def test_anomaly_flag_keeps_its_initial_value_for_a_missing_spend(spark):
    flags = _flagged(
        spark, [(1, 1.0, "N"), (2, 2.0, "N"), (3, 3.0, "N"), (4, 100.0, "N"), (5, None, "N")]
    )
    assert flags[5] == "N"
    assert flags[4] == "Y"


# -- reporting period and the composed transform ---------------------------------------


@pytest.mark.parametrize(
    ("run_date", "expected"),
    [
        (date(2026, 4, 10), "2026-04"),
        (date(2026, 1, 1), "2026-01"),
        (date(2025, 12, 31), "2025-12"),
    ],
)
def test_reporting_period_is_the_first_of_the_run_month(run_date, expected):
    assert reporting_period(run_date) == expected


def test_transform_transaction_analytics_matches_the_ddl_contract(staging, run_date, load_ts):
    output = transform_transaction_analytics(
        staging(
            {"CUSTOMER_ID": 1, "ACCOUNT_ID": 10},
            {"CUSTOMER_ID": 2, "ACCOUNT_ID": 20, "AMT_TOTAL_DEBIT": 900.00},
        ),
        run_date=run_date,
        load_ts=load_ts,
    )
    assert_schema(output, schemas.TRANSACTION_ANALYTICS)

    rows = {row["CUSTOMER_ID"]: row for row in output.collect()}
    assert set(rows) == {1, 2}
    assert rows[1]["REPORTING_PERIOD"] == "2026-04"
    assert rows[1]["MODEL_VERSION"] == MODEL_VERSION
    assert rows[1]["EFFECTIVE_DATE"] == run_date
    assert rows[1]["ANOMALY_FLAG"] == "N"
    assert rows[2]["TOTAL_DEBIT_AMT"] == Decimal("900.00")
    assert rows[2]["INTEREST_INCOME"] == Decimal("18.00")
    # Two customers: floor(rank * 100 / 3) for ranks 1 and 2.
    assert rows[1]["SPEND_PERCENTILE"] == Decimal("33.00")
    assert rows[2]["SPEND_PERCENTILE"] == Decimal("66.00")
