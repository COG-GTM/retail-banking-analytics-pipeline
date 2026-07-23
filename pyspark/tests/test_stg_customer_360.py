"""Unit tests for ``jobs/stg_customer_360`` seeded from the repo sample CSVs."""

from __future__ import annotations

import calendar
import datetime as dt
import math
from pathlib import Path

import pandas as pd
import pytest

from jobs import stg_customer_360
from tests._seed import seed_core_banking


def _bteq_age(run_date: dt.date, dob: dt.date) -> int:
    """BTEQ ``CAST((CURRENT_DATE - dob) / 365.25 AS SMALLINT)`` (round half-up)."""
    return math.floor((run_date - dob).days / 365.25 + 0.5)


def _spark_months_between(end: dt.date, start: dt.date) -> float:
    """Replicate Spark ``months_between`` (== Teradata ``MONTHS_BETWEEN``)."""
    month_diff = (end.year - start.year) * 12 + (end.month - start.month)
    end_eom = end.day == calendar.monthrange(end.year, end.month)[1]
    start_eom = start.day == calendar.monthrange(start.year, start.month)[1]
    if end.day == start.day or (end_eom and start_eom):
        return float(month_diff)
    return month_diff + (end.day - start.day) / 31.0


def _bteq_tenure(run_date: dt.date, since: dt.date) -> int:
    """BTEQ ``CAST(MONTHS_BETWEEN(CURRENT_DATE, since) AS INTEGER)``."""
    return math.floor(_spark_months_between(run_date, since) + 0.5)

REPO_ROOT = Path(__file__).resolve().parents[2]
PARITY_CSV = REPO_ROOT / "data" / "02_bteq_staging" / "stg_customer_360.csv"

EXPECTED_COLUMNS = [
    "customer_id", "first_name", "last_name", "date_of_birth", "age",
    "customer_since", "tenure_months", "customer_status", "segment_code",
    "branch_id", "primary_address", "city", "state_code", "zip_code",
    "num_accounts", "num_active_accounts", "has_checking", "has_savings",
    "has_credit", "has_loan", "total_balance", "total_credit_limit",
    "credit_utilization_pct", "load_ts",
]


@pytest.fixture()
def built(spark, cfg):
    seed_core_banking(spark, cfg)
    df = stg_customer_360.run(spark, cfg)
    return df


def test_output_schema(built):
    assert built.columns == EXPECTED_COLUMNS
    dtypes = dict(built.dtypes)
    assert dtypes["customer_id"] == "bigint"
    assert dtypes["age"] == "smallint"
    assert dtypes["tenure_months"] == "int"
    assert dtypes["num_accounts"] == "smallint"
    assert dtypes["total_balance"] == "decimal(18,2)"
    assert dtypes["credit_utilization_pct"] == "decimal(5,2)"
    assert dtypes["load_ts"] == "timestamp"


def test_customer_id_unique_and_not_null(built):
    total = built.count()
    assert total > 0
    assert built.select("customer_id").distinct().count() == total
    assert built.where("customer_id IS NULL").count() == 0


def test_only_active_or_inactive_customers(built, spark, cfg):
    statuses = {r[0] for r in built.select("customer_status").distinct().collect()}
    assert statuses.issubset({"A", "I"})
    src = spark.table(cfg.table(cfg.schema_core, "customers"))
    expected = src.where("customer_status IN ('A','I')").count()
    assert built.count() == expected


def test_has_flags_are_y_or_n(built):
    for col in ("has_checking", "has_savings", "has_credit", "has_loan"):
        vals = {r[0] for r in built.select(col).distinct().collect()}
        assert vals.issubset({"Y", "N"})


def test_derivations_against_parity_csv(built):
    """Compare the port against the legacy BTEQ output CSV (``data/02_...``).

    Roll-ups, flags and address are date-independent and must match exactly.
    Decimals match within a rounding tolerance. ``age``/``tenure_months`` are
    allowed to differ by at most 1 from the CSV: the sample CSV was produced by
    the out-of-scope DuckDB generator using ``date_diff('year'|'month', ...)``
    (calendar-boundary counts), whereas this port faithfully reproduces the
    Teradata BTEQ formulas ``(CURRENT_DATE - dob)/365.25`` and
    ``MONTHS_BETWEEN(CURRENT_DATE, customer_since)``.
    """
    got = built.toPandas().set_index("customer_id").sort_index()
    exp = pd.read_csv(PARITY_CSV).set_index("customer_id").sort_index()

    assert set(got.index) == set(exp.index)
    joined = got.join(exp, lsuffix="_got", rsuffix="_exp")

    # Date-independent derivations must match the legacy output exactly.
    for col in ("num_accounts", "num_active_accounts"):
        mismatch = (joined[f"{col}_got"].astype(float) != joined[f"{col}_exp"].astype(float)).sum()
        assert mismatch == 0, f"{col}: {mismatch} mismatched rows vs parity CSV"
    for col in ("has_checking", "has_savings", "has_credit", "has_loan",
                "primary_address", "city", "state_code", "zip_code"):
        mismatch = (joined[f"{col}_got"].astype(str) != joined[f"{col}_exp"].astype(str)).sum()
        assert mismatch == 0, f"{col}: {mismatch} mismatched rows vs parity CSV"

    # Decimal derivations match within rounding tolerance.
    for col in ("total_balance", "total_credit_limit", "credit_utilization_pct"):
        diff = (joined[f"{col}_got"].astype(float) - joined[f"{col}_exp"].astype(float)).abs()
        assert diff.max() <= 0.01, f"{col}: max abs diff {diff.max()} vs parity CSV"

    # age/tenure: same value modulo the date-function semantics noted above.
    for col in ("age", "tenure_months"):
        diff = (joined[f"{col}_got"].astype(float) - joined[f"{col}_exp"].astype(float)).abs()
        assert diff.max() <= 1, f"{col}: max abs diff {diff.max()} vs parity CSV"


def test_age_tenure_reproduce_bteq_formula(built, spark, cfg):
    """age/tenure exactly follow the ported Teradata BTEQ formulas for run_date."""
    src = spark.table(cfg.table(cfg.schema_core, "customers")).select(
        "customer_id", "date_of_birth", "customer_since"
    ).toPandas().set_index("customer_id")
    got = built.select("customer_id", "age", "tenure_months").toPandas().set_index("customer_id")

    for cid, row in got.iterrows():
        dob = pd.Timestamp(src.loc[cid, "date_of_birth"]).date()
        since = pd.Timestamp(src.loc[cid, "customer_since"]).date()
        assert row["age"] == _bteq_age(cfg.run_date, dob), f"age mismatch cust {cid}"
        assert row["tenure_months"] == _bteq_tenure(cfg.run_date, since), f"tenure mismatch cust {cid}"


def test_specific_customer_derivations(built):
    row = built.where("customer_id = 1").first()
    assert row["age"] == 24
    assert row["tenure_months"] == 28
    assert row["primary_address"] == "6957 Tina Land"
    assert row["state_code"] == "NC"


def test_idempotent_rerun(built, spark, cfg):
    first = built.count()
    again = stg_customer_360.run(spark, cfg)
    assert again.count() == first
