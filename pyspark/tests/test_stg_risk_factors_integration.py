"""Integration tests: full ``run`` on the repo's seed CSVs + parity cross-checks.

Cross-checks against ``data/02_bteq_staging/stg_risk_factors.csv`` are limited to
the date-independent, deterministic columns (``credit_util_ratio``,
``external_credit_score``) plus the customer key set / row count, which reproduce
exactly regardless of the "as-of" run date.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from common.validation import ValidationError, validate_table
from jobs import stg_risk_factors as job

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
REFERENCE_CSV = REPO_ROOT / "data" / "02_bteq_staging" / "stg_risk_factors.csv"

EXPECTED_COLUMNS = [
    "customer_id",
    "account_overdraft_cnt",
    "nsf_fee_total",
    "large_withdrawal_cnt",
    "large_withdrawal_amt",
    "avg_daily_balance_30d",
    "avg_daily_balance_90d",
    "balance_volatility",
    "credit_util_ratio",
    "payment_ontime_pct",
    "payment_late_cnt",
    "months_since_last_late",
    "external_credit_score",
    "debit_velocity_7d",
    "debit_velocity_30d",
    "new_merchant_cnt_30d",
    "international_txn_cnt",
    "high_risk_merchant_cnt",
    "load_ts",
]


@pytest.fixture(scope="module")
def result_pdf(spark, seeded):
    cfg = seeded
    job.run(spark, cfg)
    # Re-run to assert idempotency (overwrite, not append).
    df = job.run(spark, cfg)
    return df.toPandas()


@pytest.fixture(scope="module")
def reference_pdf():
    return pd.read_csv(REFERENCE_CSV)


def test_schema_matches_ddl_order(result_pdf):
    assert list(result_pdf.columns) == EXPECTED_COLUMNS


def test_row_count_and_key_uniqueness(result_pdf, reference_pdf):
    assert len(result_pdf) == len(reference_pdf)
    assert result_pdf["customer_id"].is_unique
    assert result_pdf["customer_id"].notna().all()


def test_customer_id_set_matches_reference(result_pdf, reference_pdf):
    assert set(result_pdf["customer_id"]) == set(reference_pdf["customer_id"])


def test_value_ranges_are_valid(result_pdf):
    assert (result_pdf["credit_util_ratio"] >= 0).all()
    assert (result_pdf["payment_ontime_pct"].between(0, 100)).all()
    for col in [
        "account_overdraft_cnt",
        "large_withdrawal_cnt",
        "payment_late_cnt",
        "new_merchant_cnt_30d",
        "international_txn_cnt",
        "high_risk_merchant_cnt",
        "nsf_fee_total",
        "debit_velocity_7d",
        "debit_velocity_30d",
    ]:
        assert (result_pdf[col] >= 0).all(), col


def test_credit_util_matches_reference(result_pdf, reference_pdf):
    merged = result_pdf.merge(
        reference_pdf, on="customer_id", suffixes=("_out", "_ref")
    )
    out = merged["credit_util_ratio_out"].astype(float)
    ref = merged["credit_util_ratio_ref"].astype(float)
    assert (out - ref).abs().max() < 1e-4


def test_external_credit_score_matches_reference(result_pdf, reference_pdf):
    merged = result_pdf.merge(
        reference_pdf, on="customer_id", suffixes=("_out", "_ref")
    )
    assert (
        merged["external_credit_score_out"] == merged["external_credit_score_ref"]
    ).all()


def test_validation_rejects_duplicate_keys(spark):
    df = spark.createDataFrame(
        [(1,), (1,)], "customer_id BIGINT"
    )
    with pytest.raises(ValidationError):
        validate_table(df, unique_keys=["customer_id"])


def test_validation_rejects_nulls(spark):
    df = spark.createDataFrame(
        [(1,), (None,)], "customer_id BIGINT"
    )
    with pytest.raises(ValidationError):
        validate_table(df, not_null_cols=["customer_id"])
