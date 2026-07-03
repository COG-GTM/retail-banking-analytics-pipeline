"""Functional test: run STG_RISK_FACTORS end-to-end on the committed sample data."""

from __future__ import annotations

from decimal import Decimal

import pytest

from common import schemas
from common.io import LocalDataIO
from jobs import staging_risk_factors as job

pytestmark = pytest.mark.functional


@pytest.fixture(scope="module")
def result(spark, config, data_dir, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    io = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=lake, read_products_from_source=True)
    out = job.run(spark, io, config)
    return out, io, lake


def test_schema_matches_ddl_exactly(result):
    out, _, _ = result
    schemas.assert_schema(out, schemas.STG_RISK_FACTORS)


def test_primary_key_unique(result):
    out, _, _ = result
    assert out.count() > 0
    assert out.count() == out.select("customer_id").distinct().count()


def test_credit_util_ratio_in_range(result):
    out, _, _ = result
    assert out.filter(out.credit_util_ratio < 0).count() == 0
    # decimal(5,4) tops out below 10; utilisation should be a small non-negative ratio.
    assert out.filter(out.credit_util_ratio > Decimal("5.0000")).count() == 0


def test_payment_ontime_pct_in_range(result):
    out, _, _ = result
    assert out.filter((out.payment_ontime_pct < 0) | (out.payment_ontime_pct > 100)).count() == 0


def test_non_negative_counts(result):
    out, _, _ = result
    for col in (
        "account_overdraft_cnt", "large_withdrawal_cnt", "payment_late_cnt",
        "new_merchant_cnt_30d", "international_txn_cnt", "high_risk_merchant_cnt",
        "external_credit_score",
    ):
        assert out.filter(out[col] < 0).count() == 0


def test_written_output_readable(result):
    out, io, _ = result
    read_back = io.read_staging("STG_RISK_FACTORS")
    assert read_back.count() == out.count()
    schemas.assert_schema(read_back, schemas.STG_RISK_FACTORS)
