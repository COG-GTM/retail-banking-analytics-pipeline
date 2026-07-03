"""Functional test: run STG_TXN_SUMMARY end-to-end on the committed sample data."""

from __future__ import annotations

from decimal import Decimal

import pytest

from common import schemas
from common.io import LocalDataIO
from jobs import staging_txn_summary as job

pytestmark = pytest.mark.functional


@pytest.fixture(scope="module")
def result(spark, config, data_dir, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    io = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=lake)
    out = job.run(spark, io, config)
    return out, io, lake


def test_schema_matches_ddl_exactly(result):
    out, _, _ = result
    schemas.assert_schema(out, schemas.STG_TXN_SUMMARY)


def test_primary_key_unique(result):
    out, _, _ = result
    keyed = out.select("customer_id", "account_id")
    assert out.count() == keyed.distinct().count()


def test_period_matches_config(result, config):
    out, _, _ = result
    rows = out.select("summary_period_start", "summary_period_end").distinct().collect()
    assert len(rows) == 1
    assert rows[0].summary_period_start == config.lookback_start
    assert rows[0].summary_period_end == config.run_date


def test_percentages_within_bounds(result):
    from pyspark.sql import functions as F

    out, _, _ = result
    for col in ("pct_atm", "pct_pos", "pct_web", "pct_mobile"):
        bad = out.filter(
            (F.col(col) < F.lit(Decimal("0.00"))) | (F.col(col) > F.lit(Decimal("100.00")))
        ).count()
        assert bad == 0, f"{col} out of [0,100]"


def test_counts_are_consistent(result):
    from pyspark.sql import functions as F

    out, _, _ = result
    # debit + credit + fee counts never exceed the total (other categories may exist)
    bad = out.filter(
        (F.col("txn_count_debit") + F.col("txn_count_credit") + F.col("txn_count_fee"))
        > F.col("txn_count_total")
    ).count()
    assert bad == 0


def test_debit_amounts_non_negative(result):
    from pyspark.sql import functions as F

    out, _, _ = result
    bad = out.filter(
        (F.col("amt_total_debit") < 0)
        | (F.col("amt_total_fees") < 0)
        | (F.col("amt_max_single_debit") < 0)
    ).count()
    assert bad == 0


def test_written_output_readable(result):
    out, io, _ = result
    read_back = io.read_staging("STG_TXN_SUMMARY")
    assert read_back.count() == out.count()
    schemas.assert_schema(read_back, schemas.STG_TXN_SUMMARY)
