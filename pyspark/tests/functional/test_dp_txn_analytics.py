"""Functional test: run TRANSACTION_ANALYTICS end-to-end on the committed sample data."""

from __future__ import annotations

import pytest

from common import schemas
from common.io import LocalDataIO
from jobs import dp_txn_analytics as job

pytestmark = pytest.mark.functional


@pytest.fixture(scope="module")
def result(spark, config, data_dir, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    # read_products_from_source=True -> read STG_TXN_SUMMARY from the committed
    # CSV fixtures so this test does not depend on the staging job running first.
    io = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=lake, read_products_from_source=True)
    out = job.run(spark, io, config)
    return out, io, lake


def test_schema_matches_ddl_exactly(result):
    out, _, _ = result
    schemas.assert_schema(out, schemas.TRANSACTION_ANALYTICS)


def test_primary_key_unique(result):
    out, _, _ = result
    assert out.count() == out.select("customer_id").distinct().count()


def test_spend_percentile_in_range(result):
    out, _, _ = result
    bounds = out.agg(
        {"spend_percentile": "min"}
    ).collect()[0][0], out.agg({"spend_percentile": "max"}).collect()[0][0]
    lo, hi = bounds
    assert lo >= 0 and hi <= 99


def test_anomaly_flag_domain(result):
    out, _, _ = result
    vals = {r.anomaly_flag for r in out.select("anomaly_flag").distinct().collect()}
    assert vals.issubset({"Y", "N"})


def test_reporting_period_matches_config(result, config):
    out, _, _ = result
    periods = {r.reporting_period for r in out.select("reporting_period").distinct().collect()}
    assert periods == {config.reporting_period}


def test_monthly_spend_trend_domain(result):
    out, _, _ = result
    vals = {r.monthly_spend_trend for r in out.select("monthly_spend_trend").distinct().collect()}
    assert vals.issubset({"UP", "DOWN", "STABLE"})


def test_written_output_readable(result, spark):
    out, _, lake = result
    # io was built with read_products_from_source=True, so read the written
    # parquet lake output directly to verify what the job actually wrote.
    read_back = spark.read.parquet(str(lake / "03_sas_data_products" / "TRANSACTION_ANALYTICS"))
    assert read_back.count() == out.count()
    # reporting_period is a partition column, so parquet returns it last; compare
    # on the (order-independent) column set, then re-enforce the DDL contract.
    assert set(read_back.columns) == set(schemas.TRANSACTION_ANALYTICS.column_names)
    schemas.assert_schema(
        schemas.enforce_schema(read_back, schemas.TRANSACTION_ANALYTICS),
        schemas.TRANSACTION_ANALYTICS,
    )
