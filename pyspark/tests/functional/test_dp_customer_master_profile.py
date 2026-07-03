"""Functional test: run CUSTOMER_MASTER_PROFILE end-to-end on the committed data.

The job merges three upstream data products, so the functional inputs are seeded
into a temporary lake first (the upstream ``dp_*`` jobs are ported in sibling
modules).  ``STG_CUSTOMER_360``, ``CUSTOMER_SEGMENTS`` and ``CUSTOMER_RISK_SCORES``
are read through the sanctioned CSV reader; ``TRANSACTION_ANALYTICS`` is read by
header name because that committed fixture's column order does not match
``schemas.TRANSACTION_ANALYTICS`` (see the PR description -- a shared fixture
issue, not fixed here).
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from common import schemas
from common.io import LocalDataIO
from jobs import dp_customer_master_profile as job

pytestmark = pytest.mark.functional


def _read_txn_by_name(spark, data_dir):
    """Header-name read of the transaction-analytics fixture, schema-enforced."""
    path = data_dir / "03_sas_data_products" / "transaction_analytics.csv"
    raw = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    return schemas.enforce_schema(raw, schemas.TRANSACTION_ANALYTICS)


def _seed_lake(spark, config, data_dir, lake, drop_risk_ids=()):
    """Materialise the four inputs into ``lake`` and return a lake-backed IO."""
    src = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=lake, read_products_from_source=True)
    dst = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=lake)

    dst.write_staging(src.read_staging("STG_CUSTOMER_360"), "STG_CUSTOMER_360")
    dst.write_data_product(src.read_data_product("CUSTOMER_SEGMENTS"), "CUSTOMER_SEGMENTS")

    risk = src.read_data_product("CUSTOMER_RISK_SCORES")
    if drop_risk_ids:
        risk = risk.filter(~F.col("customer_id").isin(*drop_risk_ids))
    dst.write_data_product(risk, "CUSTOMER_RISK_SCORES")

    dst.write_data_product(_read_txn_by_name(spark, data_dir), "TRANSACTION_ANALYTICS")
    return dst


@pytest.fixture(scope="module")
def result(spark, config, data_dir, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    io = _seed_lake(spark, config, data_dir, lake)
    out = job.run(spark, io, config).cache()
    base_ids = {
        r.customer_id
        for r in io.read_staging("STG_CUSTOMER_360")
        .filter(F.col("customer_status") == "A")
        .select("customer_id")
        .collect()
    }
    return out, io, base_ids


def test_schema_matches_ddl_exactly(result):
    out, _, _ = result
    schemas.assert_schema(out, schemas.CUSTOMER_MASTER_PROFILE)


def test_primary_key_unique(result):
    out, _, _ = result
    assert out.count() == out.select("customer_id").distinct().count()


def test_every_active_base_customer_present_once(result):
    out, _, base_ids = result
    out_ids = {r.customer_id for r in out.select("customer_id").collect()}
    assert out_ids == base_ids
    assert out.count() == len(base_ids)


def test_only_active_status(result):
    out, _, _ = result
    statuses = {r.customer_status for r in out.select("customer_status").distinct().collect()}
    assert statuses == {"A"}


def test_metadata_constant(result, config):
    out, _, _ = result
    versions = {r.model_version for r in out.select("model_version").distinct().collect()}
    assert versions == {"MASTER_V1.5"}
    eff = {r.effective_date for r in out.select("effective_date").distinct().collect()}
    assert eff == {config.run_date}


def test_written_output_readable(result):
    out, io, _ = result
    read_back = io.read_data_product("CUSTOMER_MASTER_PROFILE")
    assert read_back.count() == out.count()
    schemas.assert_schema(read_back, schemas.CUSTOMER_MASTER_PROFILE)


def test_defaults_applied_when_risk_absent(spark, config, data_dir, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake_missing_risk")
    # find an active base customer that has a risk row, then drop it.
    src = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=lake, read_products_from_source=True)
    base_ids = {
        r.customer_id for r in src.read_staging("STG_CUSTOMER_360")
        .filter(F.col("customer_status") == "A").select("customer_id").collect()
    }
    risk_ids = {r.customer_id for r in src.read_data_product("CUSTOMER_RISK_SCORES").select("customer_id").collect()}
    victim = min(base_ids & risk_ids)

    io = _seed_lake(spark, config, data_dir, lake, drop_risk_ids=(victim,))
    out = job.run(spark, io, config)
    row = out.filter(F.col("customer_id") == victim).collect()[0]
    assert row.risk_tier == "UNKNOWN"
    assert row.composite_risk_score is None
    assert row.probability_of_default is None
    assert row.watch_list_flag == "N"
