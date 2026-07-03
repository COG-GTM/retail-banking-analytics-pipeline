"""Functional test: run STG_CUSTOMER_360 end-to-end on the committed sample data."""

from __future__ import annotations

import pytest

from common import schemas
from common.io import LocalDataIO
from jobs import staging_customer_360 as job

pytestmark = pytest.mark.functional


@pytest.fixture(scope="module")
def result(spark, config, data_dir, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    io = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=lake)
    out = job.run(spark, io, config)
    return out, io, lake


def test_schema_matches_ddl_exactly(result):
    out, _, _ = result
    schemas.assert_schema(out, schemas.STG_CUSTOMER_360)


def test_primary_key_unique(result):
    out, _, _ = result
    assert out.count() == out.select("customer_id").distinct().count()


def test_only_active_or_inactive_customers(result):
    out, _, _ = result
    statuses = {r.customer_status for r in out.select("customer_status").distinct().collect()}
    assert statuses.issubset({"A", "I"})


def test_written_output_readable(result):
    out, io, _ = result
    read_back = io.read_staging("STG_CUSTOMER_360")
    assert read_back.count() == out.count()
    schemas.assert_schema(read_back, schemas.STG_CUSTOMER_360)


def test_flags_are_y_or_n(result):
    out, _, _ = result
    for col in ("has_checking", "has_savings", "has_credit", "has_loan"):
        vals = {r[col] for r in out.select(col).distinct().collect()}
        assert vals.issubset({"Y", "N", None})
