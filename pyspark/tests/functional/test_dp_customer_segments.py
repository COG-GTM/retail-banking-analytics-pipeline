"""Functional test: run CUSTOMER_SEGMENTS end-to-end on the committed sample data.

Uses ``read_products_from_source=True`` so ``read_staging`` loads the committed
``data/02_bteq_staging/stg_customer_360.csv`` directly -- this job does not
depend on the staging job running first.
"""

from __future__ import annotations

import datetime as _dt

import pytest

from common import schemas
from common.io import LocalDataIO
from jobs import dp_customer_segments as job

pytestmark = pytest.mark.functional

_ALLOWED_SEGMENTS = set(job.SEGMENT_NAMES)


@pytest.fixture(scope="module")
def result(spark, config, data_dir, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    io = LocalDataIO(
        spark, config, source_dir=data_dir, lake_dir=lake, read_products_from_source=True
    )
    out = job.run(spark, io, config)
    return out, io, lake


def test_schema_matches_ddl_exactly(result):
    out, _, _ = result
    schemas.assert_schema(out, schemas.CUSTOMER_SEGMENTS)


def test_primary_key_unique(result):
    out, _, _ = result
    assert out.count() == out.select("customer_id").distinct().count()


def test_only_active_customers_present(result):
    out, io, _ = result
    stg = io.read_staging("STG_CUSTOMER_360")
    expected = stg.filter("customer_status = 'A'").select("customer_id").distinct().count()
    assert out.count() == expected


def test_segment_names_within_allowed_set(result):
    out, _, _ = result
    names = {r.segment_name for r in out.select("segment_name").distinct().collect()}
    assert names, "expected at least one segment"
    assert names.issubset(_ALLOWED_SEGMENTS)


def test_flags_are_y_or_n(result):
    out, _, _ = result
    for col in ("cross_sell_flag", "upsell_flag", "retention_risk_flag"):
        vals = {r[col] for r in out.select(col).distinct().collect()}
        assert vals.issubset({"Y", "N"})


def test_model_version_and_effective_date(result):
    out, _, _ = result
    row = out.select("model_version", "effective_date").first()
    assert row.model_version == "SEG_V3.2"
    assert row.effective_date == _dt.date(2026, 4, 10)


def test_written_output_readable(result):
    out, io, _ = result
    read_back = io.read_data_product("CUSTOMER_SEGMENTS")
    assert read_back.count() == out.count()
    schemas.assert_schema(read_back, schemas.CUSTOMER_SEGMENTS)
