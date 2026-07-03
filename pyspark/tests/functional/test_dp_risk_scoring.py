"""Functional test: run CUSTOMER_RISK_SCORES on the committed sample data."""

from __future__ import annotations

import pytest

from common import schemas
from common.io import LocalDataIO
from jobs import dp_risk_scoring as job

pytestmark = pytest.mark.functional

_TIERS = {"LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"}


@pytest.fixture(scope="module")
def result(spark, config, data_dir, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    # read_products_from_source=True: read STG_* directly from committed CSVs so
    # this test does not depend on the staging jobs running first.
    io = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=lake, read_products_from_source=True)
    out = job.run(spark, io, config)
    return out, io, lake


def test_schema_matches_ddl_exactly(result):
    out, _, _ = result
    schemas.assert_schema(out, schemas.CUSTOMER_RISK_SCORES)


def test_primary_key_unique(result):
    out, _, _ = result
    assert out.count() > 0
    assert out.count() == out.select("customer_id").distinct().count()


def test_composite_score_in_range(result):
    out, _, _ = result
    bad = out.filter((out.composite_risk_score < 0) | (out.composite_risk_score > 100)).count()
    assert bad == 0


def test_risk_tier_valid(result):
    out, _, _ = result
    tiers = {r.risk_tier for r in out.select("risk_tier").distinct().collect()}
    assert tiers.issubset(_TIERS)


def test_probability_in_unit_interval(result):
    out, _, _ = result
    bad = out.filter((out.probability_of_default < 0) | (out.probability_of_default > 1)).count()
    assert bad == 0


def test_tier_matches_composite_cutoffs(result):
    out, _, _ = result
    # spot-check the tier logic holds on the real distribution
    assert out.filter((out.composite_risk_score < 20) & (out.risk_tier != "LOW")).count() == 0
    assert out.filter((out.composite_risk_score >= 80) & (out.risk_tier != "CRITICAL")).count() == 0


def test_flags_are_y_or_n(result):
    out, _, _ = result
    for col in ("watch_list_flag", "review_required_flag"):
        vals = {r[col] for r in out.select(col).distinct().collect()}
        assert vals.issubset({"Y", "N"})


def test_written_output_readable(result):
    out, io, _ = result
    read_back = io.read_data_product("CUSTOMER_RISK_SCORES")
    assert read_back.count() == out.count()
    schemas.assert_schema(read_back, schemas.CUSTOMER_RISK_SCORES)
