"""Regression parity: PySpark STG_CUSTOMER_360 vs the committed reference output.

The committed CSVs under ``data/`` were produced by the DuckDB demo engine
(``local/duckdb/run_demo.py``), which is NOT the authoritative logic (see the
task brief + ``MIGRATION_NOTES.md``). It diverges from the BTEQ on ``age`` /
``tenure_months``: the demo uses a calendar-component difference
(``year(run) - year(dob)`` etc.), whereas the authoritative BTEQ uses a day-based
``CAST((CURRENT_DATE - DATE_OF_BIRTH) / 365.25 AS SMALLINT)``. These differ by at
most 1 (the birthday/anniversary not-yet-reached case), so they are compared with
a +/-1 tolerance; every other column must match the reference EXACTLY.
"""

from __future__ import annotations

import pytest

from common.io import LocalDataIO
from jobs import staging_customer_360 as job
from tests.regression._parity import compare

pytestmark = pytest.mark.regression

# Columns where the non-authoritative demo engine diverges from the BTEQ.
_DEMO_DIVERGENCE = {"age": 1.0, "tenure_months": 1.0}


def test_customer_360_parity(spark, config, data_dir, tmp_path):
    io = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=tmp_path)
    actual = job.run(spark, io, config)

    expected = (
        LocalDataIO(spark, config, source_dir=data_dir, lake_dir=tmp_path, read_products_from_source=True)
        .read_staging("STG_CUSTOMER_360")
    )

    report = compare(actual, expected, key="customer_id", tol_cols=_DEMO_DIVERGENCE)
    assert not report.key_mismatches, f"key mismatches: {report.key_mismatches[:10]}"
    assert report.ok, f"value mismatches (first 15): {report.value_mismatches[:15]}"
    assert report.matched == report.total == expected.count()


def test_age_tenure_divergence_within_one(spark, config, data_dir, tmp_path):
    """The only staging divergence from the reference is age/tenure, and it is
    bounded by 1 (documents the demo-vs-BTEQ convention difference)."""
    io = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=tmp_path)
    actual = job.run(spark, io, config)
    expected = (
        LocalDataIO(spark, config, source_dir=data_dir, lake_dir=tmp_path, read_products_from_source=True)
        .read_staging("STG_CUSTOMER_360")
    )
    # With no tolerance, every mismatch must be one of the documented columns.
    strict = compare(actual, expected, key="customer_id")
    offending_cols = {c for _, c, _, _ in strict.value_mismatches}
    assert offending_cols <= set(_DEMO_DIVERGENCE), f"unexpected divergent columns: {offending_cols}"
