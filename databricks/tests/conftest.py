"""Shared pytest fixtures for the Databricks migration test-suite.

A single local Spark session (with Delta) is created for the whole test run.
The working directory is switched to a temp dir so the Derby metastore and
``spark-warehouse`` land outside the repository.
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import date, datetime

import pytest

# Make the ``databricks/`` project root importable (common / jobs / orchestration).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Reference as-of date baked into the sample fixtures (data through 2026-04-10).
REFERENCE_RUN_DATE = date(2026, 4, 10)
FIXED_LOAD_TS = datetime(2026, 4, 10, 3, 0, 0)


@pytest.fixture(scope="session")
def spark():
    workdir = tempfile.mkdtemp(prefix="rba_pytest_")
    os.chdir(workdir)
    from common.spark_utils import get_spark

    session = get_spark(app_name="rba-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def config(spark):
    from common.config import load_config

    return load_config(
        spark=spark,
        catalog="spark_catalog",
        run_date=REFERENCE_RUN_DATE,
        load_ts=FIXED_LOAD_TS,
    )


@pytest.fixture(scope="session")
def pipeline_result(spark, config):
    """Create DDL, load sample sources and run the full pipeline once."""
    from common import ddl
    from orchestration.load_sample_data import load_sample_sources
    from orchestration.pipeline import run_pipeline

    ddl.create_all(spark, config)
    load_sample_sources(spark, config)
    counts = run_pipeline(spark, config, min_rows=1)
    return counts
