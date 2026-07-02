"""Shared pytest fixtures for the PySpark job tests."""
from __future__ import annotations

import os

import pytest

from spark.config import PipelineConfig
from spark.logging_utils import PipelineAudit


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("retail_banking_analytics_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def config(tmp_path) -> PipelineConfig:
    os.environ["RUN_DATE"] = "2024-01-15"
    os.environ["RUN_TS"] = "2024-01-15 00:00:00.000000"
    os.environ["PIPELINE_MIN_ROWS"] = "1"
    os.environ["KMEANS_SEED"] = "42"
    os.environ["STAGING_PATH"] = str(tmp_path / "staging")
    os.environ["DATA_PRODUCTS_PATH"] = str(tmp_path / "products")
    return PipelineConfig.from_env()


@pytest.fixture()
def audit(config) -> PipelineAudit:
    return PipelineAudit(run_id=config.run_id)
