"""Shared pytest fixtures: a session-scoped local SparkSession + data paths."""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

from common.config import PipelineConfig
from common.spark import build_local_spark

# Repo root is two levels up from this file (pyspark/tests/conftest.py).
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

# The committed fixtures were generated as-of this date; pin run_date to it so
# wall-clock-derived fields (age, tenure, recency) are reproducible.
FIXTURE_RUN_DATE = _dt.date(2026, 4, 10)


@pytest.fixture(scope="session")
def spark():
    session = build_local_spark()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def config() -> PipelineConfig:
    return PipelineConfig(run_date=FIXTURE_RUN_DATE)


@pytest.fixture(scope="session")
def data_dir() -> Path:
    return DATA_DIR


@pytest.fixture
def tmp_lake(tmp_path) -> Path:
    lake = tmp_path / "lake"
    lake.mkdir(parents=True, exist_ok=True)
    return lake
