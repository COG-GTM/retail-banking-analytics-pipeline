"""Shared test fixtures for every tier."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DateType, DecimalType, TimestampType

from common.audit import AuditLog
from common.config import PipelineConfig
from common.io import InMemoryDataIO, LocalDataIO
from common.schemas import TableSpec
from common.spark import TEST_CONF, build_spark_session
from orchestration.sample_data import SAMPLE_RUN_DATE, reference_output_io, sample_source_io

REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_DATE = date.fromisoformat(SAMPLE_RUN_DATE)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = build_spark_session(
        "retail_banking_analytics_tests", master="local[2]", conf=TEST_CONF, log_level="ERROR"
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def run_date() -> date:
    return RUN_DATE


@pytest.fixture(scope="session")
def load_ts(spark: SparkSession) -> Column:
    """A pinned ``LOAD_TS`` so that transform outputs are byte-for-byte reproducible."""

    return F.lit("2026-04-10 00:00:00").cast("timestamp")


@pytest.fixture
def config() -> PipelineConfig:
    """Config parsed from the real ``config/pipeline_config.cfg`` with the run date pinned.

    ``min_rows`` is lowered from the production value of 1000 because the committed sample
    extract is a 500-customer slice; the production default lives in
    :class:`common.config.PipelineConfig`.
    """

    return PipelineConfig.from_cfg_file(
        REPO_ROOT / "config" / "pipeline_config.cfg", run_date=RUN_DATE, min_rows=1
    )


@pytest.fixture
def audit() -> AuditLog:
    return AuditLog(run_timestamp="20260410_000000")


@pytest.fixture
def memory_io() -> InMemoryDataIO:
    return InMemoryDataIO()


@pytest.fixture(scope="session")
def source_io(spark: SparkSession) -> LocalDataIO:
    return sample_source_io(spark, REPO_ROOT)


@pytest.fixture(scope="session")
def reference_io(spark: SparkSession) -> LocalDataIO:
    return reference_output_io(spark, REPO_ROOT)


@pytest.fixture
def make_df(spark: SparkSession):
    """Build a DataFrame for a table spec from a list of dicts, filling absent columns with NULL."""

    def _coerce(value: object, dtype: DataType) -> object:
        if value is None:
            return None
        if isinstance(dtype, DecimalType) and not isinstance(value, Decimal):
            return Decimal(str(value)).quantize(Decimal(1).scaleb(-dtype.scale))
        if isinstance(dtype, DateType) and isinstance(value, str):
            return date.fromisoformat(value)
        if isinstance(dtype, TimestampType) and isinstance(value, str):
            return datetime.fromisoformat(value)
        return value

    def _make(spec: TableSpec, rows: Sequence[dict[str, object]]) -> DataFrame:
        schema = spec.spark_schema()
        materialised = [
            tuple(_coerce(row.get(column.name), column.dtype) for column in spec.columns)
            for row in rows
        ]
        return spark.createDataFrame(materialised, schema=schema)

    return _make
