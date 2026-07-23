"""Shared pytest fixtures: a Delta-enabled local SparkSession and a test Config."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# Pin a Spark-supported JDK (PySpark 3.5 supports Java 11/17) if the ambient
# JAVA_HOME points elsewhere and a java-17 install is available locally.
_JDK17 = "/usr/lib/jvm/java-17-openjdk-amd64"
if os.path.isdir(_JDK17):
    os.environ["JAVA_HOME"] = _JDK17
    os.environ["PATH"] = f"{_JDK17}/bin:{os.environ.get('PATH', '')}"

from common.config import Config  # noqa: E402
from common.spark import build_local_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Session-scoped Delta-enabled local SparkSession."""
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = build_local_spark(app_name="rbap-tests", warehouse_dir=str(warehouse))
    yield session
    session.stop()


@pytest.fixture(scope="session")
def cfg() -> Config:
    """Test Config bound to the local Delta session catalog (``spark_catalog``).

    Using ``spark_catalog`` makes three-level ``catalog.schema.table`` names
    resolve against the Delta-enabled session catalog on local Spark, while
    production overrides ``catalog`` to the real Unity Catalog via widgets/env.
    """
    return Config(catalog="spark_catalog")


@pytest.fixture(scope="session")
def data_root() -> Path:
    """Path to the repo's seed CSVs (``../data``)."""
    return Path(__file__).resolve().parents[1] / "data"
