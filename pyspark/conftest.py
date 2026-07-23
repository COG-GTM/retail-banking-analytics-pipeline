"""Shared pytest fixtures: a local Delta-enabled SparkSession and seed helpers.

Seeds are loaded from the repo ``data/`` CSVs so tests exercise the real
pipeline shapes. The local session uses the built-in ``spark_catalog`` so the
``catalog.schema.table`` naming in ``Config`` resolves without Unity Catalog.
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from common.config import Config

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"


@pytest.fixture(scope="session")
def warehouse_dir() -> str:
    path = tempfile.mkdtemp(prefix="rba_warehouse_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(warehouse_dir: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName("rba_tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        pass

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture()
def cfg() -> Config:
    """Config bound to the local built-in catalog."""
    return Config(catalog="spark_catalog")


def read_seed_csv(spark: SparkSession, subdir: str, name: str):
    """Read a repo seed CSV with header + type inference."""
    path = DATA_DIR / subdir / f"{name}.csv"
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(path))
    )
