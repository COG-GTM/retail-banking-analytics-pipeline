"""Shared pytest fixtures: a Delta-enabled local SparkSession seeded from data/."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

# Local runs address tables via the built-in Delta-enabled ``spark_catalog`` so
# that three-part ``catalog.schema.table`` identifiers resolve without Unity
# Catalog. Set before common.config / common.spark read the environment.
os.environ.setdefault("CATALOG", "spark_catalog")

# PySpark 3.5 supports JDK 8/11/17 but NOT newer JDKs. Pin to the blueprint's
# JDK 17 when the current JAVA_HOME is unset or points at an unsupported version.
_JDK17 = "/usr/lib/jvm/java-17-openjdk-amd64"


def _java_ok(java_home: str) -> bool:
    if not java_home or not Path(java_home).exists():
        return False
    base = Path(java_home).name
    return any(v in base for v in ("java-8", "java-11", "java-17", "1.8", "11", "17"))


if Path(_JDK17).exists() and not _java_ok(os.environ.get("JAVA_HOME", "")):
    os.environ["JAVA_HOME"] = _JDK17

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="rbap-warehouse-")
    os.environ["SPARK_WAREHOUSE_DIR"] = warehouse

    from common.spark import get_spark

    session = get_spark(app_name="rbap-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def cfg():
    from common.config import get_config

    return get_config()


def _seed_delta_from_csv(spark, cfg, schema: str, table: str, csv_path: Path) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(csv_path))
    )
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(cfg.table(schema, table))


@pytest.fixture(scope="session")
def seed_staging(spark, cfg):
    """Load the staging (silver) seed CSVs into Delta tables once per session."""
    staging = DATA_DIR / "02_bteq_staging"
    _seed_delta_from_csv(
        spark, cfg, cfg.schema_stg, "stg_risk_factors", staging / "stg_risk_factors.csv"
    )
    _seed_delta_from_csv(
        spark, cfg, cfg.schema_stg, "stg_customer_360", staging / "stg_customer_360.csv"
    )
    return cfg
