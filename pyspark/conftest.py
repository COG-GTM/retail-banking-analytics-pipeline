"""Shared pytest fixtures: a local Delta-enabled SparkSession + seeded config.

The session is configured through ``common.spark.get_spark`` so tests exercise
the same factory the jobs use. Locally the ``spark_catalog`` catalog stands in
for the Unity Catalog ``retail_banking`` catalog (the only catalog name that
resolves against Delta's session catalog off-Databricks).
"""

from __future__ import annotations

import datetime as _dt
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "data"

# Fixed reference date matching the seeded sample data (``data/`` was generated
# on 2026-04-10); keeps age/tenure derivations reproducible and comparable.
SAMPLE_RUN_DATE = _dt.date(2026, 4, 10)
LOCAL_CATALOG = "spark_catalog"


@pytest.fixture(scope="session")
def spark():
    import os

    warehouse = tempfile.mkdtemp(prefix="rbap-warehouse-")
    os.environ.setdefault("RBAP_SPARK_MASTER", "local[2]")
    os.environ["RBAP_WAREHOUSE_DIR"] = warehouse

    from common.spark import get_spark

    session = get_spark("rbap-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def cfg():
    from common.config import Config

    return Config(catalog=LOCAL_CATALOG, run_date=SAMPLE_RUN_DATE)
