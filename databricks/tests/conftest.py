"""Shared pytest fixtures.

The integration fixtures run the whole medallion pipeline on a local Spark
session against the generated CSVs in ``data/01_source_tables``, so the ported
transforms are exercised end to end without a Databricks workspace. Local Spark
has no Delta jars, so tables are written as Parquet (``table_format=parquet``);
every other code path is identical to a workspace run.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pytest

DATABRICKS_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = DATABRICKS_ROOT.parent
SOURCE_DATA = REPO_ROOT / "data" / "01_source_tables"
SILVER_REFERENCE = REPO_ROOT / "data" / "02_bteq_staging"
GOLD_REFERENCE = REPO_ROOT / "data" / "03_sas_data_products"

# The committed reference outputs were produced by the legacy pipeline with this
# as-of date; the port must use the same one to be comparable.
REFERENCE_RUN_DATE = date(2026, 4, 10)

if str(DATABRICKS_ROOT) not in sys.path:
    sys.path.insert(0, str(DATABRICKS_ROOT))

from shared import io  # noqa: E402
from shared.config import PipelineConfig  # noqa: E402

NOTEBOOKS = [
    "notebooks/bronze/00_ingest_source_tables.py",
    "notebooks/silver/01_stg_customer_360.py",
    "notebooks/silver/02_stg_txn_summary.py",
    "notebooks/silver/03_stg_risk_factors.py",
    "notebooks/gold/01_customer_segments.py",
    "notebooks/gold/02_txn_analytics.py",
    "notebooks/gold/03_risk_scoring.py",
    "notebooks/gold/04_data_products.py",
]


def load_notebook(relative_path: str) -> ModuleType:
    """Import a notebook file as a module (names start with a digit)."""
    path = DATABRICKS_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(f"nb_{path.stem}", path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory):
    pyspark = pytest.importorskip("pyspark")
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .appName("retail-banking-pipeline-tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def cfg() -> PipelineConfig:
    return PipelineConfig(
        catalog="spark_catalog",
        bronze_schema="test_bronze",
        silver_schema="test_silver",
        gold_schema="test_gold",
        ops_schema="test_ops",
        run_date=REFERENCE_RUN_DATE,
        source_data_path=str(SOURCE_DATA),
        table_format="parquet",
        optimize_tables=False,
        write_mode="overwrite",
        run_id="pytest",
    )


@pytest.fixture(scope="session", autouse=True)
def _schemas(spark, cfg: PipelineConfig) -> None:
    io.ensure_schemas(spark, cfg)


@pytest.fixture(scope="session")
def pipeline(spark, cfg: PipelineConfig) -> dict[str, int]:
    """Run bronze -> silver -> gold once and return the row count per step."""
    counts: dict[str, int] = {}
    for relative_path in NOTEBOOKS:
        module = load_notebook(relative_path)
        entrypoint = module.ingest if hasattr(module, "ingest") else module.run
        result = entrypoint(spark, cfg)
        counts[Path(relative_path).stem] = result
    return counts
