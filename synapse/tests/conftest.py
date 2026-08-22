import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

SYNAPSE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = SYNAPSE_ROOT.parent
sys.path.insert(0, str(SYNAPSE_ROOT))


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-utils-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_data_dir() -> Path:
    return REPO_ROOT / "data"
