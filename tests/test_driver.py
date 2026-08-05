"""End-to-end test of the STEP 1 -> STEP 6 driver over the committed extracts."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from risk_scoring.audit import AuditLog  # noqa: E402
from risk_scoring.config import PipelineConfig  # noqa: E402
from risk_scoring.driver import log_tier_distribution, run  # noqa: E402
from risk_scoring.schemas import CUSTOMER_RISK_SCORES_COLUMNS  # noqa: E402
from risk_scoring.session import build_spark_session  # noqa: E402
from risk_scoring.validation import ValidationError  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent

#: The committed extracts yield 407 active customers.
EXPECTED_ROWS = 407


@pytest.fixture(scope="module")
def spark():
    session = build_spark_session("risk-scoring-driver-tests", master="local[2]")
    yield session
    session.stop()


@pytest.fixture
def config(tmp_path: Path) -> PipelineConfig:
    # min_rows below the SAS 1000 because the committed extract is a sample;
    # see docs/validation_report.md section 3.5.
    return PipelineConfig.load(
        io_backend="csv", min_rows=400, output_dir=tmp_path / "output"
    )


def test_run_loads_every_active_customer_with_the_target_schema(spark, config) -> None:
    written = run(config, spark)

    assert written == EXPECTED_ROWS

    loaded = spark.read.parquet(str(config.output_dir / "customer_risk_scores"))
    assert loaded.count() == EXPECTED_ROWS
    assert loaded.columns == list(CUSTOMER_RISK_SCORES_COLUMNS)
    assert loaded.select("CUSTOMER_ID").distinct().count() == EXPECTED_ROWS
    assert loaded.filter("COMPOSITE_RISK_SCORE IS NULL OR RISK_TIER IS NULL").count() == 0


def test_run_is_idempotent(spark, config) -> None:
    first = run(config, spark)
    second = run(config, spark)

    assert first == second == EXPECTED_ROWS
    # A truncate-load, not an append: the SAS DELETE + PROC APPEND pair.
    assert (
        spark.read.parquet(str(config.output_dir / "customer_risk_scores")).count()
        == EXPECTED_ROWS
    )


def test_run_aborts_when_the_row_count_gate_fails(spark, config) -> None:
    strict = PipelineConfig.load(
        io_backend="csv", min_rows=1000, output_dir=config.output_dir
    )

    with pytest.raises(ValidationError, match="minimum: 1000"):
        run(strict, spark)

    # STEP 6 never ran, so nothing was written.
    assert not (strict.output_dir / "customer_risk_scores").exists()
    # The abort path releases its caches: a long-lived session must not keep
    # accumulating pinned RDDs run after failed run.
    assert not spark.sparkContext._jsc.getPersistentRDDs()


def test_main_reports_a_missing_teradata_password_without_a_traceback() -> None:
    # A subprocess, not an in-process call: main() stops the SparkContext, which
    # is a JVM-wide singleton the other tests in this module share.
    env = {k: v for k, v in os.environ.items() if k != "TD_PASSWORD"}
    completed = subprocess.run(
        [sys.executable, "-m", "risk_scoring.driver", "--io-backend", "jdbc"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )

    assert completed.returncode == 1
    assert "$TD_PASSWORD is unset" in completed.stderr
    assert "Traceback" not in completed.stderr


def test_log_tier_distribution_replaces_proc_freq(spark) -> None:
    df = spark.createDataFrame(
        [("LOW",), ("LOW",), ("MODERATE",)], "RISK_TIER string"
    )
    audit = AuditLog("03_RISK_SCORING")

    distribution = log_tier_distribution(df, audit)

    assert distribution == [("LOW", 2), ("MODERATE", 1)]
    assert [(r.row_count, r.status) for r in audit.records] == [
        (2, "SUCCESS"),
        (1, "SUCCESS"),
    ]
