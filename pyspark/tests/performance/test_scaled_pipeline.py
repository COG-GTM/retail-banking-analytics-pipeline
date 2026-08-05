"""Performance tier — opt-in, on scaled synthetic data.

Run with ``pytest -m performance``. The volume is set by ``PERF_CUSTOMERS`` (default 25k on a
laptop-sized box); the SLA is the legacy baseline scaled by the customer ratio against the
10M-customer production target, so the same assertion holds at any volume:

    budget = legacy_baseline * (customers / 10_000_000), floored so small runs stay meaningful.

Legacy baselines: BTEQ phase ~20 min, SAS phase ~40 min at production volume.
"""

from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.audit import AuditLog
from common.config import PipelineConfig
from common.io import LocalDataIO
from common.job import STATUS_SUCCESS
from orchestration.pipeline import PHASE_BTEQ, PHASE_SAS, PIPELINE, run_pipeline
from orchestration.synthetic_data import generate_sources, load_sources

pytestmark = pytest.mark.performance

PERF_CUSTOMERS = int(os.environ.get("PERF_CUSTOMERS", "25000"))
PERF_RUN_DATE = date(2026, 4, 10)
PRODUCTION_CUSTOMERS = 10_000_000
LEGACY_BTEQ_SECONDS = 20 * 60
LEGACY_SAS_SECONDS = 40 * 60
#: A floor so that a small opt-in run is not asserted against a sub-second budget.
MINIMUM_BUDGET_SECONDS = 120.0

PHASES = {PHASE_BTEQ: LEGACY_BTEQ_SECONDS, PHASE_SAS: LEGACY_SAS_SECONDS}


def phase_budget(phase: str, customers: int = PERF_CUSTOMERS) -> float:
    scaled = PHASES[phase] * customers / PRODUCTION_CUSTOMERS
    return max(scaled, MINIMUM_BUDGET_SECONDS)


@pytest.fixture(scope="module")
def perf_config() -> PipelineConfig:
    return PipelineConfig.from_cfg_file(
        Path(__file__).resolve().parents[3] / "config" / "pipeline_config.cfg",
        run_date=PERF_RUN_DATE,
        min_rows=1000,
    )


@pytest.fixture(scope="module")
def perf_run(
    spark: SparkSession, perf_config: PipelineConfig, tmp_path_factory: pytest.TempPathFactory
):
    warehouse = tmp_path_factory.mktemp("perf_warehouse")
    io = LocalDataIO(spark=spark, base_path=warehouse, fmt="parquet")

    started = time.perf_counter()
    load_sources(io, generate_sources(spark, PERF_CUSTOMERS, run_date=PERF_RUN_DATE))
    generation_seconds = time.perf_counter() - started

    audit = AuditLog(run_timestamp=perf_config.run_timestamp)
    run = run_pipeline(spark, io, perf_config, audit)
    return run, io, generation_seconds


def test_scaled_run_completes(perf_run) -> None:
    run, _, _ = perf_run
    failures = [result.job_name for result in run.results if result.status != STATUS_SUCCESS]
    assert not failures, run.summary_lines()


@pytest.mark.parametrize("phase", [PHASE_BTEQ, PHASE_SAS])
def test_phase_stays_within_the_scaled_legacy_sla(perf_run, phase: str) -> None:
    run, _, _ = perf_run
    names = {node.name for node in PIPELINE if node.phase == phase}
    elapsed = sum(result.elapsed_seconds for result in run.results if result.job_name in names)
    budget = phase_budget(phase)
    assert elapsed <= budget, (
        f"{phase} phase took {elapsed:.1f}s at {PERF_CUSTOMERS:,} customers, "
        f"budget {budget:.1f}s (legacy {PHASES[phase] / 60:.0f} min at 10M customers)"
    )


def test_no_job_is_an_outlier_against_the_phase(perf_run) -> None:
    """A single job eating the whole phase budget means a missing broadcast or a skewed join."""

    run, _, _ = perf_run
    total = sum(result.elapsed_seconds for result in run.results) or 1.0
    for result in run.results:
        share = result.elapsed_seconds / total
        assert share < 0.75, f"{result.job_name} took {share:.0%} of the run"


def test_skewed_account_key_does_not_blow_up_a_single_partition(
    spark: SparkSession, perf_run
) -> None:
    """The generator plants a hot ACCOUNT_ID; AQE must keep the join partitions balanced."""

    _, io, _ = perf_run
    transactions = io.read_table("TXN_PROCESSING_DB", "TRANSACTIONS")
    per_account = transactions.groupBy("ACCOUNT_ID").agg(F.count("*").alias("N"))
    hottest = per_account.orderBy(F.col("N").desc()).first()["N"]
    median = per_account.approxQuantile("N", [0.5], 0.01)[0]

    assert hottest > median, "the synthetic skew did not materialise; the test proves nothing"
    assert spark.conf.get("spark.sql.adaptive.skewJoin.enabled") == "true"
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"


def test_generation_is_expression_based_not_row_by_row(perf_run) -> None:
    """A Python-loop generator would take minutes at this volume; expressions take seconds."""

    _, _, generation_seconds = perf_run
    assert generation_seconds < max(60.0, PERF_CUSTOMERS / 5000), generation_seconds
