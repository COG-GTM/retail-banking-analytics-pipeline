"""End-to-end tier: one full DAG run on a synthetic medium data set."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.io import LocalDataIO
from common.job import STATUS_SUCCESS
from common.schemas import assert_schema
from orchestration.pipeline import PIPELINE, run_pipeline
from orchestration.synthetic_data import generate_sources, load_sources

pytestmark = pytest.mark.e2e

E2E_CUSTOMERS = 2_000
E2E_RUN_DATE = date(2026, 4, 10)


@pytest.fixture(scope="module")
def e2e_config() -> PipelineConfig:
    return PipelineConfig.from_cfg_file(
        Path(__file__).resolve().parents[3] / "config" / "pipeline_config.cfg",
        run_date=E2E_RUN_DATE,
        min_rows=100,
    )


@pytest.fixture(scope="module")
def e2e_run(
    spark: SparkSession, e2e_config: PipelineConfig, tmp_path_factory: pytest.TempPathFactory
):
    warehouse = tmp_path_factory.mktemp("e2e_warehouse")
    io = LocalDataIO(spark=spark, base_path=warehouse, fmt="parquet")
    loaded = load_sources(io, generate_sources(spark, E2E_CUSTOMERS, run_date=E2E_RUN_DATE))
    audit = AuditLog(run_timestamp=e2e_config.run_timestamp)
    run = run_pipeline(spark, io, e2e_config, audit)
    return run, io, loaded


def test_every_job_succeeds(e2e_run) -> None:
    run, _, _ = e2e_run
    statuses = {result.job_name: result.status for result in run.results}
    assert statuses == {node.name: STATUS_SUCCESS for node in PIPELINE}, run.summary_lines()
    assert run.return_code == 0


def test_every_target_table_is_materialised_and_conforms(e2e_run) -> None:
    _, io, _ = e2e_run
    for node in PIPELINE:
        database, table = node.target.split(".")
        spec = schemas.SPECS_BY_QUALIFIED_NAME[node.target]
        assert io.table_exists(database, table), node.target
        assert_schema(io.read_spec(spec), spec)


def test_customer_counts_are_consistent_across_the_products(e2e_run) -> None:
    _, io, _ = e2e_run
    staging = io.read_table("ETL_STAGING_DB", "STG_CUSTOMER_360")
    master = io.read_table("DATA_PRODUCTS_DB", "CUSTOMER_MASTER_PROFILE")
    active = staging.filter("CUSTOMER_STATUS = 'A'").count()

    # the master profile's base is the active customers of STG_CUSTOMER_360
    assert master.count() == active
    assert master.select("CUSTOMER_ID").distinct().count() == active


def test_audit_trail_is_written_for_every_job(e2e_run) -> None:
    _, io, _ = e2e_run
    audit = io.read_table("ETL_STAGING_DB", "PIPELINE_AUDIT")
    logged = {row["JOB_NAME"] for row in audit.select("JOB_NAME").distinct().collect()}
    for node in PIPELINE:
        assert node.name in logged or node.target.split(".")[1] in " ".join(logged), node.name


def test_post_run_counts_are_non_zero(e2e_run) -> None:
    run, _, _ = e2e_run
    assert run.post_run_counts, "the driver did not run the master script's post-run block"
    assert all(count > 0 for count in run.post_run_counts.values()), run.post_run_counts
