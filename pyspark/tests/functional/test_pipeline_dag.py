"""The DAG driver must reproduce the legacy ordering and fail-fast semantics."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from common.audit import AuditLog
from common.config import PipelineConfig
from common.io import DataIO, InMemoryDataIO
from common.job import STATUS_FAILED, STATUS_SKIPPED, STATUS_SUCCESS, STATUS_WARNING, JobResult
from common.validation import ValidationFailedError
from orchestration import pipeline
from orchestration.pipeline import PHASE_BTEQ, PHASE_SAS, PIPELINE, JobNode, run_pipeline

LEGACY_ORDER = (
    "01_stg_customer_360",
    "02_stg_txn_summary",
    "03_stg_risk_factors",
    "01_sas_customer_segments",
    "02_sas_txn_analytics",
    "03_sas_risk_scoring",
    "04_sas_data_products",
)


def _runner_factory(behaviour: dict[str, str]):
    """Build a resolver returning a stub runner honouring ``behaviour[job] -> status``."""

    def resolve(node: JobNode):
        def run(
            spark: SparkSession, io: DataIO, config: PipelineConfig, audit: AuditLog
        ) -> JobResult:
            status = behaviour.get(node.name, STATUS_SUCCESS)
            if status == "raise":
                raise ValidationFailedError(f"{node.name} min_rows")
            if status == "boom":
                raise RuntimeError("unexpected")
            return JobResult(
                job_name=node.name,
                status=status,
                row_count=1,
                start_ts=datetime.now(),
                end_ts=datetime.now(),
                target_table=node.target,
            )

        return run

    return resolve


@pytest.fixture
def driver_io() -> InMemoryDataIO:
    return InMemoryDataIO()


def test_graph_matches_the_legacy_execution_order() -> None:
    assert tuple(node.name for node in PIPELINE) == LEGACY_ORDER
    assert [node.phase for node in PIPELINE] == [PHASE_BTEQ] * 3 + [PHASE_SAS] * 4


def test_every_node_points_at_a_real_legacy_source(repo_root: Path) -> None:
    for node in PIPELINE:
        assert (repo_root / node.legacy_source).exists(), node.legacy_source


def test_dependencies_only_reference_earlier_jobs() -> None:
    seen: set[str] = set()
    for node in PIPELINE:
        assert set(node.depends_on) <= seen, f"{node.name} depends on a later job"
        seen.add(node.name)


def test_dry_run_lists_the_plan_and_runs_nothing(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        dry_run=True,
        resolve=_runner_factory({"01_stg_customer_360": "boom"}),
    )

    assert run.results == []
    assert run.return_code == 0
    plan = "\n".join(pipeline.describe())
    for name in LEGACY_ORDER:
        assert name in plan


def test_happy_path_runs_every_job_in_order(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        resolve=_runner_factory({}),
        post_run_validation=False,
        persist_audit=False,
    )

    assert [result.job_name for result in run.results] == list(LEGACY_ORDER)
    assert {result.status for result in run.results} == {STATUS_SUCCESS}
    assert run.return_code == 0


def test_bteq_failure_aborts_the_sas_phase(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        resolve=_runner_factory({"02_stg_txn_summary": STATUS_FAILED}),
        post_run_validation=False,
        persist_audit=False,
    )

    statuses = {result.job_name: result.status for result in run.results}
    assert statuses["01_stg_customer_360"] == STATUS_SUCCESS
    assert statuses["02_stg_txn_summary"] == STATUS_FAILED
    assert statuses["03_stg_risk_factors"] == STATUS_SKIPPED
    # the master script exits before the SAS phase when BTEQ returns non-zero
    assert all(statuses[name] == STATUS_SKIPPED for name in LEGACY_ORDER[3:])
    assert run.return_code == 2


def test_validation_abort_is_a_job_failure(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        resolve=_runner_factory({"01_stg_customer_360": "raise"}),
        post_run_validation=False,
        persist_audit=False,
    )

    first = run.results[0]
    assert first.status == STATUS_FAILED
    assert "min_rows" in first.error
    assert run.return_code == 2


def test_unexpected_exception_is_converted_to_rc2(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        resolve=_runner_factory({"01_stg_customer_360": "boom"}),
        post_run_validation=False,
        persist_audit=False,
    )

    assert run.results[0].status == STATUS_FAILED
    assert "RuntimeError" in run.results[0].error


def test_sas_warning_return_code_is_tolerated(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        resolve=_runner_factory({"01_sas_customer_segments": STATUS_WARNING}),
        post_run_validation=False,
        persist_audit=False,
    )

    statuses = {result.job_name: result.status for result in run.results}
    assert statuses["01_sas_customer_segments"] == STATUS_WARNING
    assert statuses["04_sas_data_products"] == STATUS_SUCCESS
    assert run.return_code == 1  # SAS rc=1 is a warning, not an abort


def test_sas_failure_leaves_the_staging_phase_intact(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        resolve=_runner_factory({"02_sas_txn_analytics": STATUS_FAILED}),
        post_run_validation=False,
        persist_audit=False,
    )

    statuses = {result.job_name: result.status for result in run.results}
    assert all(statuses[name] == STATUS_SUCCESS for name in LEGACY_ORDER[:3])
    assert statuses["03_sas_risk_scoring"] == STATUS_SKIPPED
    assert statuses["04_sas_data_products"] == STATUS_SKIPPED


def test_skip_phases_mirrors_the_shell_flags(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        resolve=_runner_factory({}),
        skip_phases=(PHASE_BTEQ,),
        post_run_validation=False,
        persist_audit=False,
    )

    statuses = {result.job_name: result.status for result in run.results}
    assert all(statuses[name] == STATUS_SKIPPED for name in LEGACY_ORDER[:3])
    # --skip-bteq assumes staging is already loaded, so the SAS phase still runs
    assert all(statuses[name] == STATUS_SUCCESS for name in LEGACY_ORDER[3:])


def test_metrics_json_is_written_from_the_run(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig, tmp_path: Path
) -> None:
    run = run_pipeline(
        spark,
        driver_io,
        config,
        resolve=_runner_factory({}),
        post_run_validation=False,
        persist_audit=False,
    )
    target = run.write_json(tmp_path / "metrics.json")
    payload = json.loads(target.read_text())

    assert payload["run_date"] == config.run_date_str
    assert [job["job_name"] for job in payload["jobs"]] == list(LEGACY_ORDER)
    assert all("elapsed_seconds" in job for job in payload["jobs"])


def test_audit_trail_is_persisted_by_the_driver(
    spark: SparkSession, driver_io: InMemoryDataIO, config: PipelineConfig
) -> None:
    audit = AuditLog(run_timestamp=config.run_timestamp)
    audit.log_step("01_stg_customer_360", "SUCCESS", "done", rowcount=1)

    run_pipeline(
        spark,
        driver_io,
        config,
        audit,
        resolve=_runner_factory({}),
        post_run_validation=False,
    )

    assert driver_io.table_exists("ETL_STAGING_DB", "PIPELINE_AUDIT")


def test_scheduler_wiring_is_generated_from_the_same_graph() -> None:
    from orchestration.airflow_dag import task_specifications

    specs = task_specifications()
    assert [spec["task_id"] for spec in specs] == list(LEGACY_ORDER)
    upstream = {spec["task_id"]: spec["upstream"] for spec in specs}
    assert upstream["04_sas_data_products"] == [
        "01_sas_customer_segments",
        "02_sas_txn_analytics",
        "03_sas_risk_scoring",
    ]
