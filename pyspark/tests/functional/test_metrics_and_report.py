"""Functional tier: the metrics collector and the HTML renderer.

The report is a deliverable, so the properties asserted here are the ones that make it usable:
the numbers come from the metrics JSON (never hand-written), and the file is self-contained —
a report that fetches a stylesheet or a data file renders blank when opened from disk.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.io import InMemoryDataIO
from common.job import STATUS_SUCCESS, JobResult, default_schema_map
from orchestration import report as report_module
from orchestration.metrics import (
    LINEAGE,
    PROFILED_SPECS,
    VALIDATION_ARGUMENTS,
    audit_trail,
    business_insights,
    collect_metrics,
    deployment_topology,
    profile_table,
    scale_recommendations,
    validate_persisted,
    write_metrics,
)
from orchestration.pipeline import PIPELINE, PipelineRun, run_pipeline
from orchestration.synthetic_data import generate_sources, load_sources

pytestmark = pytest.mark.functional

METRICS_CUSTOMERS = 300
METRICS_RUN_DATE = date(2026, 4, 10)


@pytest.fixture(scope="module")
def metrics_bundle(spark: SparkSession, repo_root):
    """One small synthetic run, then the metrics gathered from what it persisted."""

    config = PipelineConfig.from_cfg_file(
        repo_root / "config" / "pipeline_config.cfg", run_date=METRICS_RUN_DATE, min_rows=10
    )
    io = InMemoryDataIO()
    load_sources(io, generate_sources(spark, METRICS_CUSTOMERS, run_date=METRICS_RUN_DATE))
    run = run_pipeline(spark, io, config, AuditLog(run_timestamp=config.run_timestamp))
    assert run.return_code == 0, run.summary_lines()
    metrics = collect_metrics(
        spark,
        io,
        config,
        run,
        jdbc_url="jdbc:postgresql://localhost:5433/retail_banking",
        schema_map=default_schema_map(config),
    )
    return metrics, io, run, config


def test_every_profiled_table_is_present_with_a_row_count(metrics_bundle) -> None:
    metrics, _, _, _ = metrics_bundle
    profiled = {entry["table"] for entry in metrics["tables"]}

    assert profiled == {spec.qualified_name for spec in PROFILED_SPECS}
    assert all(entry["row_count"] > 0 for entry in metrics["tables"])


def test_column_profiles_cover_the_whole_contract(metrics_bundle) -> None:
    metrics, _, _, _ = metrics_bundle
    entry = next(e for e in metrics["tables"] if e["table"] == "ETL_STAGING_DB.STG_CUSTOMER_360")

    assert [column["name"] for column in entry["columns"]] == [
        column.name for column in schemas.STG_CUSTOMER_360.columns
    ]
    assert entry["primary_index"] == ["CUSTOMER_ID"]
    assert len(entry["sample"]) == 5


def test_profiles_are_json_serialisable_dates_and_decimals_included(metrics_bundle) -> None:
    metrics, _, _, _ = metrics_bundle
    encoded = json.dumps(metrics)  # no `default=` — a Decimal or date would raise here

    assert '"ETL_STAGING_DB.STG_CUSTOMER_360"' in encoded


def test_validation_reruns_against_the_persisted_tables(metrics_bundle) -> None:
    metrics, _, _, _ = metrics_bundle
    validated = {entry["table"] for entry in metrics["validations"]}

    assert validated == set(VALIDATION_ARGUMENTS)
    assert all(entry["rc"] == 0 for entry in metrics["validations"]), metrics["validations"]


def test_a_failing_check_is_reported_not_swallowed(spark: SparkSession) -> None:
    empty = spark.createDataFrame([], schemas.STG_CUSTOMER_360.spark_schema())
    result = validate_persisted(empty, schemas.STG_CUSTOMER_360, min_rows=1)

    assert result["rc"] == 1
    assert any(check["status"] == "FAIL" for check in result["checks"])


def test_profile_of_an_empty_table_does_not_divide_by_zero(spark: SparkSession) -> None:
    empty = spark.createDataFrame([], schemas.CUSTOMERS.spark_schema())
    profile = profile_table(empty, schemas.CUSTOMERS)

    assert profile["row_count"] == 0
    assert all(column["null_pct"] == 0.0 for column in profile["columns"])
    assert profile["sample"] == []


def test_lineage_covers_every_job_and_names_its_legacy_source() -> None:
    assert {edge.job for edge in LINEAGE} == {node.name for node in PIPELINE}
    for edge in LINEAGE:
        node = next(n for n in PIPELINE if n.name == edge.job)
        assert edge.legacy_source == node.legacy_source
        assert edge.output == node.target
        assert edge.business_rule.strip()


def test_insights_are_derived_from_the_persisted_products(metrics_bundle) -> None:
    metrics, io, _, _ = metrics_bundle
    insights = metrics["insights"]

    assert insights["segment_distribution"]
    assert insights["risk_tier_distribution"]
    assert (
        insights["completeness"]["TOTAL"]
        == io.read_table("DATA_PRODUCTS_DB", "CUSTOMER_MASTER_PROFILE").count()
    )


def test_insights_are_empty_when_nothing_has_been_written() -> None:
    assert business_insights(InMemoryDataIO()) == {}
    assert audit_trail(InMemoryDataIO()) == []


def test_the_audit_trail_comes_from_the_persisted_table(metrics_bundle) -> None:
    metrics, _, _, _ = metrics_bundle
    jobs_logged = {row["JOB_NAME"] for row in metrics["audit_trail"]}

    assert {node.name for node in PIPELINE} <= jobs_logged


def test_topology_records_the_deployment_and_the_spark_settings(
    spark: SparkSession, config: PipelineConfig
) -> None:
    topology = deployment_topology(
        spark, config, jdbc_url="jdbc:postgresql://host/db", schema_map={"ETL_STAGING_DB": "etl"}
    )

    assert topology["database"] == "PostgreSQL"
    assert topology["jdbc_url"] == "jdbc:postgresql://host/db"
    assert topology["lookback_months"] == 12
    assert topology["risk_score_threshold"] == 700
    assert topology["min_rows_production"] == 1000
    assert topology["spark_conf"]["spark.sql.adaptive.enabled"] == "true"


def test_scale_recommendations_extrapolate_from_the_measured_volume() -> None:
    run = PipelineRun(run_timestamp="20260410_000000", run_date="2026-04-10")
    run.results.append(
        JobResult(
            job_name="01_stg_customer_360",
            status=STATUS_SUCCESS,
            row_count=500,
            end_ts=datetime.now(),
        )
    )
    recommendations = scale_recommendations(
        run,
        {"ETL_STAGING_DB.STG_CUSTOMER_360": 500, "TXN_PROCESSING_DB.TRANSACTIONS": 80_000},
    )
    topics = {entry["topic"] for entry in recommendations}

    assert "Measured scale" in topics
    assert "20,000x" in next(
        entry["detail"] for entry in recommendations if entry["topic"].startswith("Linear")
    )


def test_scale_recommendations_survive_an_empty_run() -> None:
    run = PipelineRun(run_timestamp="t", run_date="2026-04-10")
    assert scale_recommendations(run, {})  # no division by zero


def test_metrics_round_trip_through_the_json_artifact(metrics_bundle, tmp_path) -> None:
    metrics, _, _, _ = metrics_bundle
    path = write_metrics(metrics, tmp_path / "nested" / "metrics.json")

    assert json.loads(path.read_text(encoding="utf-8"))["tables"]


# ------------------------------------------------------------------------------------------
# HTML report
# ------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rendered(metrics_bundle) -> str:
    metrics, _, _, _ = metrics_bundle
    return report_module.render_html(metrics)


def test_the_report_is_self_contained(rendered: str) -> None:
    """No external stylesheet, script or image: the file must render from disk, offline."""

    assert not re.search(r"<script[^>]+\ssrc=", rendered)
    assert not re.search(r"<link[^>]+\shref=", rendered)
    assert not re.search(r"<img[^>]+\ssrc=", rendered)
    assert "http://" not in rendered
    assert "<style>" in rendered


def test_the_report_shows_every_job_table_and_lineage_hop(rendered: str, metrics_bundle) -> None:
    metrics, _, _, _ = metrics_bundle

    for node in PIPELINE:
        assert node.name in rendered
        assert node.legacy_source in rendered
    for entry in metrics["tables"]:
        assert str(entry["table"]) in rendered


def test_reported_row_counts_are_the_metrics_row_counts(rendered: str, metrics_bundle) -> None:
    metrics, _, _, _ = metrics_bundle
    customers = metrics["row_counts"]["ETL_STAGING_DB.STG_CUSTOMER_360"]

    assert f"{customers:,}" in rendered


def test_the_report_surfaces_the_sample_threshold_caveat(rendered: str) -> None:
    assert "MIGRATION_NOTES" in rendered
    assert "1,000" in rendered or "1000" in rendered


def test_html_is_escaped(metrics_bundle) -> None:
    metrics = dict(metrics_bundle[0])
    metrics["topology"] = {**metrics["topology"], "jdbc_url": "<script>alert(1)</script>"}

    html = report_module.render_html(metrics)

    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_the_renderer_survives_a_metrics_file_from_a_dry_run() -> None:
    minimal = {
        "generated_at": "2026-04-10T00:00:00",
        "topology": {},
        "run": {"return_code": 0, "elapsed_seconds": 0.0},
        "jobs": [],
        "lineage": [],
        "tables": [],
        "row_counts": {},
        "validations": [],
        "audit_trail": [],
        "insights": {},
        "scale_recommendations": [],
    }
    html = report_module.render_html(minimal)

    assert html.startswith("<!DOCTYPE html>")


def test_cli_renders_the_report_from_the_metrics_json(metrics_bundle, tmp_path) -> None:
    metrics, _, _, _ = metrics_bundle
    metrics_path = write_metrics(metrics, tmp_path / "metrics.json")
    out = tmp_path / "report.html"

    assert report_module.main(["--metrics-json", str(metrics_path), "--out", str(out)]) == 0
    assert out.stat().st_size > 10_000
    assert out.read_text(encoding="utf-8").rstrip().endswith("</html>")
