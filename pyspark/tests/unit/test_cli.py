"""CLI + Airflow-wiring smoke tests (orchestration entry points)."""

from __future__ import annotations

import pytest

from orchestration import airflow_dag, run_pipeline

pytestmark = pytest.mark.unit


def test_dry_run_lists_all_steps(capsys):
    rc = run_pipeline.main(["--source-dir", "x", "--lake-dir", "y", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    for step in ("01_stg_customer_360", "03_stg_risk_factors", "04_customer_master_profile"):
        assert step in out


def test_cli_full_run_on_sample_data(spark, data_dir, tmp_path, capsys):
    lake = tmp_path / "lake"
    rc = run_pipeline.main([
        "--source-dir", str(data_dir),
        "--lake-dir", str(lake),
        "--run-date", "2026-04-10",
    ])
    assert rc == 0
    assert "PIPELINE SUCCESS" in capsys.readouterr().out


def test_airflow_dag_optional_import():
    # Airflow is not a dependency here, so build_dag() returns None gracefully.
    assert airflow_dag.build_dag() is None
    assert airflow_dag.dag is None
