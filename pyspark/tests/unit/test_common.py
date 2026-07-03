"""Unit tests for the common/ infrastructure modules."""

from __future__ import annotations

import datetime as _dt

import pytest

from common.audit import AuditLog
from common.config import PipelineConfig, add_months
from common.validation import ValidationError, abort_on_failure, validate_table

pytestmark = pytest.mark.unit


def test_add_months_basic_and_clamping():
    assert add_months(_dt.date(2026, 4, 10), -12) == _dt.date(2025, 4, 10)
    assert add_months(_dt.date(2026, 1, 31), 1) == _dt.date(2026, 2, 28)  # clamp
    assert add_months(_dt.date(2026, 12, 15), 1) == _dt.date(2027, 1, 15)


def test_config_defaults_mirror_cfg():
    cfg = PipelineConfig()
    assert cfg.lookback_months == 12
    assert cfg.risk_score_threshold == 700
    assert cfg.db_dp == "DATA_PRODUCTS_DB"


def test_config_lookback_and_reporting_period():
    cfg = PipelineConfig(run_date=_dt.date(2026, 4, 10))
    assert cfg.lookback_start == _dt.date(2025, 4, 10)
    assert cfg.reporting_period == "2026-04"
    assert cfg.run_date_str == "2026-04-10"


def test_config_from_cfg_file(tmp_path):
    cfg_file = tmp_path / "pipeline_config.cfg"
    cfg_file.write_text(
        '# comment\n'
        'export LOOKBACK_MONTHS=6\n'
        'export RISK_SCORE_THRESHOLD=720  # inline comment\n'
        'export TD_USERNAME="${TD_USERNAME:-svc_default}"\n'
        'export RUN_DATE=$(date +%Y%m%d)\n'  # dynamic -> skipped
    )
    cfg = PipelineConfig.from_cfg_file(cfg_file, environ={})
    assert cfg.lookback_months == 6
    assert cfg.risk_score_threshold == 720
    assert cfg.td_username == "svc_default"


def test_config_env_overrides_cfg(tmp_path):
    cfg_file = tmp_path / "c.cfg"
    cfg_file.write_text('export LOOKBACK_MONTHS=6\n')
    cfg = PipelineConfig.from_cfg_file(cfg_file, environ={"LOOKBACK_MONTHS": "3"})
    assert cfg.lookback_months == 3


def test_validate_table_passes(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], "customer_id long, name string")
    res = validate_table(df, "T", key_cols=["customer_id"], not_null=["customer_id"], min_rows=1)
    assert res.passed and res.rc == 0
    assert res.row_count == 2


def test_validate_table_min_rows_fatal(spark):
    df = spark.createDataFrame([(1,)], "customer_id long")
    res = validate_table(df, "T", min_rows=5)
    assert res.rc == 1 and not res.passed
    with pytest.raises(ValidationError):
        abort_on_failure(res)


def test_validate_table_duplicate_keys_fatal(spark):
    df = spark.createDataFrame([(1,), (1,)], "customer_id long")
    res = validate_table(df, "T", key_cols=["customer_id"], min_rows=1)
    assert res.rc == 1


def test_validate_table_null_is_warning_not_fatal(spark):
    df = spark.createDataFrame([(1, None), (2, "b")], "customer_id long, name string")
    res = validate_table(df, "T", not_null=["name"], min_rows=1)
    assert res.rc == 0  # NOT NULL is non-fatal, matching the SAS macro
    assert res.null_counts["name"] == 1
    assert any(m.startswith("WARNING") for m in res.messages)


def test_audit_log_records_and_dataframes(spark):
    audit = AuditLog(run_id="run123")
    audit.log_step("JOB", "START")
    audit.log_step("JOB", "SUCCESS", "done", rowcount=10)
    audit.run_log_row("JOB", 10)
    pa = audit.pipeline_audit_df(spark)
    assert pa.count() == 3
    assert set(pa.columns) == {"run_id", "job_name", "status", "message", "row_count", "log_ts"}
    rl = audit.etl_run_log_df(spark)
    assert rl.count() == 1


def test_audit_rejects_bad_status():
    with pytest.raises(ValueError):
        AuditLog().log_step("JOB", "BOGUS")
