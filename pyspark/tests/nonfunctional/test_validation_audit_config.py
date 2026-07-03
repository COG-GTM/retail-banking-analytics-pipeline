"""Non-functional tests: ported validation rules, audit-log emission,
config-driven parameters, and abort-on-failure behaviour."""

from __future__ import annotations

import datetime as _dt

import pytest

from common.audit import (
    ETL_RUN_LOG_SCHEMA,
    PIPELINE_AUDIT_SCHEMA,
    AuditLog,
)
from common.config import PipelineConfig
from common.io import LocalDataIO
from common.validation import ValidationError, abort_on_failure, validate_table
from jobs import staging_customer_360 as job
from tests import _fixtures as fx

pytestmark = pytest.mark.nonfunctional


# --------------------------------------------------------------------------- #
# validate_table: the three ported checks + &VALIDATION_RC semantics           #
# --------------------------------------------------------------------------- #
def test_min_rows_is_fatal(spark):
    df = fx.customers(spark, [{"customer_id": 1}])
    res = validate_table(df, "T", min_rows=5)
    assert res.rc == 1 and not res.passed
    with pytest.raises(ValidationError):
        abort_on_failure(res)


def test_duplicate_key_is_fatal(spark):
    df = fx.customers(spark, [{"customer_id": 1}, {"customer_id": 1}])
    res = validate_table(df, "T", key_cols=["customer_id"], min_rows=1)
    assert res.rc == 1
    assert res.duplicate_key_groups == 1
    with pytest.raises(ValidationError):
        abort_on_failure(res)


def test_not_null_is_warning_only(spark):
    df = fx.customers(spark, [{"customer_id": 1, "first_name": None}, {"customer_id": 2, "first_name": "A"}])
    res = validate_table(df, "T", key_cols=["customer_id"], not_null=["first_name"], min_rows=1)
    assert res.rc == 0 and res.passed          # NOT NULL is non-fatal (matches SAS)
    assert res.null_counts["first_name"] == 1
    assert any(m.startswith("WARNING") for m in res.messages)
    abort_on_failure(res)                       # does not raise


def test_validation_emits_audit_records(spark):
    audit = AuditLog()
    df = fx.customers(spark, [{"customer_id": 1}])
    validate_table(df, "T", min_rows=99, audit=audit)
    assert any(r.status == "ERROR" for r in audit.records)


# --------------------------------------------------------------------------- #
# Audit-log emission: log_step, run_log_row, materialised DataFrames           #
# --------------------------------------------------------------------------- #
def test_log_step_rejects_invalid_status():
    audit = AuditLog()
    with pytest.raises(ValueError):
        audit.log_step("JOB", "BOGUS")


def test_audit_dataframes_match_schema(spark):
    audit = AuditLog()
    audit.log_step("01_job", "START", "begin")
    audit.log_step("01_job", "SUCCESS", "done", rowcount=10)
    audit.run_log_row("01_job", row_count=10)

    pa = audit.pipeline_audit_df(spark)
    rl = audit.etl_run_log_df(spark)
    assert pa.schema == PIPELINE_AUDIT_SCHEMA
    assert rl.schema == ETL_RUN_LOG_SCHEMA
    # correlation id propagates to every row
    assert {r.run_id for r in pa.collect()} == {audit.run_id}
    assert pa.count() == 3 and rl.count() == 1   # only run_log_row has start_ts


# --------------------------------------------------------------------------- #
# Config-driven parameters (no wall-clock / no hardcoding)                      #
# --------------------------------------------------------------------------- #
def test_config_lookback_is_driven_by_run_date():
    cfg = PipelineConfig(run_date=_dt.date(2026, 4, 10), lookback_months=12)
    assert cfg.lookback_start == _dt.date(2025, 4, 10)
    assert cfg.reporting_period == "2026-04"
    assert cfg.risk_score_threshold == 700


def test_config_from_env_overrides_defaults():
    cfg = PipelineConfig.from_env({"LOOKBACK_MONTHS": "6", "RISK_SCORE_THRESHOLD": "650", "RUN_DATE": "2026-01-31"})
    assert cfg.lookback_months == 6
    assert cfg.risk_score_threshold == 650
    assert cfg.run_date == _dt.date(2026, 1, 31)
    # month-end clamp through ADD_MONTHS
    assert cfg.lookback_start == _dt.date(2025, 7, 31)


# --------------------------------------------------------------------------- #
# Abort-on-failure end-to-end: a job whose validation fails must raise          #
# --------------------------------------------------------------------------- #
def test_job_aborts_when_source_empty(spark, config, data_dir, tmp_path, monkeypatch):
    io = LocalDataIO(spark, config, source_dir=data_dir, lake_dir=tmp_path)
    # Force the min-rows check to fail by making the transform output empty.
    empty = io.read_source("CUSTOMERS").limit(0)
    monkeypatch.setattr(io, "read_source", lambda t: empty if t == "CUSTOMERS" else LocalDataIO.read_source(io, t))
    with pytest.raises(ValidationError):
        job.run(spark, io, config)
