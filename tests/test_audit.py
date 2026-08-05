"""Tests for the ``%init_audit`` / ``%log_step`` port."""

from __future__ import annotations

import logging

import pytest
from pyspark.sql import SparkSession

from risk_scoring.audit import PIPELINE_AUDIT_SCHEMA, AuditLog


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("risk_scoring_tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def test_records_accumulate_in_order():
    audit = AuditLog("03_RISK_SCORING")
    audit.log_step(step="03_RISK_SCORING", status="START", msg="Model version RISK_V4.0")
    audit.log_step(step="03_RISK_SCORING", status="SUCCESS", msg="Extracted", rowcount=407)

    records = audit.records
    assert [r.status for r in records] == ["START", "SUCCESS"]
    assert records[0].row_count is None
    assert records[1].row_count == 407
    assert records[1].job_name == "03_RISK_SCORING"
    assert records[0].log_ts <= records[1].log_ts


def test_records_property_is_a_copy():
    audit = AuditLog("JOB")
    audit.log_step(step="S", status="START")
    audit.records.clear()
    assert len(audit.records) == 1


@pytest.mark.parametrize("status", ["START", "SUCCESS", "WARNING", "ERROR"])
def test_valid_statuses_accepted(status):
    audit = AuditLog("JOB")
    audit.log_step(step="S", status=status)
    assert audit.records[-1].status == status


@pytest.mark.parametrize("status", ["", "start", "INFO", "FAILED", None])
def test_invalid_status_rejected(status):
    audit = AuditLog("JOB")
    with pytest.raises(ValueError, match="Invalid audit status"):
        audit.log_step(step="S", status=status)
    assert audit.records == []


@pytest.mark.parametrize(
    ("status", "level"),
    [
        ("START", logging.INFO),
        ("SUCCESS", logging.INFO),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
    ],
)
def test_log_level_derived_from_status(status, level, caplog):
    audit = AuditLog("JOB")
    with caplog.at_level(logging.INFO, logger="risk_scoring.audit"):
        audit.log_step(step="STEP_A", status=status, msg="hello", rowcount=3)

    record = next(r for r in caplog.records if r.name == "risk_scoring.audit")
    assert record.levelno == level
    assert "step=STEP_A" in record.getMessage()
    assert f"status={status}" in record.getMessage()
    assert "rows=3" in record.getMessage()
    assert audit.run_id in record.getMessage()


def test_log_step_does_not_print(capsys, caplog):
    audit = AuditLog("JOB")
    with caplog.at_level(logging.INFO, logger="risk_scoring.audit"):
        audit.log_step(step="STEP_A", status="SUCCESS", rowcount=1)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_to_dataframe_schema_and_contents(spark):
    audit = AuditLog("03_RISK_SCORING")
    audit.log_step(step="03_RISK_SCORING", status="START", msg="go")
    audit.log_step(step="VALIDATE", status="WARNING", msg="nulls", rowcount=2)

    df = audit.to_dataframe(spark)
    assert df.schema == PIPELINE_AUDIT_SCHEMA
    rows = df.collect()
    assert [r.JOB_NAME for r in rows] == ["03_RISK_SCORING", "VALIDATE"]
    assert [r.STATUS for r in rows] == ["START", "WARNING"]
    assert [r.ROW_COUNT for r in rows] == [None, 2]
    assert rows[1].MESSAGE == "nulls"


def test_to_dataframe_empty_trail_keeps_structure(spark):
    """``%init_audit`` created an empty dataset *with structure*."""
    df = AuditLog("03_RISK_SCORING").to_dataframe(spark)
    assert df.schema == PIPELINE_AUDIT_SCHEMA
    assert df.count() == 0
