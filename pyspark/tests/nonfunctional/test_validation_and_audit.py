"""Ports of ``%validate_table`` and ``%log_step`` keep the legacy severities and abort points."""

from __future__ import annotations

from datetime import datetime

import pytest

from common import schemas
from common.audit import AuditLog
from common.io import InMemoryDataIO
from common.validation import (
    FAIL,
    PASS,
    WARN,
    ValidationFailedError,
    abort_on_failure,
    validate_table,
)

pytestmark = pytest.mark.nonfunctional


@pytest.fixture
def sample(make_df):
    return make_df(
        schemas.STG_CUSTOMER_360,
        [
            {"CUSTOMER_ID": 1, "CITY": "A"},
            {"CUSTOMER_ID": 2, "CITY": None},
        ],
    )


def test_min_rows_failure_short_circuits(sample):
    result = validate_table(sample, table="T", key_cols=("CUSTOMER_ID",), min_rows=1000)

    assert result.rc == 1
    assert [check.name for check in result.checks] == ["min_rows"]
    assert result.checks[0].status == FAIL
    assert "minimum: 1000" in result.checks[0].detail


def test_duplicate_keys_fail_and_short_circuit(sample):
    duplicated = sample.unionByName(sample)

    result = validate_table(duplicated, table="T", key_cols=("CUSTOMER_ID",), not_null=("CITY",))

    assert result.rc == 1
    assert [check.name for check in result.checks] == ["min_rows", "key_uniqueness"]
    assert result.failures[0].name == "key_uniqueness"


def test_nulls_are_warnings_not_failures(sample):
    """``%validate_table`` logs NOT NULL violations as WARNING and leaves VALIDATION_RC at 0."""

    result = validate_table(
        sample, table="T", key_cols=("CUSTOMER_ID",), not_null=("CUSTOMER_ID", "CITY")
    )

    assert result.rc == 0
    assert result.passed
    statuses = {check.name: check.status for check in result.checks}
    assert statuses["not_null.CITY"] == WARN
    assert statuses["not_null.CUSTOMER_ID"] == PASS
    assert len(result.warnings) == 1
    assert result.as_dict()["row_count"] == 2


def test_abort_on_failure_is_the_port_of_abort_cancel(sample):
    passing = validate_table(sample, table="T", min_rows=1)
    assert abort_on_failure(passing) is passing

    failing = validate_table(sample, table="T", min_rows=99)
    with pytest.raises(ValidationFailedError, match="validation failed for T"):
        abort_on_failure(failing)


def test_audit_log_records_steps_and_runs(spark, caplog):
    audit = AuditLog(run_timestamp="20260410_000000")
    start = datetime(2026, 4, 10, 1, 0, 0)
    end = datetime(2026, 4, 10, 1, 2, 30)

    audit.log_step("01_job", "START", "starting")
    audit.log_step("01_job", "SUCCESS", "done", rowcount=42, now=end)
    audit.log_run("01_job", "FULL_LOAD", "SUCCESS", 42, start, end)

    assert [record.status for record in audit.steps] == ["START", "SUCCESS"]
    assert audit.steps[1].row_count == 42
    assert audit.runs[0].step_name == "FULL_LOAD"
    assert audit.as_dicts()["runs"][0]["row_count"] == 42

    frames = audit.steps_dataframe(spark)
    assert frames.columns == list(schemas.PIPELINE_AUDIT.column_names)
    assert frames.count() == 2
    assert audit.runs_dataframe(spark).columns == list(schemas.ETL_RUN_LOG.column_names)


def test_audit_flush_persists_both_tables(spark):
    audit = AuditLog()
    audit.log_step("01_job", "SUCCESS", "done", rowcount=1)
    audit.log_run("01_job", "FULL_LOAD", "SUCCESS", 1, datetime.now(), datetime.now())
    io = InMemoryDataIO()

    written = audit.flush(spark, io)

    assert written == {"PIPELINE_AUDIT": 1, "ETL_RUN_LOG": 1}
    assert io.read_spec(schemas.PIPELINE_AUDIT).collect()[0]["JOB_NAME"] == "01_job"
    assert io.read_spec(schemas.ETL_RUN_LOG).collect()[0]["STATUS"] == "SUCCESS"


def test_empty_audit_still_produces_conformant_frames(spark):
    audit = AuditLog()

    assert audit.steps_dataframe(spark).count() == 0
    assert audit.runs_dataframe(spark).count() == 0
