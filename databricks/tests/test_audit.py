"""Unit tests for the %log_step / ETL_RUN_LOG replacement."""

from __future__ import annotations

import json
import logging

import pytest

from shared.audit import RUN_LOG_TABLE, ensure_run_log, log_step, step
from shared.logging_utils import get_logger, log_event


def test_step_writes_start_and_success_rows(spark, cfg) -> None:
    ensure_run_log(spark, cfg)
    with step(spark, cfg, "test_job", "UNIT_STEP") as ctx:
        ctx["row_count"] = 42

    rows = (
        spark.table(cfg.ops(RUN_LOG_TABLE))
        .where("JOB_NAME = 'test_job' AND STEP_NAME = 'UNIT_STEP'")
        .orderBy("STATUS")
        .collect()
    )
    statuses = {r.STATUS: r for r in rows}
    assert set(statuses) == {"START", "SUCCESS"}
    assert statuses["SUCCESS"].ROW_COUNT == 42
    assert cfg.run_id == statuses["SUCCESS"].RUN_ID
    assert statuses["SUCCESS"].DURATION_SEC is not None


def test_step_records_an_error_row_and_reraises(spark, cfg) -> None:
    ensure_run_log(spark, cfg)
    with pytest.raises(RuntimeError), step(spark, cfg, "test_job", "FAILING_STEP"):
        raise RuntimeError("boom")

    row = (
        spark.table(cfg.ops(RUN_LOG_TABLE)).where("STEP_NAME = 'FAILING_STEP'").orderBy("STATUS").collect()
    )
    assert [r.STATUS for r in row] == ["ERROR", "START"]
    assert "RuntimeError: boom" in [r.MESSAGE for r in row if r.STATUS == "ERROR"][0]


def test_log_step_never_records_customer_attributes(spark, cfg) -> None:
    """Audit rows carry counts and step names only — never PII."""
    ensure_run_log(spark, cfg)
    log_step(spark, cfg, "test_job", "PII_CHECK", "SUCCESS", row_count=7)
    row = spark.table(cfg.ops(RUN_LOG_TABLE)).where("STEP_NAME = 'PII_CHECK'").collect()[0]
    assert set(row.asDict()) == {
        "RUN_ID",
        "JOB_NAME",
        "STEP_NAME",
        "STATUS",
        "MESSAGE",
        "ROW_COUNT",
        "DURATION_SEC",
        "START_TS",
        "END_TS",
    }


def test_log_event_emits_one_json_object_per_line() -> None:
    logger = get_logger("test_logger")
    captured: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    handler = _Capture()
    logger.addHandler(handler)
    try:
        log_event(logger, "etl_step", run_id="r1", step="S", row_count=None)
    finally:
        logger.removeHandler(handler)

    payload = json.loads(captured[-1])
    assert payload["event"] == "etl_step"
    assert payload["run_id"] == "r1"
    assert "row_count" not in payload  # None fields are dropped
    assert "ts" in payload
