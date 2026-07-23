"""Unit tests for common.audit (ETL run log + structured JSON logging)."""
from __future__ import annotations

import json
import logging

import pytest

from common import audit
from common.audit import AUDIT_TABLE, init_audit, log_step


@pytest.fixture()
def captured_logs():
    """Collect records emitted by the audit logger as a list."""
    records: list[logging.LogRecord] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = audit._get_logger()
    handler = _ListHandler()
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)


def test_init_audit_creates_table(spark, cfg):
    table = init_audit(spark, cfg)
    assert table == f"{cfg.catalog}.{cfg.schema_stg}.{AUDIT_TABLE}"
    assert spark.catalog.tableExists(table)

    cols = {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}
    assert cols == {
        "run_id": "string",
        "job_name": "string",
        "step": "string",
        "status": "string",
        "row_count": "bigint",
        "message": "string",
        "log_ts": "timestamp",
    }
    assert spark.table(table).count() == 0


def test_init_audit_is_idempotent(spark, cfg):
    init_audit(spark, cfg)
    log_step(spark, cfg, "run-1", "job", "step", "START")
    # second init must not recreate/clear the table
    init_audit(spark, cfg)
    table = f"{cfg.catalog}.{cfg.schema_stg}.{AUDIT_TABLE}"
    assert spark.table(table).count() == 1


def test_log_step_appends_row(spark, cfg):
    init_audit(spark, cfg)
    log_step(
        spark,
        cfg,
        run_id="run-42",
        job_name="customer_seg",
        step="LOAD",
        status="SUCCESS",
        row_count=125000,
        message="loaded ok",
    )
    table = f"{cfg.catalog}.{cfg.schema_stg}.{AUDIT_TABLE}"
    rows = spark.table(table).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.run_id == "run-42"
    assert row.job_name == "customer_seg"
    assert row.step == "LOAD"
    assert row.status == "SUCCESS"
    assert row.row_count == 125000
    assert row.message == "loaded ok"
    assert row.log_ts is not None


def test_log_step_multiple_appends(spark, cfg):
    init_audit(spark, cfg)
    log_step(spark, cfg, "run-1", "job", "START", "START")
    log_step(spark, cfg, "run-1", "job", "END", "SUCCESS", row_count=10)
    table = f"{cfg.catalog}.{cfg.schema_stg}.{AUDIT_TABLE}"
    assert spark.table(table).count() == 2


def test_log_step_null_optionals(spark, cfg):
    init_audit(spark, cfg)
    log_step(spark, cfg, "run-1", "job", "step", "START")
    table = f"{cfg.catalog}.{cfg.schema_stg}.{AUDIT_TABLE}"
    row = spark.table(table).collect()[0]
    assert row.row_count is None
    assert row.message is None


def test_log_step_emits_json_log(spark, cfg, captured_logs):
    init_audit(spark, cfg)
    log_step(spark, cfg, "run-99", "job_x", "VALIDATE", "SUCCESS", row_count=7)

    assert captured_logs, "expected a log record to be emitted"
    payload = json.loads(captured_logs[-1].getMessage())
    assert payload["run_id"] == "run-99"
    assert payload["job_name"] == "job_x"
    assert payload["step"] == "VALIDATE"
    assert payload["status"] == "SUCCESS"
    assert payload["row_count"] == 7
    assert "log_ts" in payload


def test_log_step_error_status_logs_at_error_level(spark, cfg, captured_logs):
    init_audit(spark, cfg)
    log_step(spark, cfg, "run-1", "job", "step", "ERROR", message="boom")
    assert captured_logs[-1].levelno == logging.ERROR


def test_log_step_rejects_invalid_status(spark, cfg):
    init_audit(spark, cfg)
    with pytest.raises(ValueError):
        log_step(spark, cfg, "run-1", "job", "step", "BOGUS")
