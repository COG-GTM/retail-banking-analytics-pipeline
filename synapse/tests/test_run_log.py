from datetime import datetime, timezone

import pytest

from pipeline_utils.run_log import InMemoryRunLogSink, RunLogger, Status

FIXED_TS = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)


def make_logger(**kwargs):
    sink = InMemoryRunLogSink()
    logger = RunLogger(
        job_name="04_MASTER_PROFILE",
        run_id="run-1",
        sink=sink,
        clock=lambda: FIXED_TS,
        **kwargs,
    )
    return logger, sink


def test_log_step_records_and_flushes():
    logger, sink = make_logger()

    logger.log_step("04_MASTER_PROFILE", Status.START, "Building golden record")
    logger.log_step("04_MASTER_PROFILE", Status.SUCCESS, "Built", rowcount=1234)

    assert [record.status for record in sink.written] == [Status.START, Status.SUCCESS]
    assert sink.written[1].row_count == 1234
    assert sink.written[1].log_ts == FIXED_TS
    assert sink.written[0].run_id == "run-1"


def test_buffered_records_flush_once():
    logger, sink = make_logger(autoflush=False)

    logger.log_step("STEP", Status.START)
    logger.log_step("STEP", Status.ERROR, "boom")

    assert sink.written == []
    assert logger.flush() == 2
    assert logger.flush() == 0
    assert [record.status for record in sink.written] == [Status.START, Status.ERROR]


def test_audit_trail_keeps_history_when_not_flushed():
    logger, _ = make_logger(autoflush=False)
    logger.log_step("STEP", Status.SUCCESS, "ok", rowcount=1)

    trail = logger.audit_trail()
    assert len(trail) == 1
    assert trail[0].as_row() == ("run-1", "STEP", Status.SUCCESS, "ok", 1, FIXED_TS)


def test_message_and_job_name_are_truncated_like_the_sas_audit_columns():
    logger, sink = make_logger()
    logger.log_step("S" * 60, Status.SUCCESS, "m" * 300)

    assert len(sink.written[0].job_name) == RunLogger.JOB_NAME_MAX_LEN
    assert len(sink.written[0].message) == RunLogger.MESSAGE_MAX_LEN


def test_unknown_status_rejected():
    logger, _ = make_logger()
    with pytest.raises(ValueError):
        logger.log_step("STEP", "DONE")


def test_logger_without_sink_is_a_noop_flush():
    logger = RunLogger(job_name="J")
    logger.log_step("STEP", Status.SUCCESS)
    assert logger.flush() == 0
    assert len(logger.records) == 1
