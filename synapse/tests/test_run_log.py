import logging

from pipeline_utils.config import load_config
from pipeline_utils.run_log import ERROR, START, SUCCESS, RunLogger

ENV = {
    "SNOWFLAKE_ACCOUNT": "acme-eu",
    "SNOWFLAKE_USER": "SVC_SYNAPSE",
    "AZURE_KEY_VAULT_URL": "https://kv.vault.azure.net/",
}


class RecordingIO:
    def __init__(self):
        self.appended = []

    def append_table(self, df, schema, table):
        self.appended.append((schema, table, df.collect()))


def make_logger(spark, io=None):
    return RunLogger(spark, "04_MASTER_PROFILE", load_config(ENV), io, run_id="run-1")


def test_log_step_records_audit_trail(spark, caplog):
    run_logger = make_logger(spark)
    with caplog.at_level(logging.INFO, logger="retail_banking.pipeline"):
        run_logger.log_step("04_MASTER_PROFILE", START, "Building golden record")
        run_logger.log_step("04_MASTER_PROFILE", SUCCESS, "Built", rowcount=407)

    trail = run_logger.audit_trail()
    assert [e["STATUS"] for e in trail] == [START, SUCCESS]
    assert trail[1]["ROW_COUNT"] == 407
    assert "Building golden record" in caplog.text
    assert "Rows: 407" in caplog.text


def test_flush_appends_entries_to_run_log_table(spark):
    io = RecordingIO()
    run_logger = make_logger(spark, io)
    run_logger.log_step("04_MASTER_PROFILE", ERROR, "Validation failed")

    assert run_logger.flush() == 1
    schema, table, rows = io.appended[0]
    assert (schema, table) == ("ETL_STAGING", "PIPELINE_RUN_LOG")
    assert rows[0]["RUN_ID"] == "run-1"
    assert rows[0]["STATUS"] == ERROR
    assert rows[0]["MESSAGE"] == "Validation failed"
    assert run_logger.flush() == 0


def test_flush_without_entries_is_a_noop(spark):
    io = RecordingIO()
    assert make_logger(spark, io).flush() == 0
    assert io.appended == []
