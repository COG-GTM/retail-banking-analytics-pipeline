"""Functional test for ``jobs.stg_txn_summary``: run it on the committed sample extract."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from common import schemas
from common.io import InMemoryDataIO
from common.job import STATUS_SUCCESS
from common.schemas import assert_schema
from jobs import stg_txn_summary

pytestmark = pytest.mark.functional


@pytest.fixture
def loaded_io(spark, source_io, memory_io: InMemoryDataIO) -> InMemoryDataIO:
    for spec in (schemas.TRANSACTIONS, schemas.TRANSACTION_TYPES, schemas.ACCOUNTS):
        memory_io.put_spec(spec, source_io.read_spec(spec))
    return memory_io


def test_job_runs_and_writes_a_ddl_conformant_table(spark, loaded_io, config, audit):
    result = stg_txn_summary.run(spark, loaded_io, config, audit)

    assert result.status == STATUS_SUCCESS
    assert result.return_code == 0
    assert result.target_table == "ETL_STAGING_DB.STG_TXN_SUMMARY"
    assert result.row_count > 0

    written = loaded_io.read_spec(schemas.STG_TXN_SUMMARY)
    assert_schema(written, schemas.STG_TXN_SUMMARY)
    assert written.count() == result.row_count


def test_job_emits_one_row_per_account_over_the_configured_window(spark, loaded_io, config, audit):
    stg_txn_summary.run(spark, loaded_io, config, audit)

    written = loaded_io.read_spec(schemas.STG_TXN_SUMMARY)
    assert written.select("ACCOUNT_ID").distinct().count() == written.count()

    periods = written.select("SUMMARY_PERIOD_START", "SUMMARY_PERIOD_END").distinct().collect()
    assert len(periods) == 1
    assert periods[0]["SUMMARY_PERIOD_END"] == config.run_date
    assert periods[0]["SUMMARY_PERIOD_START"].year == config.run_date.year - 1


def test_job_output_is_internally_consistent(spark, loaded_io, config, audit):
    stg_txn_summary.run(spark, loaded_io, config, audit)

    written = loaded_io.read_spec(schemas.STG_TXN_SUMMARY)
    bad = written.filter(
        (F.col("TXN_COUNT_TOTAL") < F.col("TXN_COUNT_DEBIT") + F.col("TXN_COUNT_CREDIT"))
        | (F.col("DAYS_SINCE_LAST_TXN") < 0)
        | (F.col("PCT_ATM") + F.col("PCT_POS") + F.col("PCT_WEB") + F.col("PCT_MOBILE") > 100.01)
    )

    assert bad.count() == 0


def test_job_emits_audit_records(spark, loaded_io, config, audit):
    result = stg_txn_summary.run(spark, loaded_io, config, audit)

    assert [record.status for record in audit.steps][-1] == "SUCCESS"
    assert audit.runs[-1].job_name == stg_txn_summary.JOB_NAME
    assert audit.runs[-1].step_name == stg_txn_summary.STEP_NAME
    assert audit.runs[-1].row_count == result.row_count
    assert result.validation is not None and result.validation.passed
    assert [check.status for check in result.validation.checks] == ["PASS"] * len(
        result.validation.checks
    )
