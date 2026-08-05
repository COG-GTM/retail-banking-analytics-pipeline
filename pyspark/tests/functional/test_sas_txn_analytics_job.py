"""Functional test for the transaction analytics job: run it on the committed extract."""

from __future__ import annotations

import dataclasses

import pytest

from common import schemas
from common.io import InMemoryDataIO
from common.job import STATUS_SUCCESS
from common.schemas import assert_schema
from common.validation import ValidationFailedError
from jobs import sas_txn_analytics

pytestmark = pytest.mark.functional


@pytest.fixture
def loaded_io(spark, reference_io, memory_io: InMemoryDataIO) -> InMemoryDataIO:
    """``ETL_STAGING_DB.STG_TXN_SUMMARY`` is a BTEQ output, so it comes from the staging extract."""

    memory_io.put_spec(schemas.STG_TXN_SUMMARY, reference_io.read_spec(schemas.STG_TXN_SUMMARY))
    return memory_io


def test_job_runs_and_writes_a_ddl_conformant_table(spark, loaded_io, config, audit):
    result = sas_txn_analytics.run(spark, loaded_io, config, audit)

    assert result.status == STATUS_SUCCESS
    assert result.return_code == 0
    assert result.target_table == "DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS"
    assert result.row_count == 500

    written = loaded_io.read_spec(schemas.TRANSACTION_ANALYTICS)
    assert_schema(written, schemas.TRANSACTION_ANALYTICS)
    assert written.count() == result.row_count


def test_job_writes_one_row_per_staging_customer(spark, loaded_io, config, audit):
    sas_txn_analytics.run(spark, loaded_io, config, audit)

    staging = loaded_io.read_spec(schemas.STG_TXN_SUMMARY)
    written = loaded_io.read_spec(schemas.TRANSACTION_ANALYTICS)
    assert written.count() == staging.select("CUSTOMER_ID").distinct().count()
    assert written.select("CUSTOMER_ID").distinct().count() == written.count()


def test_job_stamps_the_run_metadata_from_the_pinned_run_date(spark, loaded_io, config, audit):
    sas_txn_analytics.run(spark, loaded_io, config, audit)

    written = loaded_io.read_spec(schemas.TRANSACTION_ANALYTICS)
    metadata = written.select("REPORTING_PERIOD", "MODEL_VERSION", "EFFECTIVE_DATE").distinct()
    assert metadata.count() == 1
    row = metadata.collect()[0]
    assert row["REPORTING_PERIOD"] == sas_txn_analytics.reporting_period(config.run_date)
    assert row["MODEL_VERSION"] == sas_txn_analytics.MODEL_VERSION
    assert row["EFFECTIVE_DATE"] == config.run_date


def test_job_writes_the_current_period_partition(spark, loaded_io, config, audit):
    """The load is a delete-by-period + append, expressed as a partition overwrite."""

    sas_txn_analytics.run(spark, loaded_io, config, audit)

    written = loaded_io.read_spec(schemas.TRANSACTION_ANALYTICS)
    periods = {row["REPORTING_PERIOD"] for row in written.select("REPORTING_PERIOD").collect()}
    assert periods == {"2026-04"}


def test_job_emits_audit_records_and_passes_validation(spark, loaded_io, config, audit):
    result = sas_txn_analytics.run(spark, loaded_io, config, audit)

    assert [record.status for record in audit.steps][-1] == "SUCCESS"
    assert {record.status for record in audit.steps} == {"START", "SUCCESS"}
    assert audit.runs[-1].job_name == sas_txn_analytics.JOB_NAME
    assert audit.runs[-1].row_count == result.row_count
    assert result.validation is not None and result.validation.passed
    assert result.validation.row_count == 500
    assert result.validation.failures == []


def test_job_aborts_and_logs_when_validation_fails(spark, loaded_io, config, audit):
    """``%validate_table`` min-rows failure short-circuits into ``%abort cancel``."""

    strict = dataclasses.replace(config, min_rows=10_000)
    with pytest.raises(ValidationFailedError):
        sas_txn_analytics.run(spark, loaded_io, strict, audit)

    assert audit.steps[-1].status == "ERROR"
    assert schemas.TRANSACTION_ANALYTICS.qualified_name not in loaded_io.tables
