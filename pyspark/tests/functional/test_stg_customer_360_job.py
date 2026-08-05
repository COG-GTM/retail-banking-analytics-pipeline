"""Functional test for the reference job: run it on the committed sample extract."""

from __future__ import annotations

import pytest

from common import schemas
from common.io import InMemoryDataIO
from common.job import STATUS_SUCCESS
from common.schemas import assert_schema
from jobs import stg_customer_360

pytestmark = pytest.mark.functional


@pytest.fixture
def loaded_io(spark, source_io, memory_io: InMemoryDataIO) -> InMemoryDataIO:
    for spec in (schemas.CUSTOMERS, schemas.ADDRESSES, schemas.ACCOUNTS):
        memory_io.put_spec(spec, source_io.read_spec(spec))
    return memory_io


def test_job_runs_and_writes_a_ddl_conformant_table(spark, loaded_io, config, audit):
    result = stg_customer_360.run(spark, loaded_io, config, audit)

    assert result.status == STATUS_SUCCESS
    assert result.return_code == 0
    assert result.target_table == "ETL_STAGING_DB.STG_CUSTOMER_360"
    assert result.row_count > 0

    written = loaded_io.read_spec(schemas.STG_CUSTOMER_360)
    assert_schema(written, schemas.STG_CUSTOMER_360)
    assert written.count() == result.row_count


def test_job_only_keeps_active_and_inactive_customers(spark, loaded_io, config, audit):
    stg_customer_360.run(spark, loaded_io, config, audit)

    written = loaded_io.read_spec(schemas.STG_CUSTOMER_360)
    statuses = {
        row["CUSTOMER_STATUS"] for row in written.select("CUSTOMER_STATUS").distinct().collect()
    }
    assert statuses <= {"A", "I"}
    assert written.select("CUSTOMER_ID").distinct().count() == written.count()


def test_job_emits_audit_records(spark, loaded_io, config, audit):
    result = stg_customer_360.run(spark, loaded_io, config, audit)

    assert [record.status for record in audit.steps][-1] == "SUCCESS"
    assert audit.runs[-1].job_name == stg_customer_360.JOB_NAME
    assert audit.runs[-1].row_count == result.row_count
    assert result.validation is not None and result.validation.passed
