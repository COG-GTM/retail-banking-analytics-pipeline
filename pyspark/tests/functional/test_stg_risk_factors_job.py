"""Functional test for ``03_stg_risk_factors``: run it on the committed sample extract."""

from __future__ import annotations

import pytest

from common import schemas
from common.io import InMemoryDataIO
from common.job import STATUS_SUCCESS
from common.schemas import assert_schema
from jobs import stg_risk_factors

pytestmark = pytest.mark.functional


@pytest.fixture
def loaded_io(spark, source_io, memory_io: InMemoryDataIO) -> InMemoryDataIO:
    for spec in (
        schemas.CUSTOMERS,
        schemas.ACCOUNTS,
        schemas.CUSTOMER_BUREAU_SCORES,
        schemas.TRANSACTIONS,
        schemas.TRANSACTION_TYPES,
    ):
        memory_io.put_spec(spec, source_io.read_spec(spec))
    return memory_io


def test_job_runs_and_writes_a_ddl_conformant_table(spark, loaded_io, config, audit):
    result = stg_risk_factors.run(spark, loaded_io, config, audit)

    assert result.status == STATUS_SUCCESS
    assert result.return_code == 0
    assert result.target_table == "ETL_STAGING_DB.STG_RISK_FACTORS"
    assert result.row_count > 0

    written = loaded_io.read_spec(schemas.STG_RISK_FACTORS)
    assert_schema(written, schemas.STG_RISK_FACTORS)
    assert written.count() == result.row_count


def test_job_writes_one_row_per_active_or_inactive_customer(spark, loaded_io, config, audit):
    stg_risk_factors.run(spark, loaded_io, config, audit)

    written = loaded_io.read_spec(schemas.STG_RISK_FACTORS)
    customers = loaded_io.read_spec(schemas.CUSTOMERS)
    eligible = customers.filter("CUSTOMER_STATUS IN ('A', 'I')").select("CUSTOMER_ID")

    assert written.select("CUSTOMER_ID").distinct().count() == written.count()
    assert written.count() == eligible.count()
    assert written.join(eligible, "CUSTOMER_ID", "left_anti").count() == 0


def test_job_applies_the_legacy_defaults(spark, loaded_io, config, audit):
    stg_risk_factors.run(spark, loaded_io, config, audit)

    written = loaded_io.read_spec(schemas.STG_RISK_FACTORS)

    # the always-true on-time comparison (LEGACY_INVENTORY.md 5.9) leaves every customer at 100%
    assert written.filter("PAYMENT_ONTIME_PCT <> 100.00").count() == 0
    assert written.filter("PAYMENT_LATE_CNT <> 0").count() == 0
    # customers without any credit/loan payment history keep the 999 default
    assert written.filter("MONTHS_SINCE_LAST_LATE = 999").count() > 0
    # a missing bureau row scores 0 here; the 680 imputation happens in the SAS risk job
    assert written.filter("EXTERNAL_CREDIT_SCORE < 0").count() == 0
    assert written.filter("CREDIT_UTIL_RATIO < 0").count() == 0


def test_job_emits_audit_records(spark, loaded_io, config, audit):
    result = stg_risk_factors.run(spark, loaded_io, config, audit)

    assert [record.status for record in audit.steps][-1] == "SUCCESS"
    assert audit.runs[-1].job_name == stg_risk_factors.JOB_NAME
    assert audit.runs[-1].row_count == result.row_count
    assert result.validation is not None and result.validation.passed
