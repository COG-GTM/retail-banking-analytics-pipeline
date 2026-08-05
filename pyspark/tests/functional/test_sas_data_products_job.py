"""Functional test: run the golden-record job end to end on the committed extract."""

from __future__ import annotations

import pytest

from common import schemas
from common.io import InMemoryDataIO
from common.job import STATUS_SUCCESS
from common.schemas import assert_schema
from common.validation import ValidationFailedError
from jobs import sas_data_products

pytestmark = pytest.mark.functional

INPUT_SPECS = (
    schemas.STG_CUSTOMER_360,
    schemas.CUSTOMER_SEGMENTS,
    schemas.TRANSACTION_ANALYTICS,
    schemas.CUSTOMER_RISK_SCORES,
)


@pytest.fixture
def loaded_io(spark, reference_io, memory_io: InMemoryDataIO) -> InMemoryDataIO:
    """The upstream products are the committed reference extracts.

    The other jobs' modules are deliberately not imported: this job is tested against the
    published contracts of its inputs, not against another port's implementation.
    """

    for spec in INPUT_SPECS:
        memory_io.put_spec(spec, reference_io.read_spec(spec))
    return memory_io


def test_job_runs_and_writes_a_ddl_conformant_table(spark, loaded_io, config, audit):
    result = sas_data_products.run(spark, loaded_io, config, audit)

    assert result.status == STATUS_SUCCESS
    assert result.return_code == 0
    assert result.target_table == "DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE"
    assert result.row_count == 407

    written = loaded_io.read_spec(schemas.CUSTOMER_MASTER_PROFILE)
    assert_schema(written, schemas.CUSTOMER_MASTER_PROFILE)
    assert written.count() == result.row_count


def test_job_keeps_one_row_per_active_customer(spark, loaded_io, config, audit):
    sas_data_products.run(spark, loaded_io, config, audit)

    written = loaded_io.read_spec(schemas.CUSTOMER_MASTER_PROFILE)
    statuses = {
        row["CUSTOMER_STATUS"] for row in written.select("CUSTOMER_STATUS").distinct().collect()
    }
    assert statuses == {"A"}
    assert written.select("CUSTOMER_ID").distinct().count() == written.count()
    assert written.filter("MODEL_VERSION <> 'MASTER_V1.5'").count() == 0
    assert written.filter("EFFECTIVE_DATE <> DATE '2026-04-10'").count() == 0


def test_job_validates_the_target_and_emits_the_audit_trail(spark, loaded_io, config, audit):
    result = sas_data_products.run(spark, loaded_io, config, audit)

    assert result.validation is not None
    assert result.validation.passed
    assert {check.name for check in result.validation.checks} == {
        "min_rows",
        "key_uniqueness",
        "not_null.CUSTOMER_ID",
        "not_null.FULL_NAME",
        "not_null.CUSTOMER_STATUS",
    }
    assert not result.validation.warnings

    messages = [record.message for record in audit.steps]
    assert [record.status for record in audit.steps][-1] == "SUCCESS"
    assert "Building golden record" in messages
    assert any("Segment Distribution" in message for message in messages)
    assert any("Risk Tier Distribution" in message for message in messages)
    assert any("Completeness Check" in message for message in messages)
    assert sum("COLLECT STATISTICS (no-op on Spark)" in message for message in messages) == len(
        sas_data_products.COLLECT_STATISTICS_TARGETS
    )
    assert audit.runs[-1].job_name == sas_data_products.JOB_NAME
    assert audit.runs[-1].row_count == result.row_count


def test_job_aborts_before_loading_when_validation_fails(spark, loaded_io, config, audit):
    """``%validate_table`` failure -> ``%abort cancel``: the target is never written."""

    from dataclasses import replace

    strict = replace(config, min_rows=100_000)

    with pytest.raises(ValidationFailedError):
        sas_data_products.run(spark, loaded_io, strict, audit)

    assert not loaded_io.table_exists(
        schemas.CUSTOMER_MASTER_PROFILE.database, schemas.CUSTOMER_MASTER_PROFILE.name
    )
    assert [record.status for record in audit.steps][-1] == "ERROR"
