"""Functional test: run the segmentation job on the committed staging extract."""

from __future__ import annotations

from dataclasses import replace

import pytest
from pyspark.sql import functions as F

from common import schemas
from common.io import InMemoryDataIO
from common.job import STATUS_SUCCESS
from common.schemas import assert_schema
from common.validation import ValidationFailedError
from jobs import sas_customer_segments

pytestmark = pytest.mark.functional


@pytest.fixture
def loaded_io(spark, reference_io, memory_io: InMemoryDataIO) -> InMemoryDataIO:
    memory_io.put_spec(schemas.STG_CUSTOMER_360, reference_io.read_spec(schemas.STG_CUSTOMER_360))
    return memory_io


@pytest.fixture
def written(spark, loaded_io, config, audit):
    result = sas_customer_segments.run(spark, loaded_io, config, audit)
    return result, loaded_io.read_spec(schemas.CUSTOMER_SEGMENTS)


def test_job_runs_and_writes_a_ddl_conformant_table(written):
    result, table = written

    assert result.status == STATUS_SUCCESS
    assert result.return_code == 0
    assert result.target_table == "DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS"
    assert result.row_count == 407
    assert_schema(table, schemas.CUSTOMER_SEGMENTS)
    assert table.count() == result.row_count


def test_job_segments_exactly_the_active_customers(spark, loaded_io, config, audit):
    sas_customer_segments.run(spark, loaded_io, config, audit)

    staging = loaded_io.read_spec(schemas.STG_CUSTOMER_360)
    active = staging.filter(F.col("CUSTOMER_STATUS") == "A").select("CUSTOMER_ID")
    written = loaded_io.read_spec(schemas.CUSTOMER_SEGMENTS)

    assert written.select("CUSTOMER_ID").distinct().count() == written.count()
    assert written.select("CUSTOMER_ID").exceptAll(active).count() == 0
    assert active.exceptAll(written.select("CUSTOMER_ID")).count() == 0
    assert staging.filter(F.col("CUSTOMER_STATUS") != "A").count() > 0


def test_every_customer_gets_one_of_the_five_segment_labels(written):
    _, table = written

    labels = {row["SEGMENT_NAME"] for row in table.select("SEGMENT_NAME").distinct().collect()}
    assert labels == set(sas_customer_segments.SEGMENT_LABELS)
    assert table.filter(F.col("SEGMENT_NAME").isNull() | F.col("SEGMENT_ID").isNull()).count() == 0
    assert {row["SEGMENT_ID"] for row in table.select("SEGMENT_ID").distinct().collect()} == set(
        range(sas_customer_segments.NUM_CLUSTERS)
    )


def test_placeholder_and_metadata_columns(written, run_date):
    _, table = written

    constants = table.select(
        F.countDistinct("MODEL_VERSION").alias("versions"),
        F.min("MODEL_VERSION").alias("model_version"),
        F.max("SUBSEGMENT_ID").alias("max_subsegment"),
        F.max("DIGITAL_ADOPTION_SCORE").alias("max_digital"),
        F.min("EFFECTIVE_DATE").alias("min_effective"),
        F.max("EFFECTIVE_DATE").alias("max_effective"),
        F.max(F.length("CHANNEL_PREFERENCE")).alias("max_channel_length"),
        F.count(F.when(F.col("CHANNEL_PREFERENCE").isNull(), 1)).alias("null_channels"),
    ).collect()[0]

    assert constants["versions"] == 1
    assert constants["model_version"] == "SEG_V3.2"
    assert constants["max_subsegment"] == 0
    assert float(constants["max_digital"]) == 0.00
    assert constants["min_effective"] == constants["max_effective"] == run_date
    assert constants["max_channel_length"] == 0
    assert constants["null_channels"] == 0


def test_job_emits_the_audit_trail_and_validation_result(spark, loaded_io, config, audit):
    result = sas_customer_segments.run(spark, loaded_io, config, audit)

    statuses = [record.status for record in audit.steps]
    assert statuses[0] == "START"
    assert statuses[-1] == "SUCCESS"
    assert audit.steps[-1].row_count == result.row_count
    assert any(record.message == "Extracted staging data" for record in audit.steps)
    assert audit.runs[-1].job_name == sas_customer_segments.JOB_NAME
    assert audit.runs[-1].row_count == result.row_count

    assert result.validation is not None
    assert result.validation.passed
    assert {check.name for check in result.validation.checks} == {
        "min_rows",
        "key_uniqueness",
        "not_null.CUSTOMER_ID",
        "not_null.SEGMENT_NAME",
        "not_null.SEGMENT_ID",
    }


def test_production_min_rows_aborts_the_load_on_the_sample_extract(spark, loaded_io, config, audit):
    """``%validate_table(min_rows=1000)`` + ``%abort cancel``: 407 rows must abort before the load."""

    production = replace(config, min_rows=1000)

    with pytest.raises(ValidationFailedError):
        sas_customer_segments.run(spark, loaded_io, production, audit)

    assert not loaded_io.table_exists("DATA_PRODUCTS_DB", "CUSTOMER_SEGMENTS")
    assert audit.steps[-1].status == "ERROR"
