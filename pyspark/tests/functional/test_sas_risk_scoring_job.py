"""Functional test for the risk scoring job: run it on the committed staging extract."""

from __future__ import annotations

import dataclasses

import pytest

from common import schemas
from common.io import InMemoryDataIO
from common.job import STATUS_SUCCESS
from common.schemas import assert_schema
from jobs import sas_risk_scoring

pytestmark = pytest.mark.functional

INPUT_SPECS = (schemas.STG_RISK_FACTORS, schemas.STG_CUSTOMER_360)


@pytest.fixture
def loaded_io(spark, reference_io, memory_io: InMemoryDataIO) -> InMemoryDataIO:
    """The staging tables this job consumes are themselves reference outputs of the pipeline."""

    for spec in INPUT_SPECS:
        memory_io.put_spec(spec, reference_io.read_spec(spec))
    return memory_io


@pytest.fixture
def result(spark, loaded_io, config, audit):
    return sas_risk_scoring.run(spark, loaded_io, config, audit)


def test_job_runs_and_writes_a_ddl_conformant_table(result, loaded_io):
    assert result.status == STATUS_SUCCESS
    assert result.return_code == 0
    assert result.target_table == "DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES"

    written = loaded_io.read_spec(schemas.CUSTOMER_RISK_SCORES)
    assert_schema(written, schemas.CUSTOMER_RISK_SCORES)
    assert written.count() == result.row_count


def test_job_scores_every_active_customer_exactly_once(result, loaded_io):
    written = loaded_io.read_spec(schemas.CUSTOMER_RISK_SCORES)
    active = (
        loaded_io.read_spec(schemas.STG_CUSTOMER_360)
        .filter("CUSTOMER_STATUS = 'A'")
        .join(loaded_io.read_spec(schemas.STG_RISK_FACTORS), on="CUSTOMER_ID")
    )

    assert result.row_count == active.count()
    assert written.select("CUSTOMER_ID").distinct().count() == written.count()


def test_job_output_stays_inside_the_business_domain(result, loaded_io, run_date):
    written = loaded_io.read_spec(schemas.CUSTOMER_RISK_SCORES)
    rows = written.collect()

    assert {row["RISK_TIER"] for row in rows} <= {"LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"}
    assert {row["WATCH_LIST_FLAG"] for row in rows} <= {"Y", "N"}
    assert {row["REVIEW_REQUIRED_FLAG"] for row in rows} <= {"Y", "N"}
    assert {row["MODEL_VERSION"] for row in rows} == {"RISK_V4.0"}
    assert {row["EFFECTIVE_DATE"] for row in rows} == {run_date}
    assert all(0 <= float(row["PROBABILITY_OF_DEFAULT"]) <= 1 for row in rows)
    assert all(0 <= float(row["COMPOSITE_RISK_SCORE"]) <= 100 for row in rows)
    assert {float(row["SCORE_DELTA_30D"]) for row in rows} == {0.0}
    assert {row["PRIMARY_RISK_DRIVER"] for row in rows} <= set(sas_risk_scoring.DRIVER_LABELS) | {
        ""
    }


def test_job_validates_the_target_and_emits_audit_records(result, audit):
    assert result.validation is not None
    assert result.validation.passed
    assert result.validation.row_count == result.row_count

    steps = [(record.status, record.message) for record in audit.steps]
    assert steps[0][0] == "START"
    assert "RISK_V4.0" in steps[0][1]
    # the config-only RISK_SCORE_THRESHOLD is recorded even though the legacy model ignores it
    assert "RISK_SCORE_THRESHOLD=700" in steps[0][1]
    assert steps[-1] == ("SUCCESS", "Pipeline complete")
    assert audit.runs[-1].job_name == sas_risk_scoring.JOB_NAME
    assert audit.runs[-1].row_count == result.row_count


def test_job_reports_the_degenerate_single_class_fallback(result, audit):
    """The upstream BTEQ makes ``PAYMENT_LATE_CNT`` zero everywhere, so no model can be fitted."""

    messages = [record.message for record in audit.steps]
    assert any("intercept-only base rate" in message for message in messages)


def test_job_is_a_full_refresh(spark, loaded_io, config, audit):
    first = sas_risk_scoring.run(spark, loaded_io, config, audit)
    second = sas_risk_scoring.run(spark, loaded_io, config, audit)

    assert second.row_count == first.row_count
    assert loaded_io.read_spec(schemas.CUSTOMER_RISK_SCORES).count() == first.row_count


def test_job_aborts_when_the_target_is_short_of_min_rows(spark, loaded_io, config, audit):
    from common.validation import ValidationFailedError

    starved = dataclasses.replace(config, min_rows=100_000)

    with pytest.raises(ValidationFailedError):
        sas_risk_scoring.run(spark, loaded_io, starved, audit)
    assert [record.status for record in audit.steps][-1] == "ERROR"
