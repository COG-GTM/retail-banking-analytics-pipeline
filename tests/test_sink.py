"""Tests for STEP 6 — the truncate-load of ``CUSTOMER_RISK_SCORES``."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from risk_scoring import schemas
from risk_scoring.audit import AuditLog
from risk_scoring.config import PipelineConfig
from risk_scoring.connections import Connections, CsvBackend
from risk_scoring.sink import SinkContractError, write_customer_risk_scores

#: The analytic (all-``double``) shape STEP 4 hands to the sink.
ANALYTIC_SCHEMA = StructType([
    StructField("CUSTOMER_ID", LongType(), False),
    StructField("COMPOSITE_RISK_SCORE", DoubleType(), True),
    StructField("RISK_TIER", StringType(), True),
    StructField("PROBABILITY_OF_DEFAULT", DoubleType(), True),
    StructField("CREDIT_RISK_COMPONENT", DoubleType(), True),
    StructField("BEHAVIOUR_RISK_COMPONENT", DoubleType(), True),
    StructField("VELOCITY_RISK_COMPONENT", DoubleType(), True),
    StructField("BUREAU_SCORE_COMPONENT", DoubleType(), True),
    StructField("PAYMENT_HISTORY_COMPONENT", DoubleType(), True),
    StructField("PRIMARY_RISK_DRIVER", StringType(), True),
    StructField("SECONDARY_RISK_DRIVER", StringType(), True),
    StructField("SCORE_DELTA_30D", DoubleType(), True),
    StructField("WATCH_LIST_FLAG", StringType(), True),
    StructField("REVIEW_REQUIRED_FLAG", StringType(), True),
    StructField("MODEL_VERSION", StringType(), True),
    StructField("EFFECTIVE_DATE", DateType(), True),
    StructField("LOAD_TS", TimestampType(), True),
])

EFFECTIVE_DATE = dt.date(2026, 4, 10)
LOAD_TS = dt.datetime(2026, 4, 10, 3, 15, 0)


def row(customer_id: int, score: float = 61.234, prob: float = 0.1234565):
    return (
        customer_id,
        score,
        "HIGH",
        prob,
        30.0,
        20.0,
        10.5,
        70.0,
        80.0,
        "CREDIT_UTILIZATION",
        "PAYMENT_BEHAVIOUR",
        0.0,
        "N",
        "Y",
        "RISK_V4.0",
        EFFECTIVE_DATE,
        LOAD_TS,
    )


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("risk_scoring_tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@pytest.fixture()
def connections(spark, tmp_path) -> Connections:
    config = PipelineConfig(
        io_backend="csv", data_dir=tmp_path / "data", output_dir=tmp_path / "out"
    )
    return Connections(spark, config, backend=CsvBackend(spark, config))


def written(connections: Connections):
    target = connections.config.output_dir / schemas.CUSTOMER_RISK_SCORES.lower()
    return connections.spark.read.parquet(str(target))


def test_writes_target_schema_and_column_order(spark, connections):
    df = spark.createDataFrame([row(1001)], schema=ANALYTIC_SCHEMA)

    assert write_customer_risk_scores(df, connections) == 1

    out = written(connections)
    assert out.columns == list(schemas.CUSTOMER_RISK_SCORES_COLUMNS)
    assert [f.dataType for f in out.schema.fields] == [
        f.dataType for f in schemas.CUSTOMER_RISK_SCORES_SCHEMA.fields
    ]


def test_decimal_casts_applied_at_the_sink(spark, connections):
    df = spark.createDataFrame([row(1001)], schema=ANALYTIC_SCHEMA)
    write_customer_risk_scores(df, connections)

    record = written(connections).first()
    assert record.COMPOSITE_RISK_SCORE == Decimal("61.23")  # DECIMAL(6,2)
    assert record.PROBABILITY_OF_DEFAULT == Decimal("0.123457")  # DECIMAL(7,6)
    assert record.VELOCITY_RISK_COMPONENT == Decimal("10.50")  # DECIMAL(5,2)
    assert record.EFFECTIVE_DATE == EFFECTIVE_DATE


def test_returns_row_count(spark, connections):
    df = spark.createDataFrame(
        [row(1000 + i) for i in range(7)], schema=ANALYTIC_SCHEMA
    )
    assert write_customer_risk_scores(df, connections) == 7
    assert written(connections).count() == 7


def test_column_order_of_the_input_is_irrelevant(spark, connections):
    df = spark.createDataFrame([row(1001)], schema=ANALYTIC_SCHEMA).select(
        *reversed(ANALYTIC_SCHEMA.fieldNames())
    )
    write_customer_risk_scores(df, connections)
    assert written(connections).columns == list(schemas.CUSTOMER_RISK_SCORES_COLUMNS)


def test_extra_input_columns_are_dropped(spark, connections):
    df = spark.createDataFrame([row(1001)], schema=ANALYTIC_SCHEMA).withColumn(
        "PROB_DEFAULT", F.lit(0.5)
    )
    write_customer_risk_scores(df, connections)
    assert "PROB_DEFAULT" not in written(connections).columns


def test_missing_contract_column_fails_loudly(spark, connections):
    df = spark.createDataFrame([row(1001)], schema=ANALYTIC_SCHEMA).drop("RISK_TIER")
    with pytest.raises(SinkContractError, match="RISK_TIER"):
        write_customer_risk_scores(df, connections)


def test_overwrite_is_idempotent(spark, connections):
    """``DELETE`` + ``PROC APPEND`` doubled rows on a re-run; overwrite does not."""
    df = spark.createDataFrame(
        [row(1000 + i) for i in range(4)], schema=ANALYTIC_SCHEMA
    )

    first = write_customer_risk_scores(df, connections)
    second = write_customer_risk_scores(df, connections)

    assert first == second == 4
    assert written(connections).count() == 4


def test_audit_trail_start_and_success(spark, connections):
    audit = AuditLog("03_RISK_SCORING")
    df = spark.createDataFrame([row(1001)], schema=ANALYTIC_SCHEMA)

    write_customer_risk_scores(df, connections, audit=audit)

    assert [r.status for r in audit.records] == ["START", "SUCCESS"]
    assert audit.records[-1].row_count == 1


def test_no_audit_is_fine(spark, connections):
    df = spark.createDataFrame([row(1001)], schema=ANALYTIC_SCHEMA)
    assert write_customer_risk_scores(df, connections) == 1
