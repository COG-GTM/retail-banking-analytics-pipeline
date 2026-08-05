"""Tests for the hardened ``JdbcBackend`` (no Teradata instance is contacted)."""

from __future__ import annotations

import logging

import pytest

from pyspark.sql.types import DoubleType, LongType, StructField, StructType

from risk_scoring.config import PipelineConfig
from risk_scoring.connections import (
    CsvBackend,
    JdbcBackend,
    JdbcConfigurationError,
    JdbcTuning,
    SourceContractError,
    build_backend,
    project_to_schema,
    redact_options,
)

PASSWORD = "s3cr3t-not-a-real-password"


@pytest.fixture()
def config() -> PipelineConfig:
    return PipelineConfig(io_backend="jdbc")


@pytest.fixture()
def backend(config, monkeypatch) -> JdbcBackend:
    monkeypatch.setenv("TD_PASSWORD", PASSWORD)
    return JdbcBackend(spark=None, config=config)


def test_read_options_per_database(backend, config):
    options = backend.read_options(config.databases.staging, "STG_RISK_FACTORS")
    assert options["url"] == (
        "jdbc:teradata://tdprod.corp.bankdemo.com/DATABASE=ETL_STAGING_DB,"
        "LOGMECH=LDAP,CHARSET=UTF8"
    )
    assert options["driver"] == "com.teradata.jdbc.TeraDriver"
    assert options["user"] == "svc_etl_pipeline"
    assert options["dbtable"] == "ETL_STAGING_DB.STG_RISK_FACTORS"
    assert options["fetchsize"] == "10000"
    # No partition column configured -> a single-partition read, as under SAS.
    assert "partitionColumn" not in options
    assert "numPartitions" not in options


def test_read_options_url_tracks_the_database(backend, config):
    products = backend.read_options(config.databases.data_products, "CUSTOMER_RISK_SCORES")
    core = backend.read_options(config.databases.core, "CUSTOMER")
    assert "DATABASE=DATA_PRODUCTS_DB" in products["url"]
    assert "DATABASE=CORE_BANKING_DB" in core["url"]


def test_partitioned_read_options(config, monkeypatch):
    monkeypatch.setenv("TD_PASSWORD", PASSWORD)
    tuning = JdbcTuning(
        fetchsize=50_000,
        num_partitions=4,
        partition_column="CUSTOMER_ID",
        lower_bound=1,
        upper_bound=1_000_000,
    )
    options = JdbcBackend(None, config, tuning).read_options(
        config.databases.staging, "STG_RISK_FACTORS"
    )
    assert options["partitionColumn"] == "CUSTOMER_ID"
    assert options["lowerBound"] == "1"
    assert options["upperBound"] == "1000000"
    assert options["numPartitions"] == "4"
    assert options["fetchsize"] == "50000"


def test_write_options_truncate_and_batching(backend, config):
    options = backend.write_options(
        config.databases.data_products, "CUSTOMER_RISK_SCORES"
    )
    assert options["dbtable"] == "DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES"
    assert options["truncate"] == "true"  # DELETE FROM, not DROP TABLE
    assert options["batchsize"] == "10000"
    assert options["numPartitions"] == "8"


def test_tuning_from_mapping_overrides_defaults():
    tuning = JdbcTuning.from_mapping({
        "TD_FETCHSIZE": "25000",
        "TD_BATCHSIZE": "5000",
        "TD_NUM_PARTITIONS": "12",
        "TD_PARTITION_COLUMN": "CUSTOMER_ID",
        "TD_LOWER_BOUND": "1",
        "TD_UPPER_BOUND": "500",
    })
    assert (tuning.fetchsize, tuning.batchsize, tuning.num_partitions) == (
        25000,
        5000,
        12,
    )
    assert tuning.partition_column == "CUSTOMER_ID"


def test_tuning_from_empty_mapping_is_the_default():
    assert JdbcTuning.from_mapping({}) == JdbcTuning()


def test_tuning_rejects_non_integer_values():
    with pytest.raises(JdbcConfigurationError, match="TD_FETCHSIZE"):
        JdbcTuning.from_mapping({"TD_FETCHSIZE": "lots"})


@pytest.mark.parametrize(
    "kwargs",
    [
        {"fetchsize": 0},
        {"batchsize": -1},
        {"num_partitions": 0},
    ],
)
def test_tuning_rejects_non_positive_sizes(kwargs):
    with pytest.raises(JdbcConfigurationError, match="must be positive"):
        JdbcTuning(**kwargs)


def test_partition_column_requires_bounds():
    with pytest.raises(JdbcConfigurationError, match="lower_bound and upper_bound"):
        JdbcTuning(partition_column="CUSTOMER_ID")


def test_bounds_require_a_partition_column():
    with pytest.raises(JdbcConfigurationError, match="partition_column"):
        JdbcTuning(lower_bound=1, upper_bound=10)


def test_missing_password_raises_an_actionable_error(config, monkeypatch, caplog):
    monkeypatch.delenv("TD_PASSWORD", raising=False)
    backend = JdbcBackend(None, config)

    with caplog.at_level(logging.DEBUG, logger="risk_scoring.connections"):
        with pytest.raises(JdbcConfigurationError) as excinfo:
            backend.read_options(config.databases.staging, "STG_RISK_FACTORS")

    message = str(excinfo.value)
    assert "TD_PASSWORD" in message
    assert "PIPELINE_IO_BACKEND=csv" in message
    assert caplog.text == ""


def test_password_never_appears_in_logs_or_repr(backend, config, caplog):
    options = backend.write_options(config.databases.data_products, "T")
    assert options["password"] == PASSWORD
    assert redact_options(options)["password"] == "***"
    assert PASSWORD not in str(redact_options(options))
    assert PASSWORD not in caplog.text


def test_build_backend_still_selects_the_csv_path():
    csv_config = PipelineConfig(io_backend="csv")
    assert isinstance(build_backend(None, csv_config), CsvBackend)


def test_build_backend_selects_jdbc_without_needing_the_password(config, monkeypatch):
    monkeypatch.delenv("TD_PASSWORD", raising=False)
    assert isinstance(build_backend(None, config), JdbcBackend)


def test_build_backend_rejects_an_unknown_backend():
    with pytest.raises(ValueError, match="Unknown PIPELINE_IO_BACKEND 'delta'"):
        build_backend(None, PipelineConfig(io_backend="delta"))


class _FakeDataFrame:
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns


def test_project_to_schema_names_the_missing_columns():
    schema = StructType([
        StructField("CUSTOMER_ID", LongType()),
        StructField("AVG_DAILY_BALANCE_90D", DoubleType()),
    ])
    frame = _FakeDataFrame(["CUSTOMER_ID"])

    with pytest.raises(SourceContractError, match="AVG_DAILY_BALANCE_90D"):
        project_to_schema(frame, schema, "STG.STG_RISK_FACTORS")

    complete = _FakeDataFrame(["CUSTOMER_ID", "AVG_DAILY_BALANCE_90D", "EXTRA"])
    assert project_to_schema(complete, schema, "STG.STG_RISK_FACTORS") is complete
