"""Unit tier: the PostgreSQL DDL generated from the schema contracts."""

from __future__ import annotations

import pytest
from pyspark.sql.types import BooleanType, DecimalType, IntegerType, StringType, TimestampType

from common import schemas
from common.config import PipelineConfig
from common.job import default_schema_map
from orchestration.deploy_postgres import (
    DeploymentReport,
    _literal,
    apply_ddl,
    create_table_ddl,
    deploy,
    deployment_ddl,
    index_ddl,
    postgres_type,
)
from orchestration.sample_data import SOURCE_SPECS

pytestmark = pytest.mark.unit


class _RecordingIO:
    """Stands in for :class:`JdbcDataIO` — DDL application is a pure statement stream."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.written: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)

    def write_spec(self, df, spec, *, mode: str = "overwrite") -> int:
        self.written.append(spec.qualified_name)
        return df.count()


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (StringType(), "TEXT"),
        (IntegerType(), "INTEGER"),
        (TimestampType(), "TIMESTAMP"),
        (DecimalType(18, 2), "NUMERIC(18,2)"),
        (DecimalType(5, 4), "NUMERIC(5,4)"),
    ],
)
def test_contract_types_map_onto_postgres_types(dtype, expected: str) -> None:
    assert postgres_type(dtype) == expected


def test_an_unmapped_type_is_a_hard_error_not_a_silent_text_column() -> None:
    with pytest.raises(TypeError, match="no PostgreSQL type mapping"):
        postgres_type(BooleanType())


def test_create_table_keeps_the_ddl_column_order_defaults_and_not_nulls() -> None:
    ddl = create_table_ddl(schemas.STG_CUSTOMER_360, "etl_staging_db")

    assert ddl.startswith("CREATE TABLE IF NOT EXISTS etl_staging_db.stg_customer_360 (")
    assert "customer_id BIGINT NOT NULL" in ddl
    assert "has_checking TEXT DEFAULT 'N'" in ddl
    assert "credit_utilization_pct NUMERIC(5,2)" in ddl

    order = [line.strip().split(" ")[0] for line in ddl.splitlines()[1:-1]]
    assert order == [column.name.lower() for column in schemas.STG_CUSTOMER_360.columns]


def test_the_primary_index_becomes_an_index_never_a_primary_key() -> None:
    """A Teradata PI is a distribution key and is not unique — a PRIMARY KEY would reject rows."""

    statements = index_ddl(schemas.STG_TXN_SUMMARY, "etl_staging_db")

    assert any("CREATE INDEX IF NOT EXISTS ix_stg_txn_summary_pi" in s for s in statements)
    assert not any("PRIMARY KEY" in s or "UNIQUE" in s for s in statements)


def test_partitioning_clauses_become_indexes_on_the_partition_column() -> None:
    statements = index_ddl(schemas.TRANSACTION_ANALYTICS, "data_products_db")

    assert any("(reporting_period)" in s for s in statements)


def test_deployment_creates_every_schema_before_any_table(config: PipelineConfig) -> None:
    statements = deployment_ddl(config)
    schema_map = default_schema_map(config)

    creates = [s for s in statements if s.startswith("CREATE SCHEMA")]
    assert {s.split()[-1] for s in creates} == set(schema_map.values())
    first_table = next(i for i, s in enumerate(statements) if s.startswith("CREATE TABLE"))
    assert all(statements.index(s) < first_table for s in creates)


def test_every_contract_table_is_deployed(config: PipelineConfig) -> None:
    statements = deployment_ddl(config)
    deployed = {s.split()[5] for s in statements if s.startswith("CREATE TABLE")}
    schema_map = default_schema_map(config)

    expected = {f"{schema_map[spec.database]}.{spec.name.lower()}" for spec in schemas.ALL_SPECS}
    assert deployed == expected


def test_apply_ddl_executes_each_statement_once(config: PipelineConfig) -> None:
    io = _RecordingIO()
    statements = deployment_ddl(config)

    assert apply_ddl(io, statements) == len(statements)
    assert io.statements == list(statements)


def test_the_deployment_report_starts_empty() -> None:
    report = DeploymentReport()
    assert report.statements == 0
    assert report.loaded == {}


def test_sources_are_loaded_through_the_jobs_own_io_layer(
    config: PipelineConfig, source_io
) -> None:
    """Not COPY: the load must exercise the same write path the jobs are tested against."""

    io = _RecordingIO()
    report = deploy(io, source_io, config)

    assert report.statements == len(deployment_ddl(config))
    assert set(report.loaded) == {spec.qualified_name for spec in SOURCE_SPECS}
    assert io.written == [spec.qualified_name for spec in SOURCE_SPECS]
    assert report.loaded["CORE_BANKING_DB.CUSTOMERS"] > 0


def test_string_defaults_are_quoted_and_escaped() -> None:
    assert _literal("N") == "'N'"
    assert _literal("O'Hara") == "'O''Hara'"
    assert _literal(7) == "7"
