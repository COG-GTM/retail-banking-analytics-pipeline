"""Deploy the warehouse to PostgreSQL and load the source extract over the JDBC IO layer.

The legacy warehouse is four Teradata databases (``CORE_BANKING_DB``, ``TXN_PROCESSING_DB``,
``ETL_STAGING_DB``, ``DATA_PRODUCTS_DB``). PostgreSQL has no equivalent of a Teradata database,
so each one becomes a schema of the same (lower-cased) name inside one PostgreSQL database; the
mapping lives in :func:`common.job.default_schema_map` and is the only place the two naming
schemes meet.

The DDL emitted here is generated from :mod:`common.schemas` — the same specs the jobs enforce
before every write — so the deployed tables cannot drift from the contract the tests assert. The
Teradata-only clauses are translated rather than dropped:

* ``PRIMARY INDEX (cols)`` -> a non-unique btree index (Teradata's PI is a distribution key, not
  a constraint, so it must not become a ``PRIMARY KEY``);
* ``PARTITION BY RANGE_N(...)`` / ``PARTITION BY COLUMN`` -> a btree index on the partitioning
  column, since the row counts here do not justify declarative partitioning;
* ``COMPRESS`` / ``FALLBACK`` / ``MULTISET`` -> dropped, storage-engine specific.

Source tables are loaded through :class:`common.io.JdbcDataIO`, i.e. the exact code path the
jobs use, rather than ``COPY``.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field

from pyspark.sql.types import (
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

from common import schemas
from common.config import PipelineConfig
from common.io import JdbcDataIO, LocalDataIO
from common.job import build_arg_parser, config_from_args, default_schema_map
from common.schemas import TableSpec
from common.spark import build_spark_session
from orchestration.sample_data import SOURCE_SPECS, sample_source_io

LOGGER = logging.getLogger(__name__)

#: Teradata VARCHAR/CHAR lengths are carried in the DDL but not in the Spark contract, so string
#: columns become ``TEXT``; PostgreSQL stores both identically and it keeps the two definitions
#: from disagreeing about a length the jobs never enforce.
_TYPE_MAP: tuple[tuple[type[DataType], str], ...] = (
    (ShortType, "SMALLINT"),
    (IntegerType, "INTEGER"),
    (LongType, "BIGINT"),
    (DateType, "DATE"),
    (TimestampType, "TIMESTAMP"),
    (StringType, "TEXT"),
)


def postgres_type(dtype: DataType) -> str:
    """Map a Spark contract type onto its PostgreSQL column type."""

    if isinstance(dtype, DecimalType):
        return f"NUMERIC({dtype.precision},{dtype.scale})"
    for spark_type, sql_type in _TYPE_MAP:
        if isinstance(dtype, spark_type):
            return sql_type
    raise TypeError(f"no PostgreSQL type mapping for {dtype}")


def _literal(value: str | int | float) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return str(value)


def create_table_ddl(spec: TableSpec, schema: str) -> str:
    """``CREATE TABLE`` for one contract, in DDL column order."""

    columns = []
    for column in spec.columns:
        parts = [f"    {column.name.lower()} {postgres_type(column.dtype)}"]
        if column.default is not None:
            parts.append(f"DEFAULT {_literal(column.default)}")
        if not column.nullable:
            parts.append("NOT NULL")
        columns.append(" ".join(parts))
    body = ",\n".join(columns)
    return f"CREATE TABLE IF NOT EXISTS {schema}.{spec.name.lower()} (\n{body}\n)"


def index_ddl(spec: TableSpec, schema: str) -> list[str]:
    """Indexes standing in for the Teradata primary index and partitioning clauses."""

    statements: list[str] = []
    table = spec.name.lower()
    if spec.primary_index:
        cols = ", ".join(name.lower() for name in spec.primary_index)
        statements.append(f"CREATE INDEX IF NOT EXISTS ix_{table}_pi ON {schema}.{table} ({cols})")
    for column in spec.partition_by:
        statements.append(
            f"CREATE INDEX IF NOT EXISTS ix_{table}_{column.lower()} "
            f"ON {schema}.{table} ({column.lower()})"
        )
    return statements


def deployment_ddl(
    config: PipelineConfig, specs: Sequence[TableSpec] = schemas.ALL_SPECS
) -> list[str]:
    """Every statement needed to stand the warehouse up, in order."""

    schema_map = default_schema_map(config)
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {schema}" for schema in sorted(set(schema_map.values()))
    ]
    for spec in specs:
        schema = schema_map[spec.database]
        statements.append(create_table_ddl(spec, schema))
        statements.extend(index_ddl(spec, schema))
    return statements


@dataclass
class DeploymentReport:
    statements: int = 0
    loaded: dict[str, int] = field(default_factory=dict)


def apply_ddl(io: JdbcDataIO, statements: Sequence[str]) -> int:
    for statement in statements:
        LOGGER.info("DDL: %s", statement.splitlines()[0])
        io.execute(statement)
    return len(statements)


def load_sources(
    io: JdbcDataIO,
    source_io: LocalDataIO,
    specs: Sequence[TableSpec] = SOURCE_SPECS,
) -> dict[str, int]:
    """Load the source extract into PostgreSQL through the jobs' own IO layer."""

    loaded: dict[str, int] = {}
    for spec in specs:
        df = source_io.read_spec(spec)
        loaded[spec.qualified_name] = io.write_spec(df, spec, mode="overwrite")
        LOGGER.info("loaded %s rows into %s", loaded[spec.qualified_name], spec.qualified_name)
    return loaded


def deploy(io: JdbcDataIO, source_io: LocalDataIO, config: PipelineConfig) -> DeploymentReport:
    report = DeploymentReport()
    report.statements = apply_ddl(io, deployment_ddl(config))
    report.loaded = load_sources(io, source_io)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser("deploy the warehouse schema to PostgreSQL and load sources")
    parser.add_argument("--ddl-only", action="store_true", help="create schemas/tables, no load")
    parser.add_argument("--print-ddl", action="store_true", help="print the DDL and exit")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s"
    )
    config = config_from_args(args)

    if args.print_ddl:
        for statement in deployment_ddl(config):
            print(f"{statement};")
        return 0

    if not args.jdbc_url:
        raise SystemExit("--jdbc-url is required")

    spark = build_spark_session("deploy_postgres", master=args.master)
    try:
        io = JdbcDataIO(
            spark=spark,
            url=args.jdbc_url,
            user=args.jdbc_user or "",
            password=args.jdbc_password or "",
            schema_map=default_schema_map(config),
        )
        statements = apply_ddl(io, deployment_ddl(config))
        LOGGER.info("applied %s DDL statements", statements)
        if not args.ddl_only:
            loaded = load_sources(io, sample_source_io(spark))
            LOGGER.info("loaded source tables: %s", loaded)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
