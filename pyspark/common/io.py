"""Data access abstraction.

Port of ``sas/macros/connect_teradata.sas``. The legacy macro bound four LIBNAMEs
(``COREDB``, ``TXNDB``, ``STGDB``, ``DPDB``) to Teradata databases; here a job asks a
:class:`DataIO` for ``(database, table)`` and never knows whether the bytes come from a local
file, a JDBC database or an in-memory fixture. Jobs must not build paths or hold credentials.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from common.schemas import TableSpec, enforce_schema, spec_for

LOGGER = logging.getLogger(__name__)

WriteMode = str


class TableNotFoundError(FileNotFoundError):
    """Raised when a requested table does not exist in the backing store."""


class DataIO(ABC):
    """Read and write DDL-contracted tables."""

    @abstractmethod
    def read_table(self, database: str, table: str) -> DataFrame:
        """Read a table, projected onto its DDL contract."""

    @abstractmethod
    def write_table(
        self,
        df: DataFrame,
        database: str,
        table: str,
        *,
        mode: WriteMode = "overwrite",
        partition_values: dict[str, str] | None = None,
    ) -> int:
        """Write a table and return the number of rows written.

        ``partition_values`` reproduces the legacy ``DELETE FROM ... WHERE <partition> = ...``
        followed by ``PROC APPEND``: only the named partition is replaced.
        """

    @abstractmethod
    def table_exists(self, database: str, table: str) -> bool:
        """Whether the table is materialised in the backing store."""

    def read_spec(self, spec: TableSpec) -> DataFrame:
        return self.read_table(spec.database, spec.name)

    def write_spec(
        self,
        df: DataFrame,
        spec: TableSpec,
        *,
        mode: WriteMode = "overwrite",
        partition_values: dict[str, str] | None = None,
    ) -> int:
        return self.write_table(
            df, spec.database, spec.name, mode=mode, partition_values=partition_values
        )


@dataclass
class LocalDataIO(DataIO):
    """CSV/Parquet implementation used for local runs, fixtures and the sample data set.

    Layout: ``<base_path>/<database>/<TABLE>.<ext>``. ``path_overrides`` maps a qualified table
    name onto an explicit path so that the repository's ``data/`` sample tree can be read
    without copying files around.
    """

    spark: SparkSession
    base_path: Path
    fmt: str = "csv"
    path_overrides: dict[str, Path] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.base_path = Path(self.base_path)

    def path_for(self, database: str, table: str) -> Path:
        override = self.path_overrides.get(f"{database.upper()}.{table.upper()}")
        if override is not None:
            return Path(override)
        suffix = "csv" if self.fmt == "csv" else self.fmt
        return self.base_path / database.upper() / f"{table.upper()}.{suffix}"

    def table_exists(self, database: str, table: str) -> bool:
        return self.path_for(database, table).exists()

    def read_table(self, database: str, table: str) -> DataFrame:
        spec = spec_for(database, table)
        path = self.path_for(database, table)
        if not path.exists():
            raise TableNotFoundError(f"{spec.qualified_name} not found at {path}")
        if self.fmt == "csv":
            raw = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "false")
                .option("multiLine", "true")
                .option("escape", '"')
                .csv(str(path))
            )
        else:
            raw = self.spark.read.format(self.fmt).load(str(path))
        # Sample extracts are written with lower-case headers and in a different column order
        # than the DDL; read by (case-insensitive) name, never positionally.
        normalised = raw.toDF(*[name.upper() for name in raw.columns])
        return enforce_schema(normalised, spec, allow_missing=True)

    def write_table(
        self,
        df: DataFrame,
        database: str,
        table: str,
        *,
        mode: WriteMode = "overwrite",
        partition_values: dict[str, str] | None = None,
    ) -> int:
        spec = spec_for(database, table)
        projected = enforce_schema(df, spec)
        projected.cache()
        row_count = projected.count()
        path = self.path_for(database, table)
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = projected.write.mode("append" if mode == "append" else "overwrite")
        if self.fmt == "csv":
            writer.option("header", "true").csv(str(path))
        else:
            if spec.partition_by:
                writer = writer.partitionBy(*spec.partition_by)
            writer.format(self.fmt).save(str(path))
        projected.unpersist()
        LOGGER.info("wrote %s rows to %s", row_count, path)
        return row_count


@dataclass
class InMemoryDataIO(DataIO):
    """In-memory implementation for unit and functional tests."""

    tables: dict[str, DataFrame] = field(default_factory=dict)

    @staticmethod
    def _key(database: str, table: str) -> str:
        return f"{database.upper()}.{table.upper()}"

    def put(self, database: str, table: str, df: DataFrame) -> None:
        self.tables[self._key(database, table)] = df

    def put_spec(self, spec: TableSpec, df: DataFrame) -> None:
        self.put(spec.database, spec.name, df)

    def table_exists(self, database: str, table: str) -> bool:
        return self._key(database, table) in self.tables

    def read_table(self, database: str, table: str) -> DataFrame:
        spec = spec_for(database, table)
        try:
            df = self.tables[self._key(database, table)]
        except KeyError as exc:
            raise TableNotFoundError(f"{spec.qualified_name} has not been written") from exc
        return enforce_schema(df, spec, allow_missing=True)

    def write_table(
        self,
        df: DataFrame,
        database: str,
        table: str,
        *,
        mode: WriteMode = "overwrite",
        partition_values: dict[str, str] | None = None,
    ) -> int:
        spec = spec_for(database, table)
        projected = enforce_schema(df, spec)
        key = self._key(database, table)
        if mode == "append" and key in self.tables:
            projected = self.tables[key].unionByName(projected)
        self.tables[key] = projected
        return projected.count()


@dataclass
class JdbcDataIO(DataIO):
    """JDBC implementation used for the PostgreSQL/Teradata deployment.

    ``schema_map`` maps a Teradata database name onto the physical schema in the target
    database (e.g. ``CORE_BANKING_DB`` -> ``core_banking_db``).
    """

    spark: SparkSession
    url: str
    user: str
    password: str
    driver: str = "org.postgresql.Driver"
    schema_map: dict[str, str] = field(default_factory=dict)
    num_partitions: int = 8

    def physical_name(self, database: str, table: str) -> str:
        schema = self.schema_map.get(database.upper(), database.lower())
        return f"{schema}.{table.lower()}"

    def _options(self) -> dict[str, str]:
        return {
            "url": self.url,
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }

    def execute(self, statement: str) -> None:
        """Run a DDL/DML statement over the same JDBC connection the jobs use."""

        jvm = self.spark.sparkContext._jvm
        jvm.java.lang.Class.forName(self.driver)
        connection = jvm.java.sql.DriverManager.getConnection(self.url, self.user, self.password)
        try:
            jdbc_statement = connection.createStatement()
            try:
                jdbc_statement.execute(statement)
            finally:
                jdbc_statement.close()
        finally:
            connection.close()

    def table_exists(self, database: str, table: str) -> bool:
        schema = self.schema_map.get(database.upper(), database.lower())
        query = (
            "(SELECT 1 AS present FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{table.lower()}') t"
        )
        return (
            self.spark.read.format("jdbc")
            .options(**self._options())
            .option("dbtable", query)
            .load()
            .count()
            > 0
        )

    def read_table(self, database: str, table: str) -> DataFrame:
        spec = spec_for(database, table)
        raw = (
            self.spark.read.format("jdbc")
            .options(**self._options())
            .option("dbtable", self.physical_name(database, table))
            .load()
        )
        renamed = raw.toDF(*[name.upper() for name in raw.columns])
        return enforce_schema(renamed, spec, allow_missing=True)

    def write_table(
        self,
        df: DataFrame,
        database: str,
        table: str,
        *,
        mode: WriteMode = "overwrite",
        partition_values: dict[str, str] | None = None,
    ) -> int:
        spec = spec_for(database, table)
        projected = enforce_schema(df, spec)
        projected.cache()
        row_count = projected.count()
        target = self.physical_name(database, table)

        if partition_values:
            predicate = " AND ".join(
                f"{column.lower()} = '{value}'" for column, value in partition_values.items()
            )
            self.execute(f"DELETE FROM {target} WHERE {predicate}")
            write_mode = "append"
        elif mode == "overwrite":
            self.execute(f"TRUNCATE TABLE {target}")
            write_mode = "append"
        else:
            write_mode = "append"

        (
            projected.toDF(*[name.lower() for name in projected.columns])
            .coalesce(self.num_partitions)
            .write.format("jdbc")
            .options(**self._options())
            .option("dbtable", target)
            .option("batchsize", "10000")
            .mode(write_mode)
            .save()
        )
        projected.unpersist()
        LOGGER.info("wrote %s rows to %s", row_count, target)
        return row_count
