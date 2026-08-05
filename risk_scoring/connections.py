"""Source/sink connectivity — the PySpark replacement for ``connect_teradata.sas``.

The SAS macro declared four Teradata LIBNAMEs (COREDB, TXNDB, STGDB, DPDB) with
``{SAS004}`` password placeholders baked into the source. Here the same four
logical databases are addressed through :class:`Connections`, backed by a
pluggable :class:`DataBackend`:

* :class:`JdbcBackend` — Teradata over JDBC, credentials from the secret store.
* :class:`CsvBackend`  — the committed demo extracts under ``data/``, used for
  local runs and oracle-parity validation.

Only the backend changes between environments; module code always goes through
:class:`Connections`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .config import PipelineConfig


def normalise_columns(df: DataFrame) -> DataFrame:
    """Upper-case every column name (CSV exports are lower-cased)."""
    return df.toDF(*[c.upper() for c in df.columns])


class DataBackend(ABC):
    """Read/write access to one logical warehouse."""

    @abstractmethod
    def read_table(
        self, database: str, table: str, schema: StructType | None = None
    ) -> DataFrame:
        """Return ``database.table`` with UPPER_SNAKE column names."""

    @abstractmethod
    def overwrite_table(self, df: DataFrame, database: str, table: str) -> None:
        """Full truncate-and-load of ``database.table`` with ``df``."""


class CsvBackend(DataBackend):
    """Reads the committed CSV extracts; writes Parquet + CSV under ``output_dir``."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def _locate(self, table: str) -> Path:
        filename = f"{table.lower()}.csv"
        matches = sorted(self.config.data_dir.rglob(filename))
        if not matches:
            raise FileNotFoundError(
                f"No CSV extract named {filename} under {self.config.data_dir}"
            )
        return matches[0]

    def read_table(
        self, database: str, table: str, schema: StructType | None = None
    ) -> DataFrame:
        path = self._locate(table)
        reader = self.spark.read.option("header", True)
        if schema is not None:
            # CSV headers are lower-cased; read permissively then cast so the
            # declared schema still drives the types.
            reader = reader.option("inferSchema", False)
            raw = normalise_columns(reader.csv(str(path)))
            return raw.select(*[
                F.col(f.name).cast(f.dataType).alias(f.name)
                for f in schema.fields
                if f.name in raw.columns
            ])
        return normalise_columns(reader.option("inferSchema", True).csv(str(path)))

    def overwrite_table(self, df: DataFrame, database: str, table: str) -> None:
        target = self.config.output_dir / table.lower()
        df.write.mode("overwrite").parquet(str(target))
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            str(target) + "_csv"
        )


class JdbcBackend(DataBackend):
    """Teradata over JDBC. Credentials come from ``TD_PASSWORD``, never source."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def _options(self, database: str) -> dict[str, str]:
        td = self.config.teradata
        return {
            "url": td.jdbc_url(database),
            "driver": td.driver,
            "user": td.username,
            "password": td.password,
        }

    def read_table(
        self, database: str, table: str, schema: StructType | None = None
    ) -> DataFrame:
        df = normalise_columns(
            self.spark.read.format("jdbc")
            .options(**self._options(database), dbtable=f"{database}.{table}")
            .load()
        )
        if schema is None:
            return df
        return df.select(*[
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in schema.fields
            if f.name in df.columns
        ])

    def overwrite_table(self, df: DataFrame, database: str, table: str) -> None:
        (
            df.write.format("jdbc")
            .options(**self._options(database), dbtable=f"{database}.{table}")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )


def build_backend(spark: SparkSession, config: PipelineConfig) -> DataBackend:
    backends = {"csv": CsvBackend, "jdbc": JdbcBackend}
    try:
        return backends[config.io_backend](spark, config)
    except KeyError:
        raise ValueError(
            f"Unknown PIPELINE_IO_BACKEND {config.io_backend!r}; "
            f"expected one of {sorted(backends)}"
        ) from None


class Connections:
    """Named handles for the four databases ``connect_teradata`` used to bind."""

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        backend: DataBackend | None = None,
    ):
        self.spark = spark
        self.config = config
        self.backend = backend or build_backend(spark, config)

    def read_staging(self, table: str, schema: StructType | None = None) -> DataFrame:
        return self.backend.read_table(self.config.databases.staging, table, schema)

    def read_core(self, table: str, schema: StructType | None = None) -> DataFrame:
        return self.backend.read_table(self.config.databases.core, table, schema)

    def read_txn(self, table: str, schema: StructType | None = None) -> DataFrame:
        return self.backend.read_table(self.config.databases.txn, table, schema)

    def write_data_product(self, df: DataFrame, table: str) -> None:
        self.backend.overwrite_table(df, self.config.databases.data_products, table)
