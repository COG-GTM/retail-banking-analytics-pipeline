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

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, IntegralType, StructType

from .config import PipelineConfig

logger = logging.getLogger(__name__)


def normalise_columns(df: DataFrame) -> DataFrame:
    """Upper-case every column name (CSV exports are lower-cased)."""
    return df.toDF(*[c.upper() for c in df.columns])


def cast_csv_value(column: Column, data_type: DataType) -> Column:
    """Cast a raw CSV string to ``data_type``.

    Teradata INTEGER columns come out of the extract as ``"13.0"``/``"0.0"``,
    which Spark's ANSI-mode ``STRING -> INT`` cast rejects outright. Routing
    integral targets through DOUBLE reproduces SQL cast semantics (truncation
    towards zero) instead of failing the read.
    """
    if isinstance(data_type, IntegralType):
        return column.cast("double").cast(data_type)
    return column.cast(data_type)


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
                cast_csv_value(F.col(f.name), f.dataType).alias(f.name)
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


class JdbcConfigurationError(RuntimeError):
    """The JDBC backend cannot run: missing credentials, driver or tuning values."""


@dataclass(frozen=True)
class JdbcTuning:
    """Read/write tuning for the Teradata JDBC connector.

    Defaults suit the volumes this pipeline moves. ``partition_column`` is
    opt-in: Spark only parallelises a read when a numeric column and both
    bounds are supplied, and the staging tables have no natural partition
    column, so a single-partition read is the default — as it was under
    SAS/ACCESS.
    """

    #: Prefix of the config keys understood by :meth:`from_mapping`.
    KEY_PREFIX = "TD_"

    fetchsize: int = 10_000
    batchsize: int = 10_000
    num_partitions: int = 8
    partition_column: str | None = None
    lower_bound: int | None = None
    upper_bound: int | None = None

    def __post_init__(self) -> None:
        for name in ("fetchsize", "batchsize", "num_partitions"):
            value = getattr(self, name)
            if value <= 0:
                raise JdbcConfigurationError(
                    f"JdbcTuning.{name} must be positive, got {value}"
                )
        bounds = (self.lower_bound, self.upper_bound)
        if self.partition_column and any(b is None for b in bounds):
            raise JdbcConfigurationError(
                "JdbcTuning.partition_column requires both lower_bound and "
                "upper_bound; Spark ignores a partition column without bounds."
            )
        if not self.partition_column and any(b is not None for b in bounds):
            raise JdbcConfigurationError(
                "JdbcTuning lower_bound/upper_bound are only meaningful "
                "together with a partition_column."
            )

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "JdbcTuning":
        """Build tuning from ``TD_*`` config keys, falling back to the defaults.

        ``values`` is whatever the caller resolved (``parse_pipeline_cfg``
        output merged with the environment); this module never reads the
        environment itself.
        """
        defaults = cls()

        def integer(key: str, default: int | None) -> int | None:
            raw = values.get(cls.KEY_PREFIX + key)
            if raw is None or raw == "":
                return default
            try:
                return int(raw)
            except ValueError:
                raise JdbcConfigurationError(
                    f"{cls.KEY_PREFIX + key} must be an integer, got {raw!r}"
                ) from None

        return cls(
            fetchsize=integer("FETCHSIZE", defaults.fetchsize),
            batchsize=integer("BATCHSIZE", defaults.batchsize),
            num_partitions=integer("NUM_PARTITIONS", defaults.num_partitions),
            partition_column=values.get(cls.KEY_PREFIX + "PARTITION_COLUMN") or None,
            lower_bound=integer("LOWER_BOUND", None),
            upper_bound=integer("UPPER_BOUND", None),
        )


def redact_options(options: Mapping[str, str]) -> dict[str, str]:
    """Copy of ``options`` that is safe to log — the password is masked."""
    return {k: ("***" if k == "password" else v) for k, v in options.items()}


class JdbcBackend(DataBackend):
    """Teradata over JDBC. Credentials come from ``TD_PASSWORD``, never source.

    Replaces the four ``LIBNAME ... teradata`` statements of
    ``connect_teradata.sas`` and their ``{SAS004}`` hardcoded passwords. The
    password is resolved per operation, so building the backend and inspecting
    its options never needs the secret — only a real connection does.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        tuning: JdbcTuning | None = None,
    ):
        self.spark = spark
        self.config = config
        self.tuning = tuning or JdbcTuning()

    def _password(self) -> str:
        td = self.config.teradata
        try:
            return td.password
        except RuntimeError as exc:
            raise JdbcConfigurationError(
                f"Cannot reach Teradata at {td.server} as {td.username}: "
                f"${td.password_env_var} is unset. Export it from the secret "
                "store before running with PIPELINE_IO_BACKEND=jdbc, or run "
                "locally with PIPELINE_IO_BACKEND=csv."
            ) from exc

    def _options(self, database: str) -> dict[str, str]:
        td = self.config.teradata
        return {
            "url": td.jdbc_url(database),
            "driver": td.driver,
            "user": td.username,
            "password": self._password(),
        }

    def read_options(self, database: str, table: str) -> dict[str, str]:
        """Every option a read of ``database.table`` is issued with."""
        options = self._options(database)
        options["dbtable"] = f"{database}.{table}"
        options["fetchsize"] = str(self.tuning.fetchsize)
        if self.tuning.partition_column:
            options["partitionColumn"] = self.tuning.partition_column
            options["lowerBound"] = str(self.tuning.lower_bound)
            options["upperBound"] = str(self.tuning.upper_bound)
            options["numPartitions"] = str(self.tuning.num_partitions)
        return options

    def write_options(self, database: str, table: str) -> dict[str, str]:
        """Every option an overwrite of ``database.table`` is issued with.

        ``truncate=true`` stops Spark dropping and recreating the target: the
        SAS this replaces issued ``DELETE FROM``, not ``DROP TABLE``, so the
        Teradata table definition (primary index, grants) has to survive.
        """
        options = self._options(database)
        options["dbtable"] = f"{database}.{table}"
        options["batchsize"] = str(self.tuning.batchsize)
        options["numPartitions"] = str(self.tuning.num_partitions)
        options["truncate"] = "true"
        return options

    def _ensure_driver(self) -> None:
        """Fail early, and legibly, when the Teradata JDBC jar is not loaded."""
        driver = self.config.teradata.driver
        try:
            self.spark.sparkContext._jvm.java.lang.Class.forName(driver)
        except Exception as exc:  # py4j surfaces the JVM ClassNotFoundException
            raise JdbcConfigurationError(
                f"Teradata JDBC driver {driver} is not on the Spark classpath. "
                "Supply terajdbc4.jar (plus tdgssconfig.jar on older releases) "
                "through spark.jars / --jars or spark.jars.packages."
            ) from exc

    def read_table(
        self, database: str, table: str, schema: StructType | None = None
    ) -> DataFrame:
        options = self.read_options(database, table)
        self._ensure_driver()
        logger.info("jdbc read options=%s", redact_options(options))
        df = normalise_columns(self.spark.read.format("jdbc").options(**options).load())
        if schema is None:
            return df
        return df.select(*[
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in schema.fields
            if f.name in df.columns
        ])

    def overwrite_table(self, df: DataFrame, database: str, table: str) -> None:
        options = self.write_options(database, table)
        self._ensure_driver()
        logger.info("jdbc overwrite options=%s", redact_options(options))
        df.write.format("jdbc").options(**options).mode("overwrite").save()


def build_backend(spark: SparkSession, config: PipelineConfig) -> DataBackend:
    backends = {"csv": CsvBackend, "jdbc": JdbcBackend}
    # Membership first: a KeyError from inside a backend constructor must not be
    # reported as an unknown backend name.
    if config.io_backend not in backends:
        raise ValueError(
            f"Unknown PIPELINE_IO_BACKEND {config.io_backend!r}; "
            f"expected one of {sorted(backends)}"
        )
    return backends[config.io_backend](spark, config)


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
