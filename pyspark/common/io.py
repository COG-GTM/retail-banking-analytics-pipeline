"""Readers/writers that replace ``connect_teradata.sas``.

The SAS macro defined four Teradata ``LIBNAME`` references (COREDB, TXNDB, STGDB,
DPDB).  Here those become a single :class:`DataIO` abstraction with two backends:

* :class:`LocalDataIO` -- CSV sources + Parquet lake, used for tests, the demo
  end-to-end run, and any credential-free environment (production target is
  Databricks/Delta -- Rules R5: no hardcoded catalog/paths, all config-driven).
* :class:`JdbcDataIO` -- reads/writes real Teradata over JDBC, reproducing the
  bulkload/fastload connection intent of ``connect_teradata``.

Both expose the same logical calls (``read_source`` / ``read_staging`` /
``read_data_product`` / ``write_staging`` / ``write_data_product``) so jobs are
storage-agnostic.  ``TRANSACTION_ANALYTICS`` is written partitioned by
``reporting_period`` to match ``PARTITION BY COLUMN(REPORTING_PERIOD)`` in the DDL.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StructField,
    StructType,
)

from . import schemas
from .config import PipelineConfig

_INT_TYPES = (ShortType, IntegerType, LongType)


def _read_struct(spec: schemas.TableSpec) -> StructType:
    """A lenient read schema: integer-family columns are widened to double so
    float-formatted integers in the CSV fixtures (e.g. ``3.0``) parse instead of
    becoming NULL under a strict integer schema. ``enforce_schema`` casts every
    column back to its exact DDL type afterwards.
    """
    fields = []
    for f in spec.struct.fields:
        dtype = DoubleType() if isinstance(f.dataType, _INT_TYPES) else f.dataType
        fields.append(StructField(f.name, dtype, True))
    return StructType(fields)

_SOURCE_SUBDIR = "01_source_tables"
_STAGING_SUBDIR = "02_bteq_staging"
_PRODUCT_SUBDIR = "03_sas_data_products"

# logical name -> file stem for the CSV fixtures/lake layout.
_SOURCE_FILE = {
    "CUSTOMERS": "customers",
    "ACCOUNTS": "accounts",
    "ADDRESSES": "addresses",
    "TRANSACTIONS": "transactions",
    "TRANSACTION_TYPES": "transaction_types",
    "CUSTOMER_BUREAU_SCORES": "customer_bureau_scores",
}
_STAGING_FILE = {
    "STG_CUSTOMER_360": "stg_customer_360",
    "STG_TXN_SUMMARY": "stg_txn_summary",
    "STG_RISK_FACTORS": "stg_risk_factors",
}
_PRODUCT_FILE = {
    "CUSTOMER_SEGMENTS": "customer_segments",
    "TRANSACTION_ANALYTICS": "transaction_analytics",
    "CUSTOMER_RISK_SCORES": "customer_risk_scores",
    "CUSTOMER_MASTER_PROFILE": "customer_master_profile",
}


class DataIO(ABC):
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    @abstractmethod
    def read_source(self, table: str) -> DataFrame: ...

    @abstractmethod
    def read_staging(self, table: str) -> DataFrame: ...

    @abstractmethod
    def read_data_product(self, table: str) -> DataFrame: ...

    @abstractmethod
    def write_staging(self, df: DataFrame, table: str) -> None: ...

    @abstractmethod
    def write_data_product(self, df: DataFrame, table: str) -> None: ...


class LocalDataIO(DataIO):
    """CSV in, Parquet out. ``source_dir`` holds the source CSVs; ``lake_dir``
    is where staging + data-product outputs are written and read back."""

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        source_dir: str | Path,
        lake_dir: str | Path,
        read_products_from_source: bool = False,
    ):
        super().__init__(spark, config)
        self.source_dir = Path(source_dir)
        self.lake_dir = Path(lake_dir)
        # When True, read_staging/read_data_product fall back to the committed
        # CSV fixtures (used by the regression harness to load legacy outputs).
        self.read_products_from_source = read_products_from_source

    def _read_csv(self, path: Path, spec: schemas.TableSpec) -> DataFrame:
        raw = (
            self.spark.read
            .option("header", True)
            .schema(_read_struct(spec))
            .csv(str(path))
        )
        return schemas.enforce_schema(raw, spec)

    def read_source(self, table: str) -> DataFrame:
        spec = schemas.SOURCE_TABLES[table]
        path = self.source_dir / _SOURCE_SUBDIR / f"{_SOURCE_FILE[table]}.csv"
        return self._read_csv(path, spec)

    def read_staging(self, table: str) -> DataFrame:
        spec = schemas.STAGING_TABLES[table]
        if self.read_products_from_source:
            path = self.source_dir / _STAGING_SUBDIR / f"{_STAGING_FILE[table]}.csv"
            return self._read_csv(path, spec)
        return self.spark.read.parquet(str(self.lake_dir / _STAGING_SUBDIR / table))

    def read_data_product(self, table: str) -> DataFrame:
        spec = schemas.DATA_PRODUCT_TABLES[table]
        if self.read_products_from_source:
            path = self.source_dir / _PRODUCT_SUBDIR / f"{_PRODUCT_FILE[table]}.csv"
            return self._read_csv(path, spec)
        return self.spark.read.parquet(str(self.lake_dir / _PRODUCT_SUBDIR / table))

    def write_staging(self, df: DataFrame, table: str) -> None:
        out = self.lake_dir / _STAGING_SUBDIR / table
        df.write.mode("overwrite").parquet(str(out))

    def write_data_product(self, df: DataFrame, table: str) -> None:
        out = self.lake_dir / _PRODUCT_SUBDIR / table
        spec = schemas.DATA_PRODUCT_TABLES[table]
        writer = df.write.mode("overwrite")
        if spec.partition_by:
            writer = writer.partitionBy(*spec.partition_by)
        writer.parquet(str(out))


class JdbcDataIO(DataIO):
    """Teradata JDBC backend reproducing the ``connect_teradata`` LIBNAMEs.

    Not exercised in the credential-free demo, but kept faithful to the source:
    COREDB/TXNDB are read-only sources, STGDB is staging, DPDB is the data
    product target (bulkload).
    """

    _DRIVER = "com.teradata.jdbc.TeraDriver"

    def __init__(self, spark: SparkSession, config: PipelineConfig, password: str):
        super().__init__(spark, config)
        self._password = password

    def _url(self, database: str) -> str:
        return (
            f"jdbc:teradata://{self.config.td_server}/"
            f"DATABASE={database},LOGMECH={self.config.td_logmech}"
        )

    def _opts(self, database: str) -> dict[str, str]:
        return {
            "url": self._url(database),
            "user": self.config.td_username,
            "password": self._password,
            "driver": self._DRIVER,
        }

    def _read(self, database: str, table: str) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .options(**self._opts(database))
            .option("dbtable", table)
            .load()
        )

    def read_source(self, table: str) -> DataFrame:
        db = self.config.db_txn if table in ("TRANSACTIONS", "TRANSACTION_TYPES") else self.config.db_core
        return self._read(db, table)

    def read_staging(self, table: str) -> DataFrame:
        return self._read(self.config.db_stg, table)

    def read_data_product(self, table: str) -> DataFrame:
        return self._read(self.config.db_dp, table)

    def _write(self, df: DataFrame, database: str, table: str) -> None:
        (
            df.write.format("jdbc")
            .options(**self._opts(database))
            .option("dbtable", table)
            .mode("overwrite")
            .save()
        )

    def write_staging(self, df: DataFrame, table: str) -> None:
        self._write(df, self.config.db_stg, table)

    def write_data_product(self, df: DataFrame, table: str) -> None:
        self._write(df, self.config.db_dp, table)


def local_io(
    spark: SparkSession,
    config: PipelineConfig,
    source_dir: str | Path,
    lake_dir: str | Path,
    **kwargs,
) -> LocalDataIO:
    return LocalDataIO(spark, config, source_dir, lake_dir, **kwargs)
