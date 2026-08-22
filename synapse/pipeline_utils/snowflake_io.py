"""Snowflake read/write helpers for Synapse Spark jobs.

Replaces ``%connect_teradata``: instead of four SAS/ACCESS LIBNAME statements
with inline passwords, jobs get a single :class:`SnowflakeIO` whose options are
built from :mod:`pipeline_utils.config` and whose key-pair credential comes
from Azure Key Vault via a :class:`~pipeline_utils.secrets.SecretResolver`.

Library reference mapping:

    COREDB -> SnowflakeIO.read_table("CORE_BANKING", ...)
    TXNDB  -> SnowflakeIO.read_table("TXN_PROCESSING", ...)
    STGDB  -> SnowflakeIO.read_table("ETL_STAGING", ...)
    DPDB   -> SnowflakeIO.read_table / overwrite_table("DATA_PRODUCTS", ...)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from pipeline_utils.config import PipelineConfig
from pipeline_utils.secrets import SecretResolver

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


class SnowflakeIO:
    """Thin wrapper around the Spark Snowflake connector."""

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        secret_resolver: SecretResolver,
    ) -> None:
        self._spark = spark
        self._config = config
        self._secret_resolver = secret_resolver

    def options(self, schema: str) -> dict[str, str]:
        sf = self._config.snowflake
        return {
            "sfURL": f"{sf.account}.snowflakecomputing.com",
            "sfUser": sf.user,
            "sfRole": sf.role,
            "sfWarehouse": sf.warehouse,
            "sfDatabase": sf.database,
            "sfSchema": schema,
            "pem_private_key": self._secret_resolver.get_secret(
                sf.private_key_secret
            ),
        }

    def read_table(self, schema: str, table: str) -> DataFrame:
        return (
            self._spark.read.format(SNOWFLAKE_SOURCE)
            .options(**self.options(schema))
            .option("dbtable", table)
            .load()
        )

    def read_query(self, schema: str, query: str) -> DataFrame:
        return (
            self._spark.read.format(SNOWFLAKE_SOURCE)
            .options(**self.options(schema))
            .option("query", query)
            .load()
        )

    def overwrite_table(self, df: DataFrame, schema: str, table: str) -> None:
        """Full refresh of a target table.

        Equivalent to the SAS ``DELETE FROM ...`` pass-through followed by
        ``PROC APPEND ... FORCE``; ``truncate_table`` keeps the Snowflake table
        definition (and its grants) instead of recreating it.
        """
        (
            df.write.format(SNOWFLAKE_SOURCE)
            .options(**self.options(schema))
            .option("dbtable", table)
            .option("truncate_table", "on")
            .mode("overwrite")
            .save()
        )

    def append_table(self, df: DataFrame, schema: str, table: str) -> None:
        (
            df.write.format(SNOWFLAKE_SOURCE)
            .options(**self.options(schema))
            .option("dbtable", table)
            .mode("append")
            .save()
        )
