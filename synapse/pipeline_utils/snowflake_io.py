"""Snowflake connectivity for Synapse Spark jobs.

Replaces ``sas/macros/connect_teradata.sas``:

* the four Teradata LIBNAMEs (COREDB / TXNDB / STGDB / DPDB) become fully
  qualified ``database.schema.table`` references built from
  :class:`~pipeline_utils.config.SnowflakeConfig`;
* LDAP username/password authentication becomes key-pair (JWT) authentication
  with the private key resolved from Azure Key Vault;
* ``PROC APPEND ... FORCE`` / ``PROC SQL ... execute by teradata`` become
  Snowflake Spark connector reads and writes.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from pipeline_utils.config import SnowflakeConfig
from pipeline_utils.secrets import SecretResolver

LOGGER = logging.getLogger(__name__)

SNOWFLAKE_SPARK_SOURCE = "net.snowflake.spark.snowflake"


class SnowflakeIO:
    """Read and write Snowflake tables from Spark, plus a thin SQL escape hatch."""

    def __init__(
        self,
        config: SnowflakeConfig,
        secret_resolver: Optional[SecretResolver] = None,
    ) -> None:
        self.config = config
        self.secrets = secret_resolver or SecretResolver(config.key_vault_url)

    def connector_options(self, database: str, schema: str) -> Dict[str, str]:
        """Options for the Snowflake Spark connector, including the private key."""
        options = self.base_options(database, schema)
        options["pem_private_key"] = self.secrets.get(self.config.private_key_secret_name)
        return options

    def base_options(self, database: str, schema: str) -> Dict[str, str]:
        """Non-secret connector options (safe to log)."""
        return {
            "sfURL": f"{self.config.account}.snowflakecomputing.com",
            "sfUser": self.config.user,
            "sfRole": self.config.role,
            "sfWarehouse": self.config.warehouse,
            "sfDatabase": database,
            "sfSchema": schema,
        }

    def read_table(self, spark: Any, table_fqn: str) -> Any:
        database, schema, table = _split_fqn(table_fqn)
        LOGGER.info("Reading Snowflake table %s", table_fqn)
        return (
            spark.read.format(SNOWFLAKE_SPARK_SOURCE)
            .options(**self.connector_options(database, schema))
            .option("dbtable", table)
            .load()
        )

    def write_table(self, df: Any, table_fqn: str, mode: str = "overwrite") -> None:
        """Write a DataFrame to Snowflake.

        ``mode="overwrite"`` with ``truncate_table=on`` reproduces the SAS
        ``DELETE FROM ... ; PROC APPEND FORCE`` sequence while preserving the
        table definition created by the DDL scripts.
        """
        database, schema, table = _split_fqn(table_fqn)
        LOGGER.info("Writing %s to Snowflake (mode=%s)", table_fqn, mode)
        (
            df.write.format(SNOWFLAKE_SPARK_SOURCE)
            .options(**self.connector_options(database, schema))
            .option("dbtable", table)
            .option("truncate_table", "on")
            .option("usestagingtable", "off")
            .mode(mode)
            .save()
        )

    def execute(self, statement: str, database: str = "", schema: str = "") -> None:
        """Run a single SQL statement through the Python Snowflake connector."""
        import snowflake.connector  # imported lazily: not needed for transforms

        connection = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            role=self.config.role,
            warehouse=self.config.warehouse,
            database=database or self.config.data_products_database,
            schema=schema or self.config.data_products_schema,
            authenticator=self.config.authenticator,
            private_key=self.secrets.get(self.config.private_key_secret_name).encode(),
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(statement)
        finally:
            connection.close()


def _split_fqn(table_fqn: str) -> tuple:
    parts = table_fqn.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Expected a fully qualified 'database.schema.table' name, got '{table_fqn}'"
        )
    return parts[0], parts[1], parts[2]
