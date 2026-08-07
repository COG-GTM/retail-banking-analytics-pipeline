# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — source table ingestion
# MAGIC
# MAGIC Loads the six operational extracts (`data/01_source_tables/*.csv`) into Delta
# MAGIC tables under `<catalog>.core_banking`, using the explicit schemas transcribed
# MAGIC from `ddl/00_source_tables.sql`.
# MAGIC
# MAGIC The Teradata source tables lived in two databases (`CORE_BANKING_DB` and
# MAGIC `TXN_PROCESSING_DB`); both land in the single bronze schema.

# COMMAND ----------

import os
import sys
from pathlib import Path

for _p in [os.getcwd(), *[str(p) for p in Path(os.getcwd()).parents]]:
    if os.path.isdir(os.path.join(_p, "shared")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from shared.audit import AUDIT_TABLE, AuditLogger
from shared.dq import validate_dataframe
from shared.runtime import PipelineConfig, get_param, in_databricks
from shared.schemas import SOURCE_FILES, source_schema

JOB_NAME = "bronze_source_tables"

# COMMAND ----------


def read_source_csv(spark: SparkSession, path: str, table: str) -> DataFrame:
    """Read one source extract with the DDL schema.

    Columns are read as strings and cast explicitly: the CSV extracts carry
    microsecond timestamps and empty strings for NULL, neither of which the CSV
    reader's typed parser handles consistently across runtimes.
    """
    schema = source_schema(table)
    raw = StructType([StructField(f.name, StringType(), True) for f in schema.fields])
    df = (
        spark.read.option("header", "true")
        .option("enforceSchema", "false")
        .schema(raw)
        .csv(path)
    )
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


# COMMAND ----------

if in_databricks():
    cfg = PipelineConfig.from_widgets()
    source_path = get_param("source_data_path").rstrip("/")
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))

    for file_name, table in SOURCE_FILES.items():
        target = cfg.bronze(table)
        with audit.step(f"LOAD_{table}", f"{source_path}/{file_name}.csv -> {target}") as ctx:
            df = read_source_csv(spark, f"{source_path}/{file_name}.csv", table)
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(target)
            ctx.row_count = spark.table(target).count()
            print(f"{target}: {ctx.row_count:,} rows")

    # Replaces COLLECT STATISTICS / PRIMARY INDEX on the Teradata sources.
    spark.sql(f"OPTIMIZE {cfg.bronze('TRANSACTIONS')} ZORDER BY (ACCOUNT_ID)")
    spark.sql(f"OPTIMIZE {cfg.bronze('ACCOUNTS')} ZORDER BY (CUSTOMER_ID)")

    validate_dataframe(
        spark.table(cfg.bronze("CUSTOMERS")),
        cfg.bronze("CUSTOMERS"),
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID", "CUSTOMER_STATUS"],
        min_rows=1,
    )
