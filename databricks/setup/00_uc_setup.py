# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog setup
# MAGIC
# MAGIC Replaces the four Teradata databases with one Unity Catalog catalog:
# MAGIC
# MAGIC | Teradata database    | Unity Catalog schema            | Layer  |
# MAGIC |----------------------|---------------------------------|--------|
# MAGIC | `CORE_BANKING_DB`    | `<catalog>.core_banking`        | bronze |
# MAGIC | `TXN_PROCESSING_DB`  | `<catalog>.core_banking`        | bronze |
# MAGIC | `ETL_STAGING_DB`     | `<catalog>.etl_staging`         | silver |
# MAGIC | `DATA_PRODUCTS_DB`   | `<catalog>.data_products`       | gold   |
# MAGIC | `ETL_STAGING_DB.ETL_RUN_LOG` | `<catalog>._ops.etl_run_log` | ops |
# MAGIC
# MAGIC Access is governed by Unity Catalog grants, so no `LIBNAME`/`LOGON`
# MAGIC credentials exist anywhere in this pipeline.

# COMMAND ----------

import os
import sys
from pathlib import Path

for _p in [os.getcwd(), *[str(p) for p in Path(os.getcwd()).parents]]:
    if os.path.isdir(os.path.join(_p, "shared")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break

from shared.audit import AUDIT_TABLE, AuditLogger, create_audit_table
from shared.runtime import PipelineConfig, get_param

# COMMAND ----------

cfg = PipelineConfig.from_widgets()
landing_volume = get_param("landing_volume", "landing")

print(f"catalog={cfg.catalog} bronze={cfg.bronze_schema} silver={cfg.silver_schema} "
      f"gold={cfg.gold_schema} ops={cfg.ops_schema}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")

for schema in (cfg.bronze_schema, cfg.silver_schema, cfg.gold_schema, cfg.ops_schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.`{schema}`")
    print(f"schema ready: {cfg.catalog}.{schema}")

# Managed volume the bronze notebook reads the source CSV extracts from.
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}.{landing_volume}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit table
# MAGIC Replaces `ETL_STAGING_DB.ETL_RUN_LOG` and the SAS `WORK.PIPELINE_AUDIT` dataset.

# COMMAND ----------

audit_table = cfg.ops(AUDIT_TABLE)
if not spark.catalog.tableExists(audit_table):
    create_audit_table(spark, audit_table)
print(f"audit table ready: {audit_table}")

AuditLogger(spark, audit_table, job_name="00_uc_setup").success(
    "UC_SETUP", f"catalog {cfg.catalog} and schemas provisioned"
)
