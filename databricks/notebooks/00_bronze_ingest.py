# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bronze ingest
# MAGIC Loads the 6 source tables into Delta bronze tables
# MAGIC (`<catalog>.core_banking.*` and `<catalog>.txn_processing.*`).
# MAGIC source_mode=jdbc reads Teradata via JDBC with secret-scope credentials;
# MAGIC source_mode=csv reads CSV files from the landing volume (demo path).

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_banking_dev")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("lookback_months", "12")
dbutils.widgets.text("source_mode", "csv")
dbutils.widgets.text("volume_path", "")

try:
    import retail_banking  # noqa: F401
except ImportError:
    import sys
    root = dbutils.notebook.entry_point.getDbutils().notebook() \
        .getContext().notebookPath().get().rsplit("/", 2)[0]
    sys.path.append(f"/Workspace{root}/src")

from retail_banking.bronze import ingest_source_tables
from retail_banking.config import PipelineConfig
from retail_banking.logging_utils import log_step

# COMMAND ----------

cfg = PipelineConfig.from_widgets(dbutils)
volume_path = dbutils.widgets.get("volume_path") or None
source_mode = dbutils.widgets.get("source_mode")

log_step(spark, "00_BRONZE_INGEST", "START",
         msg=f"source={source_mode} catalog={cfg.catalog}",
         audit_table=cfg.audit_table)

frames = ingest_source_tables(spark, cfg, source=source_mode,
                              volume_path=volume_path, dbutils=dbutils)

total = sum(df.count() for df in frames.values())
log_step(spark, "00_BRONZE_INGEST", "SUCCESS", rowcount=total,
         audit_table=cfg.audit_table)
