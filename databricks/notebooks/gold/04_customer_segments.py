# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 04 — customer_segments (sas/01_sas_customer_segments.sas)

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_banking_dev")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("lookback_months", "12")
dbutils.widgets.text("min_gold_rows", "1000")

try:
    import retail_banking  # noqa: F401
except ImportError:
    import sys
    root = dbutils.notebook.entry_point.getDbutils().notebook() \
        .getContext().notebookPath().get().rsplit("/", 3)[0]
    sys.path.append(f"/Workspace{root}/src")

from retail_banking.config import PipelineConfig
from retail_banking.gold import build_customer_segments
from retail_banking.logging_utils import log_step
from retail_banking.validation import raise_if_failed, validate_table

# COMMAND ----------

cfg = PipelineConfig.from_widgets(dbutils)
step = "04_CUSTOMER_SEGMENTS"
log_step(spark, step, "START", msg="Model version SEG_V3.2",
         audit_table=cfg.audit_table)

df = build_customer_segments(
    spark.table(cfg.silver_table("stg_customer_360")), cfg.run_date)

res = validate_table(df, "customer_segments", key_cols=["customer_id"],
                     not_null=["customer_id", "segment_name", "segment_id"],
                     min_rows=cfg.min_gold_rows)
raise_if_failed(res)

(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(cfg.gold_table("customer_segments")))

log_step(spark, step, "SUCCESS", rowcount=res.row_count,
         audit_table=cfg.audit_table)
