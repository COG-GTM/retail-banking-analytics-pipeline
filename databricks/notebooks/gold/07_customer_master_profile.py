# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 07 — customer_master_profile (sas/04_sas_data_products.sas)

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
from retail_banking.gold import build_customer_master_profile
from retail_banking.logging_utils import log_step
from retail_banking.validation import raise_if_failed, validate_table

# COMMAND ----------

cfg = PipelineConfig.from_widgets(dbutils)
step = "07_MASTER_PROFILE"
log_step(spark, step, "START", msg="Building golden record",
         audit_table=cfg.audit_table)

df = build_customer_master_profile(
    spark.table(cfg.silver_table("stg_customer_360")),
    spark.table(cfg.gold_table("customer_segments")),
    spark.table(cfg.gold_table("transaction_analytics")),
    spark.table(cfg.gold_table("customer_risk_scores")),
    cfg.run_date)

res = validate_table(df, "customer_master_profile", key_cols=["customer_id"],
                     not_null=["customer_id", "full_name", "customer_status"],
                     min_rows=cfg.min_gold_rows)
raise_if_failed(res)

(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(cfg.gold_table("customer_master_profile")))

log_step(spark, step, "SUCCESS", rowcount=res.row_count,
         audit_table=cfg.audit_table)
