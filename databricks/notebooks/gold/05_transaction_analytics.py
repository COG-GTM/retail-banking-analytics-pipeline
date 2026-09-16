# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 05 — transaction_analytics (sas/02_sas_txn_analytics.sas)

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
from retail_banking.gold import build_transaction_analytics
from retail_banking.logging_utils import log_step
from retail_banking.validation import raise_if_failed, validate_table

# COMMAND ----------

cfg = PipelineConfig.from_widgets(dbutils)
step = "05_TXN_ANALYTICS"
log_step(spark, step, "START", msg="Model version TXN_V2.1",
         audit_table=cfg.audit_table)

df = build_transaction_analytics(
    spark.table(cfg.silver_table("stg_txn_summary")), cfg.run_date)

res = validate_table(df, "transaction_analytics", key_cols=["customer_id"],
                     not_null=["customer_id", "reporting_period",
                               "total_transactions"],
                     min_rows=cfg.min_gold_rows)
raise_if_failed(res)

period = cfg.run_date.strftime("%Y-%m")
# Mirrors the SAS DELETE WHERE REPORTING_PERIOD before reload
(df.write.format("delta").mode("overwrite")
   .option("replaceWhere", f"reporting_period = '{period}'")
   .saveAsTable(cfg.gold_table("transaction_analytics")))

log_step(spark, step, "SUCCESS", rowcount=res.row_count,
         audit_table=cfg.audit_table)
