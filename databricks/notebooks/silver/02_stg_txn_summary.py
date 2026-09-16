# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 02 — stg_txn_summary (bteq/02_stg_txn_summary.bteq)

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_banking_dev")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("lookback_months", "12")

try:
    import retail_banking  # noqa: F401
except ImportError:
    import sys
    root = dbutils.notebook.entry_point.getDbutils().notebook() \
        .getContext().notebookPath().get().rsplit("/", 3)[0]
    sys.path.append(f"/Workspace{root}/src")

from retail_banking.config import PipelineConfig
from retail_banking.logging_utils import log_step
from retail_banking.silver import build_stg_txn_summary
from retail_banking.validation import raise_if_failed, validate_table

# COMMAND ----------

cfg = PipelineConfig.from_widgets(dbutils)
step = "02_STG_TXN_SUMMARY"
log_step(spark, step, "START",
         msg=f"run_date={cfg.run_date} lookback={cfg.lookback_months}m",
         audit_table=cfg.audit_table)

df = build_stg_txn_summary(
    spark.table(cfg.txn_table("transactions")),
    spark.table(cfg.bronze_table("accounts")),
    spark.table(cfg.txn_table("transaction_types")),
    cfg.run_date, cfg.lookback_months)

res = validate_table(df, "stg_txn_summary",
                     key_cols=["customer_id", "account_id"],
                     not_null=["customer_id", "account_id"], min_rows=1)
raise_if_failed(res)

(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(cfg.silver_table("stg_txn_summary")))
spark.sql(f"ANALYZE TABLE {cfg.silver_table('stg_txn_summary')} "
          "COMPUTE STATISTICS FOR COLUMNS customer_id, account_id")

log_step(spark, step, "SUCCESS", rowcount=res.row_count,
         audit_table=cfg.audit_table)
