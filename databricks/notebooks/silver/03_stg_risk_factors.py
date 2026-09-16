# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 03 — stg_risk_factors (bteq/03_stg_risk_factors.bteq)

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
from retail_banking.silver import build_stg_risk_factors
from retail_banking.validation import raise_if_failed, validate_table

# COMMAND ----------

cfg = PipelineConfig.from_widgets(dbutils)
step = "03_STG_RISK_FACTORS"
log_step(spark, step, "START", msg=f"run_date={cfg.run_date}",
         audit_table=cfg.audit_table)

df = build_stg_risk_factors(
    spark.table(cfg.bronze_table("customers")),
    spark.table(cfg.bronze_table("accounts")),
    spark.table(cfg.txn_table("transactions")),
    spark.table(cfg.txn_table("transaction_types")),
    spark.table(cfg.bronze_table("customer_bureau_scores")),
    cfg.run_date)

res = validate_table(df, "stg_risk_factors", key_cols=["customer_id"],
                     not_null=["customer_id"], min_rows=1)
raise_if_failed(res)

(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(cfg.silver_table("stg_risk_factors")))
spark.sql(f"ANALYZE TABLE {cfg.silver_table('stg_risk_factors')} "
          "COMPUTE STATISTICS FOR COLUMNS customer_id")

log_step(spark, step, "SUCCESS", rowcount=res.row_count,
         audit_table=cfg.audit_table)
