# Databricks notebook source
# MAGIC %md
# MAGIC # 90 — Row count validation
# MAGIC Equivalent of the final BTEQ SELECT ... UNION ALL over the 4 gold
# MAGIC tables in orchestration/run_full_pipeline.sh. Fails if any is 0.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_banking_dev")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("lookback_months", "12")

try:
    import retail_banking  # noqa: F401
except ImportError:
    import sys
    root = dbutils.notebook.entry_point.getDbutils().notebook() \
        .getContext().notebookPath().get().rsplit("/", 2)[0]
    sys.path.append(f"/Workspace{root}/src")

from retail_banking.config import PipelineConfig
from retail_banking.logging_utils import log_step

# COMMAND ----------

cfg = PipelineConfig.from_widgets(dbutils)
step = "90_ROW_COUNT_VALIDATION"
log_step(spark, step, "START", audit_table=cfg.audit_table)

counts = spark.sql(f"""
    SELECT 'customer_segments'       AS tbl, COUNT(*) AS rows
        FROM {cfg.gold_table('customer_segments')}
    UNION ALL
    SELECT 'transaction_analytics',  COUNT(*)
        FROM {cfg.gold_table('transaction_analytics')}
    UNION ALL
    SELECT 'customer_risk_scores',   COUNT(*)
        FROM {cfg.gold_table('customer_risk_scores')}
    UNION ALL
    SELECT 'customer_master_profile', COUNT(*)
        FROM {cfg.gold_table('customer_master_profile')}
    ORDER BY 1
""").collect()

for r in counts:
    print(f"{r['tbl']:<28} {r['rows']:>10,}")

empty = [r["tbl"] for r in counts if r["rows"] == 0]
if empty:
    log_step(spark, step, "ERROR",
             msg=f"Empty gold tables: {empty}",
             audit_table=cfg.audit_table)
    raise ValueError(f"Empty gold tables: {empty}")

log_step(spark, step, "SUCCESS",
         rowcount=sum(r["rows"] for r in counts),
         audit_table=cfg.audit_table)
