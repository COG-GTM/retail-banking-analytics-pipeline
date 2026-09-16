# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 06 — customer_risk_scores (sas/03_sas_risk_scoring.sas)

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
from retail_banking.gold import build_customer_risk_scores
from retail_banking.logging_utils import log_step
from retail_banking.validation import raise_if_failed, validate_table

# COMMAND ----------

cfg = PipelineConfig.from_widgets(dbutils)
step = "06_RISK_SCORING"
# RISK_SCORE_THRESHOLD is read and logged, as in the SAS program
# (%let RISK_THRESHOLD = %sysget(RISK_SCORE_THRESHOLD)); it is not
# applied anywhere in the scoring logic.
log_step(spark, step, "START",
         msg=f"Model version RISK_V4.0, "
             f"RISK_SCORE_THRESHOLD={cfg.risk_score_threshold}",
         audit_table=cfg.audit_table)

df = build_customer_risk_scores(
    spark.table(cfg.silver_table("stg_risk_factors")),
    spark.table(cfg.silver_table("stg_customer_360")),
    cfg.run_date)

res = validate_table(df, "customer_risk_scores", key_cols=["customer_id"],
                     not_null=["customer_id", "composite_risk_score",
                               "risk_tier"],
                     min_rows=cfg.min_gold_rows)
raise_if_failed(res)

(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(cfg.gold_table("customer_risk_scores")))

log_step(spark, step, "SUCCESS", rowcount=res.row_count,
         audit_table=cfg.audit_table)
