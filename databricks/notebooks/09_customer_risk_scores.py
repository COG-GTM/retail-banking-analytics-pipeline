# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 9 - Build data_products.customer_risk_scores

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from orchestration.pipeline import run_customer_risk_scores

run_customer_risk_scores(spark, config, min_rows=min_rows)
