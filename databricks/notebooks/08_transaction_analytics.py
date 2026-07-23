# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 8 - Build data_products.transaction_analytics

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from orchestration.pipeline import run_transaction_analytics

run_transaction_analytics(spark, config, min_rows=min_rows)
