# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 7 - Build data_products.customer_segments

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from orchestration.pipeline import run_customer_segments

run_customer_segments(spark, config, min_rows=min_rows)
