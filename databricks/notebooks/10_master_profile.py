# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 10 - Build data_products.customer_master_profile

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from orchestration.pipeline import run_master_profile

run_master_profile(spark, config, min_rows=min_rows)
