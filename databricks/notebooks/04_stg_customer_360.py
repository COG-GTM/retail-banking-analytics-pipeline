# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 4 - Build etl_staging.stg_customer_360

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from orchestration.pipeline import run_stg_customer_360

run_stg_customer_360(spark, config, min_rows=min_rows)
