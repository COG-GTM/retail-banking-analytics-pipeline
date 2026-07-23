# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 6 - Build etl_staging.stg_risk_factors

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from orchestration.pipeline import run_stg_risk_factors

run_stg_risk_factors(spark, config, min_rows=min_rows)
