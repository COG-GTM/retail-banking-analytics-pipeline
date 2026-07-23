# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 5 - Build etl_staging.stg_txn_summary

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from orchestration.pipeline import run_stg_txn_summary

run_stg_txn_summary(spark, config, min_rows=min_rows)
