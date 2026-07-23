# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 1 - Create Unity Catalog catalog, schemas and Delta tables

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from common import ddl

ddl.create_all(spark, config)
print("Catalog, schemas and tables created for", config.catalog)
