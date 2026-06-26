# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up Unity Catalog, schemas, and Delta tables
# MAGIC
# MAGIC One-time (idempotent) setup that replaces the Teradata DDL deployment.
# MAGIC It executes the three migrated DDL files under `databricks/ddl/`:
# MAGIC
# MAGIC 1. `00_source_tables.sql`     - source tables (core_banking, txn_processing)
# MAGIC 2. `01_staging_tables.sql`    - etl_staging tables + `etl_run_log` audit table
# MAGIC 3. `02_data_product_tables.sql` - data_products tables
# MAGIC
# MAGIC Each `{{CATALOG}}` token is replaced with the configured catalog name and
# MAGIC every statement is executed with `spark.sql`.
# MAGIC
# MAGIC Optionally, set the `sample_data_path` widget to a folder containing the
# MAGIC repo's `data/01_source_tables/*.csv` files (e.g. a UC Volume path) to load
# MAGIC the source Delta tables for an end-to-end demo run.

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

import os

dbutils.widgets.text("sample_data_path", "", "Optional: source CSV folder (UC Volume path)")
SAMPLE_DATA_PATH = dbutils.widgets.get("sample_data_path").strip()

# COMMAND ----------

# Resolve the databricks/ddl directory relative to this notebook (Workspace Files).
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_nb_path = ctx.notebookPath().get()                  # .../databricks/setup/00_setup_unity_catalog
_databricks_dir = os.path.dirname(os.path.dirname(_nb_path))  # .../databricks
DDL_DIR = f"/Workspace{_databricks_dir}/ddl"
print(f"Reading DDL from: {DDL_DIR}")

# COMMAND ----------


def run_ddl_file(filename: str) -> None:
    """Read a migrated DDL file, substitute the catalog, and run each statement."""
    path = f"{DDL_DIR}/{filename}"
    with open(path) as fh:
        sql_text = fh.read().replace("{{CATALOG}}", CATALOG)  # noqa: F821

    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for stmt in statements:
        # Skip pure-comment blocks (lines beginning with -- only)
        if all(line.strip().startswith("--") or not line.strip() for line in stmt.splitlines()):
            continue
        spark.sql(stmt)  # noqa: F821
    print(f"  [{filename}] executed {len(statements)} statement(s)")


# COMMAND ----------

print(f"Setting up catalog '{CATALOG}' and schemas ...")
run_ddl_file("00_source_tables.sql")
run_ddl_file("01_staging_tables.sql")
run_ddl_file("02_data_product_tables.sql")
print("Unity Catalog setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: load sample source data
# MAGIC
# MAGIC If `sample_data_path` points at a folder with the repo's source CSVs, load
# MAGIC them into the source Delta tables so the staging/analytics notebooks have
# MAGIC data to process.

# COMMAND ----------

if SAMPLE_DATA_PATH:
    csv_to_table = {
        "customers.csv": core("customers"),                      # noqa: F821
        "accounts.csv": core("accounts"),                        # noqa: F821
        "addresses.csv": core("addresses"),                      # noqa: F821
        "customer_bureau_scores.csv": core("customer_bureau_scores"),  # noqa: F821
        "transactions.csv": txn("transactions"),                 # noqa: F821
        "transaction_types.csv": txn("transaction_types"),       # noqa: F821
    }
    for csv_name, table in csv_to_table.items():
        src = f"{SAMPLE_DATA_PATH.rstrip('/')}/{csv_name}"
        df = (
            spark.read.option("header", True)        # noqa: F821
            .option("inferSchema", True)
            .csv(src)
        )
        df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table)
        print(f"  loaded {df.count():,} rows -> {table}")
    print("Sample source data loaded.")
else:
    print("sample_data_path not set - skipping source data load.")
