# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Configuration
# MAGIC
# MAGIC Databricks replacement for `config/pipeline_config.cfg` and
# MAGIC `sas/macros/connect_teradata.sas`.
# MAGIC
# MAGIC Because data lives in Delta / Unity Catalog there is no LIBNAME / JDBC
# MAGIC connection to establish. This notebook only resolves the catalog and
# MAGIC schema names and the run parameters, then exposes helper functions to
# MAGIC build fully-qualified table names.
# MAGIC
# MAGIC `%run` this notebook from every staging / analytics notebook to import
# MAGIC the configuration into the caller's namespace:
# MAGIC
# MAGIC ```python
# MAGIC # MAGIC %run ../config/pipeline_config
# MAGIC ```
# MAGIC
# MAGIC The four Teradata databases map to Unity Catalog schemas under one catalog:
# MAGIC
# MAGIC | Teradata database   | Unity Catalog schema           |
# MAGIC |---------------------|--------------------------------|
# MAGIC | CORE_BANKING_DB     | `<catalog>`.core_banking       |
# MAGIC | TXN_PROCESSING_DB   | `<catalog>`.txn_processing     |
# MAGIC | ETL_STAGING_DB      | `<catalog>`.etl_staging        |
# MAGIC | DATA_PRODUCTS_DB    | `<catalog>`.data_products      |

# COMMAND ----------

# Job parameters (widgets). Defaults match the original pipeline_config.cfg so
# the pipeline runs end-to-end without any overrides.
dbutils.widgets.text("catalog", "retail_banking", "Unity Catalog name")
dbutils.widgets.text("lookback_months", "12", "Txn lookback window (months)")
dbutils.widgets.text("risk_score_threshold", "700", "Bureau risk-score threshold")
# Run-control flags (replace the orchestrator's --skip-bteq/--skip-sas/--dry-run)
dbutils.widgets.dropdown("skip_bteq", "false", ["true", "false"], "Skip staging (BTEQ) layer")
dbutils.widgets.dropdown("skip_sas", "false", ["true", "false"], "Skip analytics (SAS) layer")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (plan only)")

# COMMAND ----------

# ---- Catalog / schema configuration ----------------------------------------
CATALOG = dbutils.widgets.get("catalog")

SCHEMA_CORE = "core_banking"      # was CORE_BANKING_DB
SCHEMA_TXN = "txn_processing"     # was TXN_PROCESSING_DB
SCHEMA_STG = "etl_staging"        # was ETL_STAGING_DB
SCHEMA_DP = "data_products"       # was DATA_PRODUCTS_DB

# ---- Run parameters (formerly env vars in pipeline_config.cfg) --------------
LOOKBACK_MONTHS = int(dbutils.widgets.get("lookback_months"))
RISK_SCORE_THRESHOLD = int(dbutils.widgets.get("risk_score_threshold"))

# ---- Run-control flags ------------------------------------------------------
SKIP_BTEQ = dbutils.widgets.get("skip_bteq") == "true"
SKIP_SAS = dbutils.widgets.get("skip_sas") == "true"
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

# ---- Model versions (formerly %let MODEL_VERSION in each SAS program) -------
MODEL_VERSION_SEGMENTS = "SEG_V3.2"
MODEL_VERSION_TXN = "TXN_V2.1"
MODEL_VERSION_RISK = "RISK_V4.0"
MODEL_VERSION_MASTER = "MASTER_V1.5"

# ---- Audit table (replaces ETL_RUN_LOG + SAS WORK.PIPELINE_AUDIT) -----------
AUDIT_TABLE = f"{CATALOG}.{SCHEMA_STG}.etl_run_log"

# COMMAND ----------


def core(table: str) -> str:
    """Fully-qualified name for a core_banking (CORE_BANKING_DB) table."""
    return f"{CATALOG}.{SCHEMA_CORE}.{table}"


def txn(table: str) -> str:
    """Fully-qualified name for a txn_processing (TXN_PROCESSING_DB) table."""
    return f"{CATALOG}.{SCHEMA_TXN}.{table}"


def stg(table: str) -> str:
    """Fully-qualified name for an etl_staging (ETL_STAGING_DB) table."""
    return f"{CATALOG}.{SCHEMA_STG}.{table}"


def dp(table: str) -> str:
    """Fully-qualified name for a data_products (DATA_PRODUCTS_DB) table."""
    return f"{CATALOG}.{SCHEMA_DP}.{table}"


# COMMAND ----------

print("Pipeline configuration loaded")
print(f"  catalog               = {CATALOG}")
print(f"  lookback_months       = {LOOKBACK_MONTHS}")
print(f"  risk_score_threshold  = {RISK_SCORE_THRESHOLD}")
print(f"  audit_table           = {AUDIT_TABLE}")
