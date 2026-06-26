# Retail Banking Customer Analytics Pipeline

An end-to-end data engineering demo that transforms operational banking tables
into a set of certified **data product** tables (customer segments, transaction
analytics, risk scores, and a golden-record master profile).

> **Migration note:** This pipeline was originally built on **Teradata BTEQ** +
> **SAS**. It has been migrated to a **Databricks-native** implementation using
> **Unity Catalog**, **Delta** tables, **Spark SQL / PySpark** notebooks, and a
> **Databricks Workflow** (Asset Bundle) for orchestration. The new code lives
> under [`databricks/`](databricks/). The original `bteq/`, `sas/`, and `ddl/`
> directories are retained **as reference** and are no longer used at runtime.

## Architecture (Databricks)

```
 SOURCE (Delta / UC)              STAGING (Spark SQL)          ANALYTICS (PySpark/ML)        DATA PRODUCTS (Delta)
 ===================              ===================          ======================        =====================

 retail_banking.core_banking                                                                retail_banking.data_products
 ├─ customers ──────────┐
 ├─ accounts  ──────────┼─▶ staging/01_stg_customer_360 ─▶ analytics/01_customer_segments ─▶ customer_segments
 ├─ addresses ──────────┘    (stg_customer_360)              (StandardScaler + KMeans k=5)
 │
 retail_banking.txn_processing
 ├─ transactions ───────┐
 ├─ transaction_types ──┼─▶ staging/02_stg_txn_summary ──▶ analytics/02_txn_analytics ────▶ transaction_analytics
 │                      │    (stg_txn_summary)               (percent_rank + IQR anomaly)
 │
 ├─ transactions ───────┐
 ├─ accounts / customers┼─▶ staging/03_stg_risk_factors ─▶ analytics/03_risk_scoring ─────▶ customer_risk_scores
 │  bureau scores ──────┘    (stg_risk_factors)              (LogisticRegression)
 │
 │                                                          analytics/04_data_products ────▶ customer_master_profile
 │                                                          (Spark left joins + coalesce)     (golden record)
```

The four former Teradata databases map to Unity Catalog schemas under a single
catalog (default `retail_banking`):

| Teradata database   | Unity Catalog schema             |
|---------------------|----------------------------------|
| `CORE_BANKING_DB`   | `retail_banking.core_banking`    |
| `TXN_PROCESSING_DB` | `retail_banking.txn_processing`  |
| `ETL_STAGING_DB`    | `retail_banking.etl_staging`     |
| `DATA_PRODUCTS_DB`  | `retail_banking.data_products`   |

## Directory Structure (`databricks/`)

```
databricks/
├── databricks.yml                      # Asset Bundle (bundle name, vars, targets)
├── resources/
│   └── retail_banking_analytics.job.yml  # Databricks Workflow (job) definition
├── config/
│   ├── pipeline_config.py              # Config notebook (catalog/schema resolution, params)
│   └── pipeline_config.yml             # Human-readable config (replaces pipeline_config.cfg)
├── lib/
│   └── pipeline_utils.py               # log_step / validate_table / guard helpers (audit -> Delta)
├── ddl/
│   ├── 00_source_tables.sql            # Source tables (Delta + Unity Catalog)
│   ├── 01_staging_tables.sql           # Staging tables + etl_run_log audit table
│   └── 02_data_product_tables.sql      # Data product tables
├── setup/
│   └── 00_setup_unity_catalog.py       # Creates catalog/schemas, runs DDL, (opt.) loads sample data
├── staging/                            # BTEQ -> Spark SQL
│   ├── 01_stg_customer_360.py
│   ├── 02_stg_txn_summary.py
│   └── 03_stg_risk_factors.py
├── analytics/                          # SAS -> PySpark / ML
│   ├── 01_customer_segments.py
│   ├── 02_txn_analytics.py
│   ├── 03_risk_scoring.py
│   └── 04_data_products.py
└── validation/
    └── post_run_validation.py          # Post-run row-count checks on the 4 data products
```

## Migration mapping

| Legacy (Teradata / SAS)                              | Databricks                                                |
|-----------------------------------------------------|-----------------------------------------------------------|
| `CREATE MULTISET TABLE ... NO FALLBACK`             | `CREATE TABLE ... USING DELTA`                            |
| `PRIMARY INDEX (...)`                               | `CLUSTER BY (...)` (liquid clustering)                    |
| `COLLECT STATISTICS`                                | dropped (optional `ANALYZE TABLE ... COMPUTE STATISTICS`) |
| `VARCHAR`/`CHAR` · `TIMESTAMP(6)` · `FORMAT`        | `STRING` · `TIMESTAMP` · dropped                          |
| `*.bteq` (BTEQ)                                     | `databricks/staging/*.py` (Spark SQL)                     |
| `QUALIFY ROW_NUMBER()` · `NULLIFZERO` · `ADD_MONTHS`| native `QUALIFY` · `nullif(x,0)` · `add_months`           |
| `VOLATILE TABLE` · `WRK_*` tables                   | CTE / temporary views                                     |
| `PROC STDIZE` + `PROC FASTCLUS`                     | `StandardScaler` + `KMeans` (scikit-learn)               |
| `PROC RANK` + `PROC MEANS` (IQR)                    | `percent_rank()` + `percentile_approx`                    |
| `PROC LOGISTIC`                                     | `LogisticRegression` (scikit-learn)                       |
| 4-way data-step `MERGE` (`IN=`)                     | Spark `LEFT JOIN` + `coalesce`                            |
| `connect_teradata.sas` (LIBNAME/JDBC)              | `config/pipeline_config.py` (UC name resolution)         |
| `log_step.sas` / `validate_table.sas`              | `lib/pipeline_utils.py` helpers                           |
| `ETL_RUN_LOG` / `WORK.PIPELINE_AUDIT`              | `etl_staging.etl_run_log` (Delta)                        |
| `run_full_pipeline.sh` (+ BTEQ/SAS shells)          | Databricks Workflow (`*.job.yml`)                         |

Business semantics are preserved: identical staging columns and data product
schemas, the 5 segments (PREMIUM_WEALTH, ENGAGED_MAINSTREAM, GROWING_DIGITAL,
CREDIT_DEPENDENT, VALUE_BASIC), the 5 risk tiers (LOW, MODERATE, ELEVATED, HIGH,
CRITICAL), the weighted composite risk score, and thresholds such as
`LOOKBACK_MONTHS = 12` and `RISK_SCORE_THRESHOLD = 700`.

## Prerequisites

- **Databricks workspace** with **Unity Catalog** enabled, and permission to
  create a catalog (or an existing catalog you can write to — set the `catalog`
  parameter accordingly).
- **Databricks Runtime 15.4 LTS ML** (or compatible) — the segmentation and
  risk-scoring notebooks use `numpy` / `pandas` / `scikit-learn`, which ship with
  the ML runtime.
- **Databricks CLI v0.2+** (the one that supports Asset Bundles) configured with
  authentication to your workspace (`databricks configure` or env vars).
- Source data in the `core_banking` / `txn_processing` schemas. For a self-
  contained demo, point `sample_data_path` at a UC Volume holding the CSVs from
  `data/01_source_tables/` and the setup notebook will load them.

## Deploy & run

```bash
cd databricks

# 1. Set your workspace host in databricks.yml (targets.dev.workspace.host) or
#    via the DATABRICKS_HOST / DATABRICKS_TOKEN environment variables.

# 2. Deploy the bundle (uploads notebooks + creates the job)
databricks bundle deploy -t dev

# 3. Run the full pipeline
databricks bundle run retail_banking_analytics -t dev
```

### Parameters

Override at run time with `--params` (job parameters), e.g.:

```bash
# Use a different catalog and a 6-month lookback
databricks bundle run retail_banking_analytics -t dev \
  --params catalog=my_catalog,lookback_months=6

# Load sample source data during setup
databricks bundle run retail_banking_analytics -t dev \
  --params sample_data_path=/Volumes/retail_banking/etl_staging/landing
```

| Parameter              | Default          | Purpose                                            |
|------------------------|------------------|----------------------------------------------------|
| `catalog`              | `retail_banking` | Unity Catalog name                                 |
| `lookback_months`      | `12`             | Transaction lookback window                         |
| `risk_score_threshold` | `700`            | Bureau risk-score threshold                          |
| `sample_data_path`     | *(empty)*        | Optional UC Volume folder of source CSVs to load    |
| `skip_bteq`            | `false`          | Skip the staging layer (was `--skip-bteq`)          |
| `skip_sas`             | `false`          | Skip the analytics layer (was `--skip-sas`)         |
| `dry_run`              | `false`          | Plan only; notebooks exit without writing (was `--dry-run`) |
| `min_rows`             | `1`              | Minimum rows per data product (post-run validation) |

The workflow is fail-fast by construction: each task only runs after its
upstream dependencies succeed, so the analytics layer never runs if staging
fails, and `post_run_validation` only runs after the master profile is built.

## Reference implementation

`local/duckdb/run_demo.py` reimplements the same BTEQ transforms and SAS
analytics in DuckDB + scikit-learn. It is the authoritative reference for the
migrated business logic and is useful for local, workspace-free validation.

---

## Legacy Teradata + SAS (reference only)

The original implementation is kept for reference and audit. It is **not** part
of the Databricks runtime.

| Layer | Path | Description |
|-------|------|-------------|
| DDL   | `ddl/`           | Teradata `CREATE MULTISET TABLE` DDL |
| BTEQ  | `bteq/`          | Teradata staging transforms (`*.bteq`) |
| SAS   | `sas/`           | SAS analytics programs + macros |
| Config | `config/pipeline_config.cfg` | Teradata/SAS env vars and paths |
| Orchestration | `orchestration/run_full_pipeline.sh` | Shell master orchestrator |

See `docs/pipeline_flow.md` for the detailed legacy technical documentation.
