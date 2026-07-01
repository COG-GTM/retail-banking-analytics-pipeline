# Retail Banking Customer Analytics Pipeline

An end-to-end data engineering demo showing **Teradata BTEQ** scripts transforming
operational tables into staging datasets, which then flow into **SAS** analytical
pipelines, producing a final set of certified **data product** tables.

## Architecture

```
 SOURCE TABLES (Teradata)           BTEQ STAGING              SAS ANALYTICS              DATA PRODUCTS
 ========================          ===============            ==============             ===============

 CORE_BANKING_DB                                                                        DATA_PRODUCTS_DB
 ├─ CUSTOMERS ──────────┐
 ├─ ACCOUNTS  ──────────┼──▶ 01_stg_customer_360.bteq ──▶ 01_customer_segments.sas ──▶ CUSTOMER_SEGMENTS
 ├─ ADDRESSES ──────────┘    (STG_CUSTOMER_360)             (k-means clustering)
 │
 TXN_PROCESSING_DB
 ├─ TRANSACTIONS ───────┐
 ├─ TRANSACTION_TYPES ──┼──▶ 02_stg_txn_summary.bteq  ──▶ 02_txn_analytics.sas    ──▶ TRANSACTION_ANALYTICS
 │  CORE_BANKING_DB     │    (STG_TXN_SUMMARY)             (trend + anomaly detect)
 │  └─ ACCOUNTS ────────┘
 │
 ├─ TRANSACTIONS ───────┐
 ├─ TRANSACTION_TYPES ──┼──▶ 03_stg_risk_factors.bteq  ──▶ 03_risk_scoring.sas     ──▶ CUSTOMER_RISK_SCORES
 │  CORE_BANKING_DB     │    (STG_RISK_FACTORS)             (logistic regression)
 │  ├─ CUSTOMERS ───────┘
 │  └─ ACCOUNTS
 │
 │                                                          04_data_products.sas    ──▶ CUSTOMER_MASTER_PROFILE
 │                                                          (golden record assembly)     (enterprise-wide view)
```

## Directory Structure

```
demo/
├── README.md                              # This file
├── config/
│   └── pipeline_config.cfg                # Environment variables, DB refs, paths
├── ddl/
│   ├── 00_source_tables.sql               # Source table DDL (documentation)
│   ├── 01_staging_tables.sql              # BTEQ staging table DDL
│   └── 02_data_product_tables.sql         # Final data product DDL
├── bteq/                                  # Legacy Teradata BTEQ (kept for reference)
│   ├── 01_stg_customer_360.bteq           # Customer denormalization
│   ├── 02_stg_txn_summary.bteq           # Transaction aggregation
│   ├── 03_stg_risk_factors.bteq          # Risk feature engineering
│   └── run_bteq_pipeline.sh              # BTEQ orchestrator
├── dbt/                                   # dbt project (staging on Databricks)
│   ├── dbt_project.yml                    # Project config, lookback_months var
│   ├── profiles.yml                       # Example databricks profile (no creds)
│   ├── packages.yml                       # dbt_utils dependency
│   └── models/staging/                    # Ported staging models + sources + tests
│       ├── _sources.yml                   # Source table declarations
│       ├── _staging.yml                   # Model + column tests
│       ├── stg_customer_360.sql           # from 01_stg_customer_360.bteq
│       ├── stg_txn_summary.sql            # from 02_stg_txn_summary.bteq
│       └── stg_risk_factors.sql           # from 03_stg_risk_factors.bteq
├── sas/
│   ├── macros/
│   │   ├── connect_teradata.sas           # Teradata LIBNAME connections
│   │   ├── log_step.sas                   # Pipeline logging macro
│   │   └── validate_table.sas             # Data quality validation macro
│   ├── 01_sas_customer_segments.sas       # Customer segmentation (PROC FASTCLUS)
│   ├── 02_sas_txn_analytics.sas           # Transaction analytics (PROC RANK)
│   ├── 03_sas_risk_scoring.sas            # Risk scoring (PROC LOGISTIC)
│   ├── 04_sas_data_products.sas           # Golden record assembly
│   └── run_sas_pipeline.sh               # SAS orchestrator
├── orchestration/
│   └── run_full_pipeline.sh              # End-to-end master orchestrator
└── docs/
    └── pipeline_flow.md                   # Detailed technical documentation
```

## Pipeline Phases

### Phase 1: Staging (dbt on Databricks)

> **Migration note:** The staging layer has been migrated off Teradata BTEQ. It now
> runs as **dbt models on Databricks** (Delta tables / Spark SQL) under `dbt/`.
> The original BTEQ scripts remain in `bteq/` for reference, but the dbt models are
> the source of truth. See [Running the dbt models](#running-the-dbt-models).

The dbt staging models perform the same heavy-lifting transformations, materialized
as Delta tables via the `dbt-databricks` adapter:

| dbt model | Ported from | Source Tables | Target (Delta) | Key Operations |
|-----------|-------------|---------------|----------------|----------------|
| `stg_customer_360.sql` | `01_stg_customer_360.bteq` | customers, accounts, addresses | `stg_customer_360` | LEFT JOINs, `QUALIFY ROW_NUMBER`, CASE expressions, derived metrics |
| `stg_txn_summary.sql` | `02_stg_txn_summary.bteq` | transactions, transaction_types, accounts | `stg_txn_summary` | Aggregations (SUM/AVG/COUNT), channel mix %, `lookback_months` var CTE |
| `stg_risk_factors.sql` | `03_stg_risk_factors.bteq` | transactions, accounts, customers, customer_bureau_scores | `stg_risk_factors` | CTE work tables, `stddev_pop`, velocity windows, multi-pass joins |

Teradata/BTEQ-only constructs were dropped during the port and replaced with dbt
equivalents:

| Legacy BTEQ construct | dbt / Databricks replacement |
|-----------------------|------------------------------|
| `CREATE TABLE ... WITH DATA PRIMARY INDEX (...)` | `+materialized: table` (Delta tables) |
| `COLLECT STATISTICS` | (removed — not needed on Databricks) |
| `.SET` / `.LOGON` / `.IF` / `.LABEL` / `.EXIT` | (removed — dbt handles orchestration & errors) |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | dbt row-count test in `_staging.yml` |
| `ETL_RUN_LOG` audit inserts | dbt run results / logs |
| `VT_RUN_PARAMS` volatile table | `run_params` CTE from `var('lookback_months')` |
| `NULLIFZERO(...)` | `nullif(..., 0)` |
| `(INTEGER)` inline casts | `cast(... as int)` |

### Phase 2: SAS Analytics

SAS programs consume the staging tables and apply statistical and business-rule
transformations. Following the dbt migration, these Phase 2 consumers now read the
**dbt-produced Delta tables** on Databricks (`stg_customer_360`, `stg_txn_summary`,
`stg_risk_factors`) instead of the Teradata BTEQ staging tables:

| Program | Input | Output | SAS Techniques |
|---------|-------|--------|----------------|
| `01_sas_customer_segments.sas` | STG_CUSTOMER_360 | CUSTOMER_SEGMENTS | PROC STDIZE, PROC FASTCLUS (k-means), feature engineering |
| `02_sas_txn_analytics.sas` | STG_TXN_SUMMARY | TRANSACTION_ANALYTICS | PROC RANK (percentiles), PROC MEANS (IQR anomaly detection) |
| `03_sas_risk_scoring.sas` | STG_RISK_FACTORS + STG_CUSTOMER_360 | CUSTOMER_RISK_SCORES | PROC LOGISTIC (stepwise selection), weighted composite scoring |
| `04_sas_data_products.sas` | All 3 upstream DPs + STG_CUSTOMER_360 | CUSTOMER_MASTER_PROFILE | 4-way MERGE, default handling, completeness reporting |

SAS patterns used:
- Shared macro library (`%connect_teradata`, `%log_step`, `%validate_table`)
- `PROC SQL` with pass-through for Teradata-side operations
- `PROC APPEND` with `FORCE` for bulk loading to Teradata
- Data step MERGE with IN= variables for outer-join semantics
- `%ABORT CANCEL` on validation failure
- Work library cleanup via `PROC DATASETS`

### Phase 3: Data Products

Four certified data product tables in `DATA_PRODUCTS_DB`:

| Table | Description | Primary Consumer |
|-------|------------|------------------|
| **CUSTOMER_SEGMENTS** | Behavioural clusters with LTV, engagement, and action flags | Marketing, CRM |
| **TRANSACTION_ANALYTICS** | Per-customer spend trends, percentiles, anomaly flags | Finance, Fraud |
| **CUSTOMER_RISK_SCORES** | Composite risk scores with probability of default | Credit, Collections |
| **CUSTOMER_MASTER_PROFILE** | Golden record joining all products | Enterprise-wide |

## Running the dbt models

The staging layer (Phase 1) runs as dbt models against a Databricks target.

```bash
cd dbt

# 1. Install package dependencies (dbt_utils)
dbt deps

# 2. Configure a Databricks profile. Copy dbt/profiles.yml to ~/.dbt/profiles.yml
#    (or set DBT_PROFILES_DIR=$(pwd)) and provide your workspace values, e.g.:
#      export DBT_DATABRICKS_HOST=dbc-xxxx.cloud.databricks.com
#      export DBT_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxx
#      export DBT_DATABRICKS_TOKEN=dapi...            # do NOT commit this
#      export DBT_DATABRICKS_CATALOG=retail_banking
#      export DBT_DATABRICKS_SCHEMA=etl_staging

# 3. Build the staging models and run their tests (Delta tables on Databricks)
dbt build --select staging
```

`dbt build --select staging` compiles and runs the three staging models
(`stg_customer_360`, `stg_txn_summary`, `stg_risk_factors`) as Delta tables and
executes the `not_null` / `unique` / combination-uniqueness / row-count tests
defined in `dbt/models/staging/_staging.yml`. The `lookback_months` var (default
`12`, mirroring `LOOKBACK_MONTHS` in `config/pipeline_config.cfg`) can be
overridden with `--vars '{lookback_months: 6}'`.

## Running the Pipeline

```bash
# Full end-to-end run
./orchestration/run_full_pipeline.sh

# Skip BTEQ (re-run only SAS on existing staging data)
./orchestration/run_full_pipeline.sh --skip-bteq

# Skip SAS (refresh only BTEQ staging)
./orchestration/run_full_pipeline.sh --skip-sas

# Dry run (print steps without executing)
./orchestration/run_full_pipeline.sh --dry-run
```

## Prerequisites

- **Databricks (staging)**: a workspace with a SQL warehouse or cluster, plus
  `dbt-core` and the `dbt-databricks` adapter (`pip install dbt-databricks`). The
  source schemas (`core_banking`, `txn_processing`) must exist in the target catalog.
- **SAS**: SAS 9.4 M7+ with Base SAS, SAS/STAT (reads the dbt-produced Delta tables)
- **Shell**: bash 4+, `envsubst` (from gettext)
- **Teradata** _(legacy only)_: BTEQ client (TTU 17.x+) — retained for the original
  `bteq/` scripts; no longer required for the staging layer.

## Reference Repositories

This demo was informed by patterns found in these open-source repositories:

- [sassoftware](https://github.com/sassoftware) - Official SAS open-source org (saspy, vscode-sas-extension)
- [vildaduan/clinical-sas-cdisc-pipeline](https://github.com/vildaduan/clinical-sas-cdisc-pipeline) - Numbered SAS pipeline with data steps and PROC transforms
- [pankotskyi/ecommerce-pipeline](https://github.com/pankotskyi/ecommerce-pipeline) - SAS star-schema ETL with fact/dimension tables
- [sajithkalarikal/process-automation](https://github.com/sajithkalarikal/process-automation) - Real-world BTEQ + shell automation with Teradata
- [xlfe/dwhwrapper](https://github.com/xlfe/dwhwrapper) - CLI wrapper for Teradata BTEQ
- [sassoftware/sas-viya-dmml-pipelines](https://github.com/sassoftware/sas-viya-dmml-pipelines) - SAS Viya data mining/ML pipelines
