# Retail Banking Customer Analytics Pipeline

An end-to-end data engineering demo showing **Teradata BTEQ** scripts transforming
operational tables into staging datasets, which then flow into **SAS** analytical
pipelines, producing a final set of certified **data product** tables.

> **The full pipeline now runs locally with no Teradata or SAS install.** All
> legacy BTEQ and SAS ETL logic has been migrated into a single Python/DuckDB
> engine (`local/duckdb/run_demo.py`). The original BTEQ/SAS scripts are kept as
> the source-of-truth reference for the migrated logic. See
> [Primary Execution Path](#primary-execution-path-pythonduckdb) below.

## Primary Execution Path (Python + DuckDB)

The canonical way to run the pipeline is the Python/DuckDB engine, which
reproduces every BTEQ transformation and SAS analytics program with no external
database or proprietary runtime:

```bash
# Generate sources, run all 3 phases, export every CSV (5,000 customers default)
uv run export_data.py

# Custom customer count
uv run export_data.py --customers 10000
```

This writes source, staging, and data-product CSVs to `data/01_source_tables`,
`data/02_bteq_staging`, and `data/03_sas_data_products`.

### How the legacy logic maps to the engine

`export_data.py` drives three public functions in `local/duckdb/run_demo.py`:

| Phase | Engine function | Replaces (source of truth) | Technique |
|-------|-----------------|----------------------------|-----------|
| 1 - Source generation | `_builtin_populate_sources` | n/a (synthetic data) | Faker + numpy, fixed seed |
| 2 - BTEQ staging | `phase2_bteq_transforms` | `bteq/01_stg_customer_360.bteq`, `bteq/02_stg_txn_summary.bteq`, `bteq/03_stg_risk_factors.bteq` | DuckDB SQL (joins, aggregations, window functions, `STDDEV_POP`, CASE) |
| 3 - SAS analytics | `phase3_python_analytics` | `sas/01_sas_customer_segments.sas`, `sas/02_sas_txn_analytics.sas`, `sas/03_sas_risk_scoring.sas`, `sas/04_sas_data_products.sas` | pandas + scikit-learn (k-means, percentile rank + IQR, logistic regression, golden-record merge) |

Produced tables follow the schemas in `ddl/01_staging_tables.sql` and
`ddl/02_data_product_tables.sql`. The run is deterministic (fixed RNG seeds)
apart from wall-clock `LOAD_TS` columns.

The Teradata BTEQ + SAS path documented below is the **legacy reference**
architecture; it requires the proprietary stack in [Prerequisites](#prerequisites-legacy-teradatasas-path).

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
├── bteq/
│   ├── 01_stg_customer_360.bteq           # Customer denormalization
│   ├── 02_stg_txn_summary.bteq           # Transaction aggregation
│   ├── 03_stg_risk_factors.bteq          # Risk feature engineering
│   └── run_bteq_pipeline.sh              # BTEQ orchestrator
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
├── local/
│   └── duckdb/
│       └── run_demo.py                    # Python/DuckDB engine (migrated BTEQ + SAS)
├── export_data.py                         # Primary entrypoint: runs all 3 phases, exports CSVs
└── docs/
    └── pipeline_flow.md                   # Detailed technical documentation
```

## Pipeline Phases

### Phase 1: Teradata BTEQ Staging

The BTEQ scripts run directly on Teradata to perform heavy-lifting transformations
close to the data. Each script follows a consistent pattern:

| Script | Source Tables | Target | Key Operations |
|--------|-------------|--------|----------------|
| `01_stg_customer_360.bteq` | CUSTOMERS, ACCOUNTS, ADDRESSES | STG_CUSTOMER_360 | LEFT JOINs, QUALIFY ROW_NUMBER, CASE expressions, derived metrics |
| `02_stg_txn_summary.bteq` | TRANSACTIONS, TRANSACTION_TYPES, ACCOUNTS | STG_TXN_SUMMARY | Aggregations (SUM/AVG/COUNT), channel mix %, volatile table params |
| `03_stg_risk_factors.bteq` | TRANSACTIONS, ACCOUNTS, CUSTOMERS | STG_RISK_FACTORS | Work tables, STDDEV_POP, velocity calcs, multi-pass joins, cleanup |

BTEQ conventions used:
- `.SET ERRORLEVEL 3807 SEVERITY 0` - suppress "table does not exist" on DROP
- `.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE` - fail-fast error handling
- `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` - zero-row validation
- `COLLECT STATISTICS` after every table creation
- `CREATE TABLE ... AS (...) WITH DATA PRIMARY INDEX (...)` pattern
- ETL_RUN_LOG audit inserts at each step

### Phase 2: SAS Analytics

SAS programs read from the staging tables via SAS/ACCESS to Teradata and apply
statistical and business-rule transformations:

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

## Running the Legacy Teradata/SAS Pipeline

> Requires the proprietary Teradata + SAS stack (see Prerequisites). For local
> runs, use the [Python/DuckDB engine](#primary-execution-path-pythonduckdb).

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

### Python/DuckDB path (primary)

- **uv** (or Python 3.10+). Dependencies (`duckdb`, `pandas`, `scikit-learn`,
  `numpy`, `faker`) are declared inline in `export_data.py` and auto-installed by
  `uv run`. No Teradata or SAS required.

### Legacy Teradata/SAS path

- **Teradata**: BTEQ client (TTU 17.x+), service account with SELECT on source DBs
  and ALL on staging/data product DBs
- **SAS**: SAS 9.4 M7+ with Base SAS, SAS/STAT, SAS/ACCESS Interface to Teradata
- **Shell**: bash 4+, `envsubst` (from gettext)

## Reference Repositories

This demo was informed by patterns found in these open-source repositories:

- [sassoftware](https://github.com/sassoftware) - Official SAS open-source org (saspy, vscode-sas-extension)
- [vildaduan/clinical-sas-cdisc-pipeline](https://github.com/vildaduan/clinical-sas-cdisc-pipeline) - Numbered SAS pipeline with data steps and PROC transforms
- [pankotskyi/ecommerce-pipeline](https://github.com/pankotskyi/ecommerce-pipeline) - SAS star-schema ETL with fact/dimension tables
- [sajithkalarikal/process-automation](https://github.com/sajithkalarikal/process-automation) - Real-world BTEQ + shell automation with Teradata
- [xlfe/dwhwrapper](https://github.com/xlfe/dwhwrapper) - CLI wrapper for Teradata BTEQ
- [sassoftware/sas-viya-dmml-pipelines](https://github.com/sassoftware/sas-viya-dmml-pipelines) - SAS Viya data mining/ML pipelines
