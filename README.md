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
