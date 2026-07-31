# Retail Banking Customer Analytics Pipeline

An end-to-end data engineering demo. Operational Teradata tables are transformed
into staging datasets and certified **data product** tables by a **dbt** project
(`dbt-teradata` adapter). The original **Teradata BTEQ** scripts and **SAS**
programs are retained under `bteq/` and `sas/` as legacy reference only.

## Architecture

```
 SOURCE TABLES (Teradata)          dbt STAGING                dbt MARTS                  DATA PRODUCTS
 ========================          ===============            ==============             ===============

 CORE_BANKING_DB                   ETL_STAGING_DB                                        DATA_PRODUCTS_DB
 ├─ CUSTOMERS ──────────┐
 ├─ ACCOUNTS  ──────────┼──▶ stg_customer_360 ──────┬──▶ customer_segments ─────────▶ CUSTOMER_SEGMENTS
 ├─ ADDRESSES ──────────┘         │                 │    (+ int_customer_segment_features)
 │                                │                 │      ▲ k-means scored outside dbt
 TXN_PROCESSING_DB                │                 │
 ├─ TRANSACTIONS ───────┐         │                 │
 ├─ TRANSACTION_TYPES ──┼──▶ stg_txn_summary ───────┼──▶ transaction_analytics ─────▶ TRANSACTION_ANALYTICS
 │  CORE_BANKING_DB     │         │                 │
 │  └─ ACCOUNTS ────────┘         │                 │
 │                                │                 │
 ├─ TRANSACTIONS ───────┐         │                 │
 ├─ TRANSACTION_TYPES ──┼──▶ stg_risk_factors ──────┼──▶ customer_risk_scores ──────▶ CUSTOMER_RISK_SCORES
 │  CORE_BANKING_DB     │    (+ int_daily_balance,  │    (+ int_customer_risk_features)
 │  ├─ CUSTOMERS ───────┘       int_payment_history)│      ▲ PD scored outside dbt
 │  └─ ACCOUNTS                                     │
 │                                                  └──▶ customer_master_profile ──▶ CUSTOMER_MASTER_PROFILE
 │                                                       (golden record assembly)      (enterprise-wide view)
```

## Directory Structure

```
demo/
├── README.md                              # This file
├── config/
│   └── pipeline_config.cfg                # Environment variables, DB refs, dbt paths
├── ddl/
│   ├── 00_source_tables.sql               # Source table DDL (documentation)
│   ├── 01_staging_tables.sql              # Staging table DDL - the model contract
│   └── 02_data_product_tables.sql         # Data product DDL - the model contract
├── dbt/
│   ├── dbt_project.yml                    # Project config, vars, tags, on-run-end hook
│   ├── packages.yml                       # dbt-utils
│   ├── profiles.yml                       # Teradata profile template (env vars)
│   ├── macros/
│   │   ├── generate_schema_name.sql       # Use the DDL database names verbatim
│   │   ├── log_run_results.sql            # ETL_RUN_LOG audit hook
│   │   ├── run_td_kmeans_segments.sql     # In-DB replacement for PROC FASTCLUS
│   │   └── run_td_glm_default_scores.sql  # In-DB replacement for PROC LOGISTIC
│   ├── models/
│   │   ├── staging/                       # _sources.yml + stg_* models (was BTEQ)
│   │   ├── intermediate/                  # int_* work tables and feature models
│   │   └── marts/                         # Data products (was SAS)
│   ├── seeds/                             # Scoring hand-off tables
│   └── tests/generic/min_row_count.sql    # Row-count floor (was %validate_table)
├── bteq/                                  # LEGACY - superseded by dbt/models/staging
├── sas/                                   # LEGACY - superseded by dbt/models/marts
├── orchestration/
│   └── run_full_pipeline.sh              # End-to-end dbt orchestrator
└── docs/
    └── pipeline_flow.md                   # Detailed technical documentation
```

## Pipeline Phases

### Phase 1: dbt Staging (`ETL_STAGING_DB`, tags `staging` / `intermediate`)

| Model | Sources | Target table | Replaces |
|-------|---------|--------------|----------|
| `stg_customer_360` | CUSTOMERS, ACCOUNTS, ADDRESSES | STG_CUSTOMER_360 | `01_stg_customer_360.bteq` |
| `stg_txn_summary` | TRANSACTIONS, TRANSACTION_TYPES, ACCOUNTS | STG_TXN_SUMMARY | `02_stg_txn_summary.bteq` |
| `stg_risk_factors` | TRANSACTIONS, ACCOUNTS, CUSTOMERS, CUSTOMER_BUREAU_SCORES | STG_RISK_FACTORS | `03_stg_risk_factors.bteq` |
| `int_daily_balance`, `int_payment_history` | TRANSACTIONS, ACCOUNTS | work tables | BTEQ `WRK_*` tables |

BTEQ conventions and their dbt equivalents:

| BTEQ | dbt |
|------|-----|
| `DROP`/`CREATE TABLE AS ... WITH DATA` | `materialized='table'` |
| `.IF ERRORCODE <> 0 THEN .EXIT` | dbt fails the node and its children |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | `min_row_count` test |
| `.GOTO` + work-table cleanup | DAG ordering via `ref()` |
| `COLLECT STATISTICS` | post-hook / DBA job (see `ddl/`) |
| `ETL_RUN_LOG` inserts | `on-run-end` hook + `target/run_results.json` |

### Phase 2: dbt Marts (`DATA_PRODUCTS_DB`, tag `marts`)

| Model | Input | Output table | Replaces |
|-------|-------|--------------|----------|
| `customer_segments` | `int_customer_segment_features` + segment hand-off | CUSTOMER_SEGMENTS | `01_sas_customer_segments.sas` |
| `transaction_analytics` | `stg_txn_summary` | TRANSACTION_ANALYTICS | `02_sas_txn_analytics.sas` |
| `customer_risk_scores` | `int_customer_risk_features` + PD hand-off | CUSTOMER_RISK_SCORES | `03_sas_risk_scoring.sas` |
| `customer_master_profile` | the three marts + `stg_customer_360` | CUSTOMER_MASTER_PROFILE | `04_sas_data_products.sas` |

SAS techniques and their dbt equivalents:

| SAS | dbt |
|-----|-----|
| `PROC RANK groups=100` | `NTILE(100) OVER (...)` |
| `PROC MEANS` IQR outliers | `PERCENTILE_CONT` window functions |
| 4-way data-step `MERGE` / `if _base` | `LEFT JOIN` from the base model |
| `%validate_table` | `unique`, `not_null`, `accepted_values`, `dbt_utils.expression_is_true`, `min_row_count` |
| `%log_step` / `ETL_RUN_LOG` | `on-run-end` hook + dbt run artifacts |
| `PROC STDIZE` | standardization in `int_customer_segment_features` |
| `PROC FASTCLUS` (k-means) | **not expressible in dbt SQL** - see scoring boundaries |
| `PROC LOGISTIC` (PD) | **not expressible in dbt SQL** - see scoring boundaries |

### Scoring boundaries (the two ML steps)

k-means clustering and probability-of-default scoring cannot run inside a dbt
SQL model, so each is isolated behind a hand-off table that the marts join back
to. Either populate it in-database with the shipped macros, or load the output
of an external scoring job with `dbt seed`:

| Hand-off seed | Consumed by | In-database refresh |
|---------------|-------------|---------------------|
| `customer_segment_assignments` | `customer_segments` | `dbt run-operation run_td_kmeans_segments` (TD_KMeans) |
| `customer_default_probabilities` | `customer_risk_scores` | `dbt run-operation run_td_glm_default_scores` (TD_GLM) |

Customers with no hand-off row are not dropped: they surface as `UNCLASSIFIED`
segments and a `0` default probability, so the DAG is runnable before the first
scoring pass.

### Phase 3: Data Products

Four certified data product tables in `DATA_PRODUCTS_DB`:

| Table | Description | Primary Consumer |
|-------|------------|------------------|
| **CUSTOMER_SEGMENTS** | Behavioural clusters with LTV, engagement, and action flags | Marketing, CRM |
| **TRANSACTION_ANALYTICS** | Per-customer spend trends, percentiles, anomaly flags | Finance, Fraud |
| **CUSTOMER_RISK_SCORES** | Composite risk scores with probability of default | Credit, Collections |
| **CUSTOMER_MASTER_PROFILE** | Golden record joining all products | Enterprise-wide |

The DDL in `ddl/01_staging_tables.sql` and `ddl/02_data_product_tables.sql`
remains the output contract for the models; column names, types and order are
unchanged from the BTEQ/SAS implementation.

## Running the Pipeline

```bash
export TD_PASSWORD='...'          # never stored in the repo

# Full end-to-end run (deps -> seed -> staging -> marts -> test)
./orchestration/run_full_pipeline.sh

# Build marts only, on existing staging data (was --skip-bteq)
./orchestration/run_full_pipeline.sh --skip-bteq

# Refresh staging only (was --skip-sas)
./orchestration/run_full_pipeline.sh --skip-sas

# Print the dbt commands without executing them
./orchestration/run_full_pipeline.sh --dry-run
```

Or drive dbt directly:

```bash
cd dbt
dbt deps
dbt seed
dbt run  --select tag:staging tag:intermediate
dbt run  --select tag:marts
dbt test
dbt docs generate && dbt docs serve
```

Run parameters live in `dbt/dbt_project.yml` as vars (`lookback_months`,
`risk_score_threshold`, the `*_model_version` stamps) and can be overridden per
run with `--vars`.

## Prerequisites

- **Teradata**: service account with SELECT on the source databases and ALL on
  `ETL_STAGING_DB` / `DATA_PRODUCTS_DB`
- **dbt**: Python 3.9+, `pip install -r dbt/requirements.txt`
  (dbt-core 1.12, dbt-teradata 1.11)
- **Shell**: bash 4+
- **Legacy only**: BTEQ client (TTU 17.x+) and SAS 9.4 M7+ are no longer needed
  by the pipeline

## Legacy BTEQ and SAS Scripts

`bteq/` and `sas/` are kept for reference during the migration. Their runners
(`bteq/run_bteq_pipeline.sh`, `sas/run_sas_pipeline.sh`) are deprecated: they
print a warning and exit unless `ALLOW_DEPRECATED_BTEQ=true` /
`ALLOW_DEPRECATED_SAS=true` is set. Do not add logic to them - change the dbt
models instead.

## Sample Data

`export_data.py` generates realistic source, staging and data-product CSVs under
`data/`. The logical table names it exports (`stg_customer_360`,
`customer_segments`, ...) are unchanged by the migration and map one-to-one onto
the dbt models.

## Reference Repositories

This demo was informed by patterns found in these open-source repositories:

- [sassoftware](https://github.com/sassoftware) - Official SAS open-source org (saspy, vscode-sas-extension)
- [vildaduan/clinical-sas-cdisc-pipeline](https://github.com/vildaduan/clinical-sas-cdisc-pipeline) - Numbered SAS pipeline with data steps and PROC transforms
- [pankotskyi/ecommerce-pipeline](https://github.com/pankotskyi/ecommerce-pipeline) - SAS star-schema ETL with fact/dimension tables
- [sajithkalarikal/process-automation](https://github.com/sajithkalarikal/process-automation) - Real-world BTEQ + shell automation with Teradata
- [xlfe/dwhwrapper](https://github.com/xlfe/dwhwrapper) - CLI wrapper for Teradata BTEQ
- [sassoftware/sas-viya-dmml-pipelines](https://github.com/sassoftware/sas-viya-dmml-pipelines) - SAS Viya data mining/ML pipelines
