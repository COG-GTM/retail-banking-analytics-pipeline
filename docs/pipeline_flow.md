# Pipeline Technical Documentation

## Primary Execution Path: Python + DuckDB

The pipeline's primary, dependency-free execution path is the Python/DuckDB
engine in `local/duckdb/run_demo.py`, driven by `export_data.py`. It reproduces
all legacy Teradata BTEQ and SAS ETL logic so the full pipeline runs without a
Teradata or SAS install:

```bash
uv run export_data.py                    # 5,000 customers (default)
uv run export_data.py --customers 10000  # custom count
```

### Phase mapping (migrated logic)

```
 SOURCE GENERATION            BTEQ STAGING (DuckDB SQL)         SAS ANALYTICS (pandas + scikit-learn)
 =================            ========================         =====================================
 _builtin_populate_sources    phase2_bteq_transforms           phase3_python_analytics
   Faker + numpy                _stg_customer_360   <- bteq/01    _sas_customer_segments  <- sas/01 (k-means, k=5)
   fixed seed (42)              _stg_txn_summary    <- bteq/02    _sas_txn_analytics      <- sas/02 (PROC RANK + IQR)
                               _stg_risk_factors    <- bteq/03    _sas_risk_scoring       <- sas/03 (logistic PD)
                                                                  _sas_data_products      <- sas/04 (golden record)
```

| Legacy artifact | Migrated to | Notes on faithful reproduction |
|-----------------|-------------|--------------------------------|
| `bteq/01_stg_customer_360.bteq` | `_stg_customer_360` | LEFT JOINs, `QUALIFY ROW_NUMBER()` for primary HOME address, account-portfolio aggregation, `AGE`/`TENURE_MONTHS`/`CREDIT_UTILIZATION_PCT` derivations |
| `bteq/02_stg_txn_summary.bteq` | `_stg_txn_summary` | 12-month lookback (`LOOKBACK_MONTHS`), volume/amount aggregations, channel-mix %, top merchant category per account, recency |
| `bteq/03_stg_risk_factors.bteq` | `_stg_risk_factors` | Daily-balance & payment-history work CTEs, `STDDEV_POP` balance volatility, overdraft/NSF, large-withdrawal, velocity (7d/30d), bureau join, new-merchant & high-risk-merchant indicators |
| `sas/01_sas_customer_segments.sas` | `_sas_customer_segments` | `StandardScaler` (PROC STDIZE) + `KMeans` k=5 (PROC FASTCLUS); clusters labelled by mean balance rank; LTV/engagement/breadth scores and action flags |
| `sas/02_sas_txn_analytics.sas` | `_sas_txn_analytics` | Customer-level aggregation, spend-trend rules, `PROC RANK groups=100` percentile buckets (0–99), `PROC MEANS` median+3·IQR anomaly flag |
| `sas/03_sas_risk_scoring.sas` | `_sas_risk_scoring` | `LogisticRegression` PD model, weighted composite risk score, 5-tier classification, top-two risk drivers, watch-list/review flags |
| `sas/04_sas_data_products.sas` | `_sas_data_products` | 4-way left merge into `CUSTOMER_MASTER_PROFILE` with default handling for missing upstream products |

Output schemas conform to `ddl/01_staging_tables.sql` and
`ddl/02_data_product_tables.sql`. Runs are deterministic (fixed RNG seeds) apart
from wall-clock `LOAD_TS` columns.

The Teradata/SAS data flow below is retained as the **legacy reference**.

## Data Flow Diagram

```
                        ┌──────────────────────────────────┐
                        │     OPERATIONAL SOURCE SYSTEMS    │
                        │  ┌───────────┐  ┌──────────────┐ │
                        │  │CORE_BANK_ │  │TXN_PROCESS_  │ │
                        │  │    DB      │  │     DB       │ │
                        │  │           │  │              │ │
                        │  │ CUSTOMERS │  │ TRANSACTIONS │ │
                        │  │ ACCOUNTS  │  │ TXN_TYPES    │ │
                        │  │ ADDRESSES │  │              │ │
                        │  └─────┬─────┘  └──────┬───────┘ │
                        └────────┼───────────────┼─────────┘
                                 │               │
                    ─────────────▼───────────────▼──────────────
                    │        BTEQ STAGING LAYER (Teradata)     │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ 01_stg_customer_360.bteq            │ │
                    │  │  - JOIN customers + accounts + addr  │ │
                    │  │  - Derive age, tenure, credit util   │ │
                    │  │  → STG_CUSTOMER_360                  │ │
                    │  └─────────────────────────────────────┘ │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ 02_stg_txn_summary.bteq             │ │
                    │  │  - Aggregate txns by customer/acct   │ │
                    │  │  - Channel mix, merchant diversity   │ │
                    │  │  → STG_TXN_SUMMARY                   │ │
                    │  └─────────────────────────────────────┘ │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ 03_stg_risk_factors.bteq            │ │
                    │  │  - Balance volatility (STDDEV_POP)   │ │
                    │  │  - Payment history, velocity metrics │ │
                    │  │  - Merchant risk indicators          │ │
                    │  │  → STG_RISK_FACTORS                  │ │
                    │  └─────────────────────────────────────┘ │
                    ───────────────────┬────────────────────────
                                       │
                    ───────────────────▼────────────────────────
                    │          SAS ANALYTICS LAYER             │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ 01_sas_customer_segments.sas        │ │
                    │  │  - PROC STDIZE → normalize features │ │
                    │  │  - PROC FASTCLUS → k-means (k=5)   │ │
                    │  │  - Label clusters, set action flags │ │
                    │  │  → CUSTOMER_SEGMENTS                 │ │
                    │  └─────────────────────────────────────┘ │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ 02_sas_txn_analytics.sas            │ │
                    │  │  - Customer-level aggregation        │ │
                    │  │  - PROC RANK → spend percentiles    │ │
                    │  │  - PROC MEANS → IQR anomaly flags   │ │
                    │  │  → TRANSACTION_ANALYTICS             │ │
                    │  └─────────────────────────────────────┘ │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ 03_sas_risk_scoring.sas             │ │
                    │  │  - Feature prep & imputation         │ │
                    │  │  - PROC LOGISTIC → PD model         │ │
                    │  │  - Weighted composite scoring        │ │
                    │  │  - Tier classification (5 levels)    │ │
                    │  │  → CUSTOMER_RISK_SCORES              │ │
                    │  └─────────────────────────────────────┘ │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ 04_sas_data_products.sas            │ │
                    │  │  - 4-way MERGE of all products      │ │
                    │  │  - Default handling for gaps         │ │
                    │  │  - Completeness quality report       │ │
                    │  │  - COLLECT STATISTICS on all DPs     │ │
                    │  │  → CUSTOMER_MASTER_PROFILE           │ │
                    │  └─────────────────────────────────────┘ │
                    ───────────────────┬────────────────────────
                                       │
                    ───────────────────▼────────────────────────
                    │        DATA PRODUCT LAYER (Teradata)     │
                    │        DATA_PRODUCTS_DB                   │
                    │                                          │
                    │  ┌──────────────────┐ ┌────────────────┐ │
                    │  │CUSTOMER_SEGMENTS │ │  TRANSACTION_  │ │
                    │  │                  │ │  ANALYTICS     │ │
                    │  │ segment_name     │ │                │ │
                    │  │ ltv_score        │ │ spend_trend    │ │
                    │  │ engagement_score │ │ percentile     │ │
                    │  │ cross_sell_flag  │ │ anomaly_flag   │ │
                    │  └────────┬─────────┘ └──────┬─────────┘ │
                    │           │                   │           │
                    │  ┌────────▼───────────────────▼─────────┐ │
                    │  │    CUSTOMER_MASTER_PROFILE            │ │
                    │  │    (Golden Record)                    │ │
                    │  │                                      │ │
                    │  │    Demographics + Segments +          │ │
                    │  │    Transactions + Risk Scores         │ │
                    │  └────────▲───────────────────▲─────────┘ │
                    │           │                   │           │
                    │  ┌────────┴─────────┐ ┌──────┴─────────┐ │
                    │  │CUSTOMER_RISK_    │ │  STG_CUSTOMER_ │ │
                    │  │   SCORES         │ │     360        │ │
                    │  │                  │ │  (base attrs)  │ │
                    │  │ risk_score       │ │                │ │
                    │  │ risk_tier        │ └────────────────┘ │
                    │  │ prob_default     │                    │
                    │  │ watch_list_flag  │                    │
                    │  └──────────────────┘                    │
                    ────────────────────────────────────────────
```

## Error Handling Strategy

### BTEQ Layer
- `.SET ERRORLEVEL 3807 SEVERITY 0` suppresses "table does not exist" on DROP TABLE
- `.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE` after every DML/DDL statement
- `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` as a zero-row guard
- `.GOTO` labels to skip past expected errors (e.g., DROP on first run)
- ETL_RUN_LOG audit inserts track job name, row counts, and timestamps

### SAS Layer
- `%validate_table` macro checks row counts, key uniqueness, and NOT NULL columns
- `%ABORT CANCEL` halts the program on validation failure
- `%log_step` macro writes structured audit trail to WORK.PIPELINE_AUDIT
- Pass-through SQL with error code checking
- SAS exit code inspection in shell wrapper (rc >= 2 = error)

### Shell Orchestrator
- `set -euo pipefail` for strict error handling
- BTEQ failure blocks SAS from running (dependency enforcement)
- Log rotation (gzip logs > 30 days)
- `--dry-run` mode for safe testing

## Scheduling

Typical cron schedule for daily execution:

```
# M  H  DOM MON DOW  COMMAND
  0  2  *   *   *    /opt/etl/retail_banking_analytics/orchestration/run_full_pipeline.sh >> /opt/etl/retail_banking_analytics/logs/cron.log 2>&1
```

Timeline:
- **02:00** BTEQ Phase starts (~20 min)
- **02:20** SAS Phase starts (~40 min)
- **03:00** Post-validation and log archival
- **03:05** Data products available to consumers
