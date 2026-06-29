# Pipeline Technical Documentation

## Modernized Python/DuckDB Engine (Primary Execution Path)

The pipeline has been migrated off Teradata + SAS into a single, zero-install
Python engine backed by an in-memory **DuckDB** database:
`local/duckdb/run_demo.py`. The legacy `bteq/` and `sas/` artifacts remain as the
authoritative specification; the Python engine reproduces their logic exactly.

Run it with:

```bash
uv run local/duckdb/run_demo.py     # full pipeline + stdout summary
uv run export_data.py               # full pipeline + CSV export of every table
```

### Phase mapping

| Engine function | Replaces | Logic reproduced |
|-----------------|----------|------------------|
| `_builtin_populate_sources` | Teradata source ingestion | Deterministic synthetic `customers`, `accounts`, `addresses`, `transactions`, `transaction_types`, `customer_bureau_scores` (seeded) |
| `phase2_bteq_transforms` | `bteq/01_stg_customer_360.bteq` | DuckDB SQL: most-recent HOME address via `QUALIFY ROW_NUMBER()`, account portfolio aggregation, `AGE = (current_date - dob)/365.25`, `TENURE_MONTHS`, `CREDIT_UTILIZATION_PCT` |
| `phase2_bteq_transforms` | `bteq/02_stg_txn_summary.bteq` | Per-account aggregation over a 12-month window, debit/credit/fee counts & amounts, channel-mix %, top merchant category, recency |
| `phase2_bteq_transforms` | `bteq/03_stg_risk_factors.bteq` | Daily-balance + payment-history work tables, overdraft/NSF, large withdrawals, `STDDEV_POP` balance volatility, credit-util ratio, debit velocity, and *new*-merchant / high-risk indicators |
| `_phase3a_customer_segments` | `sas/01_sas_customer_segments.sas` | Feature engineering → `StandardScaler` (PROC STDIZE) → `KMeans` k=5 (PROC FASTCLUS), cluster labelling by balance, LTV/engagement scores, action flags |
| `_phase3b_txn_analytics` | `sas/02_sas_txn_analytics.sas` | Customer-level rollup, spend trend, percentile ranking (PROC RANK `groups=100`), IQR anomaly flag (PROC MEANS) |
| `_phase3c_risk_scoring` | `sas/03_sas_risk_scoring.sas` | Feature prep/imputation, `LogisticRegression` PD model (PROC LOGISTIC), weighted composite score, 5-tier classification, primary/secondary risk drivers |
| `_phase3d_master_profile` | `sas/04_sas_data_products.sas` | 4-way LEFT JOIN golden-record assembly with default handling → `CUSTOMER_MASTER_PROFILE` |
| `phase4_summary` | SAS `PROC PRINT`/`PROC FREQ` | Row counts, segment & risk-tier distributions, top-risk sample |

### Teradata → DuckDB SQL translation notes

- `MULTISET TABLE ... AS (...) WITH DATA` → `CREATE OR REPLACE TABLE ... AS SELECT ...`
- `QUALIFY ROW_NUMBER() OVER (...)` is supported natively by DuckDB.
- `ADD_MONTHS(CURRENT_DATE, -n)` → `current_date - INTERVAL 'n' MONTH`.
- `NULLIFZERO(COUNT(*))` → `GREATEST(COUNT(*), 1)` for safe channel-mix division.
- `STDDEV_POP`, `CASE`, and multi-pass `LEFT JOIN` aggregations carry over directly.
- The four `data_products.*` and three `etl_staging.*` tables match the column
  order and types defined in `ddl/01_staging_tables.sql` and
  `ddl/02_data_product_tables.sql`.

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
