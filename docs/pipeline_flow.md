# Pipeline Technical Documentation

## Execution Engines

There are two equivalent ways to run this pipeline:

1. **Python/DuckDB engine (primary)** — `local/duckdb/run_demo.py`, driven by
   `export_data.py`. Runs anywhere with Python + `uv`; no Teradata or SAS required.
   This is the migration target and the recommended path.
2. **Legacy Teradata BTEQ + SAS (reference)** — `bteq/`, `sas/`, `orchestration/`.
   Requires a live Teradata instance and a SAS install. Retained as the source of
   truth for the migrated transformation logic.

The diagram below describes the logical data flow. Both engines implement the same
flow and produce tables conforming to `ddl/01_staging_tables.sql` and
`ddl/02_data_product_tables.sql`.

### Python/DuckDB engine mapping

| Legacy artifact | `run_demo.py` function | Technique in the migrated engine |
|-----------------|------------------------|----------------------------------|
| (source systems) | `_builtin_populate_sources` | Seeded synthetic generation (Faker + NumPy) into `core_banking` / `txn_processing` schemas |
| `bteq/01_stg_customer_360.bteq` | `phase2_bteq_transforms` | DuckDB SQL: LEFT JOINs, `QUALIFY ROW_NUMBER`, age/tenure/credit-utilization derivations |
| `bteq/02_stg_txn_summary.bteq`  | `phase2_bteq_transforms` | DuckDB SQL: 12-month lookback aggregation, channel-mix %, top-merchant subquery |
| `bteq/03_stg_risk_factors.bteq` | `phase2_bteq_transforms` | DuckDB SQL CTEs: daily-balance & payment-history work tables, `STDDEV_POP`, velocity, merchant-risk |
| `sas/01_sas_customer_segments.sas` | `phase3_python_analytics` | `StandardScaler` (PROC STDIZE) + `KMeans` k=5 (PROC FASTCLUS), cluster labelling |
| `sas/02_sas_txn_analytics.sas`     | `phase3_python_analytics` | customer aggregation, percentile rank (PROC RANK), IQR anomaly flag (PROC MEANS) |
| `sas/03_sas_risk_scoring.sas`      | `phase3_python_analytics` | `LogisticRegression` (PROC LOGISTIC) PD + weighted composite score, tiering |
| `sas/04_sas_data_products.sas`     | `phase3_python_analytics` | 4-way pandas merge with default handling → `CUSTOMER_MASTER_PROFILE` |

> Migration note: the legacy late-payment proxy
> (`txn_date > open_date + (months_between+1)`) can never evaluate true, so
> `PAYMENT_LATE_CNT` is always 0 and the logistic PD target is single-class. The
> engine reproduces this faithfully; `PROBABILITY_OF_DEFAULT` therefore collapses
> to the base rate while `COMPOSITE_RISK_SCORE` (bureau/behaviour/velocity driven)
> remains the substantive risk signal.

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
