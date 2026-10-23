# Pipeline Technical Documentation

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
                    │  │    (REF_HIGH_RISK_MERCHANT_CATEGORY) │ │
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

## Business Rule: High-Risk Merchant Categories

The only externally-maintained rule in the pipeline is the high-risk merchant list used by
`03_stg_risk_factors.bteq` to compute `HIGH_RISK_MERCHANT_CNT`.

- **Where it lives:** `ETL_STAGING_DB.REF_HIGH_RISK_MERCHANT_CATEGORY`
  (DDL `ddl/03_reference_tables.sql`, seed `ddl/04_reference_seed.sql`). Columns:
  `MERCHANT_CATEGORY`, `EFFECTIVE_DATE`, `EXPIRATION_DATE`, `CHANGE_REFERENCE`.
- **How it is applied:** at the start of `03_stg_risk_factors.bteq` the rows in force on the run date
  (`EFFECTIVE_DATE <= CURRENT_DATE`, `EXPIRATION_DATE` null or `> CURRENT_DATE`) are loaded into
  `VT_HIGH_RISK_MERCHANT_CATEGORY`; the 6-month transaction window is LEFT JOINed to it and matches are
  summed. An empty list aborts the script with exit 99.
- **Current rows:** GAMBLING, WIRE_TRANSFER_INTL, CRYPTO_EXCHANGE, PAWN_SHOP (effective 2020-01-01);
  PAYDAY_LENDER (effective 2026-11-01).
- **Downstream effect:** `HIGH_RISK_MERCHANT_CNT` is a candidate predictor in the `PROC LOGISTIC` PD model
  (`03_sas_risk_scoring.sas`) → `PROBABILITY_OF_DEFAULT`, `WATCH_LIST_FLAG` in `CUSTOMER_RISK_SCORES`
  → copied into `CUSTOMER_MASTER_PROFILE` by `04_sas_data_products.sas`.

**Rerun after a change to the list (in order):**

| # | Step | Rebuilds |
|---|------|----------|
| 1 | `bteq/03_stg_risk_factors.bteq` | STG_RISK_FACTORS |
| 2 | `sas/03_sas_risk_scoring.sas` | CUSTOMER_RISK_SCORES |
| 3 | `sas/04_sas_data_products.sas` | CUSTOMER_MASTER_PROFILE (same calendar day as step 2) |

**Do not rerun:** `01_stg_customer_360.bteq`, `02_stg_txn_summary.bteq`, `01_sas_customer_segments.sas`,
`02_sas_txn_analytics.sas` — none of them read the high-risk list. A row with a future `EFFECTIVE_DATE`
needs no manual rerun; the nightly run applies it from that date.

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
