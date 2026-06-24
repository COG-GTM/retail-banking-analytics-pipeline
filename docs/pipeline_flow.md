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

## dbt Re-Implementation

The `dbt/` project re-expresses the BTEQ + SQL-based SAS logic as a single dbt
DAG on Teradata (`dbt-teradata`). It does not replace the BTEQ/SAS scripts; it
sits alongside them for validation. Full details are in `dbt/README.md`.

### Layering

```
   SOURCES                 STAGING (views)            INTERMEDIATE                MARTS (tables)
   ===============         =====================      =========================   ==========================
   core_banking.*    ┌──▶  stg_customer_360   ──┬──▶  int_customer_segment_       (external k-means)
   (customers,       │                          │     features (view) ───────────────▶ seed: customer_segments ─┐
    accounts,        │                          │                                                                │
    addresses,       │                          └────────────────────────────────────────────────────────────  ├─▶ customer_master_profile
    bureau_scores)   │     stg_txn_summary    ──────▶  transaction_analytics ─────────────────────────────────  │
                     │                                                                                            │
   txn_processing.*  ┘     stg_risk_factors   ◀── int_wrk_daily_balance (ephemeral)                              │
   (transactions,                              ◀── int_wrk_payment_history (ephemeral)                           │
    transaction_types)            │                                                                              │
                                  └──────────▶ (external logistic regression) ─▶ seed: customer_risk_scores ─────┘
```

### Why two steps stay external

`PROC FASTCLUS` (k-means) and `PROC LOGISTIC` (probability of default) require
model fitting that cannot be expressed in dbt SQL. Their outputs are loaded as
dbt seeds (from `data/03_sas_data_products/`) so `customer_master_profile` can
still join them; the feature engineering that feeds segmentation is implemented
as the `int_customer_segment_features` model. Percentile ranking (`PROC RANK`)
and IQR anomaly detection (`PROC MEANS`) in `transaction_analytics` are
re-expressed in SQL (`ntile`, `percentile_cont`) and stay inside dbt.

### Materialization & schema mapping

| Layer | Materialization | Teradata DB (from `config/pipeline_config.cfg`) |
|-------|-----------------|--------------------------------------------------|
| staging | view | `ETL_STAGING_DB` (`DB_STG`) |
| intermediate work tables | ephemeral | inlined |
| segmentation features | view | `ETL_STAGING_DB` (`DB_STG`) |
| marts | table | `DATA_PRODUCTS_DB` (`DB_DP`) |
| seeds (ML outputs) | seed | `DATA_PRODUCTS_DB` (`DB_DP`) |
