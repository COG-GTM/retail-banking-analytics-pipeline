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
                    │   dbt STAGING LAYER (Databricks / Delta)  │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ stg_customer_360.sql                │ │
                    │  │  - JOIN customers + accounts + addr  │ │
                    │  │  - Derive age, tenure, credit util   │ │
                    │  │  → stg_customer_360 (Delta)          │ │
                    │  └─────────────────────────────────────┘ │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ stg_txn_summary.sql                 │ │
                    │  │  - Aggregate txns by customer/acct   │ │
                    │  │  - Channel mix, merchant diversity   │ │
                    │  │  → stg_txn_summary (Delta)           │ │
                    │  └─────────────────────────────────────┘ │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ stg_risk_factors.sql                │ │
                    │  │  - Balance volatility (stddev_pop)   │ │
                    │  │  - Payment history, velocity metrics │ │
                    │  │  - Merchant risk indicators          │ │
                    │  │  → stg_risk_factors (Delta)          │ │
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

### Staging Layer (dbt on Databricks)

The staging layer was migrated from Teradata BTEQ to dbt models that run on
Databricks (Delta tables / Spark SQL). The three models live in
`dbt/models/staging/` and are ported 1:1 from the `bteq/` scripts, preserving the
output column names and business logic documented in `ddl/01_staging_tables.sql`.

- Models are materialized as **Delta tables** (`dbt-databricks` default); the
  Teradata `PRIMARY INDEX` / `COLLECT STATISTICS` constructs were removed.
- The BTEQ `.SET` / `.LOGON` / `.IF` / `.LABEL` / `.EXIT` orchestration control
  and `ETL_RUN_LOG` audit inserts were dropped — dbt handles run orchestration,
  error propagation, and run logging.
- Validation/audit logic moved to **dbt tests** in
  `dbt/models/staging/_staging.yml`: `not_null` + `unique` on `customer_id`
  (`stg_customer_360`, `stg_risk_factors`), `not_null` on
  (`customer_id`, `account_id`) plus a combination-uniqueness test
  (`stg_txn_summary`), and a row-count expectation replacing the old
  `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` guard.
- The `VT_RUN_PARAMS` volatile table became a `run_params` CTE derived from
  `var('lookback_months')` (default 12), and Teradata-only functions/casts
  (`NULLIFZERO`, inline `(INTEGER)`) were replaced with `nullif(...)` /
  `cast(... as int)`.
- Run with `dbt build --select staging`.

Legacy Teradata BTEQ conventions (retained in `bteq/` for reference only):
- `.SET ERRORLEVEL 3807 SEVERITY 0` suppressed "table does not exist" on DROP TABLE
- `.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE` after every DML/DDL statement
- `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` as a zero-row guard
- `.GOTO` labels to skip past expected errors (e.g., DROP on first run)
- ETL_RUN_LOG audit inserts tracked job name, row counts, and timestamps

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
