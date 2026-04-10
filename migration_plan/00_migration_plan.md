# BTEQ/SAS to PySpark Migration Plan

## Retail Banking Customer Analytics Pipeline

**Version:** 1.0
**Date:** 2026-04-10
**Status:** Planning Phase

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current Architecture Inventory](#2-current-architecture-inventory)
3. [Target Architecture](#3-target-architecture)
4. [Migration Strategy](#4-migration-strategy)
5. [Detailed Conversion Guide](#5-detailed-conversion-guide)
6. [Data Lineage & Dependencies](#6-data-lineage--dependencies)
7. [Validation Strategy](#7-validation-strategy)
8. [Risk Assessment & Mitigations](#8-risk-assessment--mitigations)
9. [Timeline & Effort Estimates](#9-timeline--effort-estimates)
10. [Appendix: SAS-to-PySpark Pattern Reference](#10-appendix-sas-to-pyspark-pattern-reference)

---

## 1. Executive Summary

This document provides a comprehensive migration plan for converting the Retail Banking Customer Analytics Pipeline from **Teradata BTEQ + SAS 9.4** to **PySpark**. The pipeline currently:

- **Phase 1 (BTEQ):** Runs 3 staging scripts on Teradata, transforming 6 source tables into 3 staging tables
- **Phase 2 (SAS):** Runs 4 SAS analytics programs producing 4 certified data product tables
- **Orchestration:** Shell scripts coordinate execution with error handling and audit logging

The migration targets a PySpark-based architecture that can run on Spark clusters (Databricks, EMR, Dataproc) or locally, replacing both the BTEQ SQL transformations and SAS statistical procedures.

### Key Numbers

| Metric | Count |
|--------|-------|
| BTEQ scripts | 3 |
| SAS programs | 4 |
| SAS macros | 3 |
| Shell orchestrators | 3 |
| Source tables | 6 |
| Staging tables | 3 (+ 2 work tables) |
| Data product tables | 4 |
| Total lines of BTEQ | ~680 |
| Total lines of SAS | ~930 |
| Total lines of SQL DDL | ~378 |

---

## 2. Current Architecture Inventory

### 2.1 Source Tables (CORE_BANKING_DB / TXN_PROCESSING_DB)

| Table | Database | Row Count (sample) | Description |
|-------|----------|-------------------|-------------|
| `CUSTOMERS` | CORE_BANKING_DB | 500 | Master customer records |
| `ACCOUNTS` | CORE_BANKING_DB | 1,251 | Account-level detail (checking, savings, credit, loan) |
| `ADDRESSES` | CORE_BANKING_DB | 1,000 | Customer mailing and residential addresses |
| `TRANSACTIONS` | TXN_PROCESSING_DB | 80,528 | Individual financial transactions |
| `TRANSACTION_TYPES` | TXN_PROCESSING_DB | 8 | Reference/lookup for transaction type codes |
| `CUSTOMER_BUREAU_SCORES` | CORE_BANKING_DB | 500 | External credit bureau scores |

### 2.2 BTEQ Scripts (Phase 1 — Staging Layer)

| # | Script | Source Tables | Target Table | Lines | Key Operations |
|---|--------|--------------|--------------|-------|----------------|
| 1 | `01_stg_customer_360.bteq` | CUSTOMERS, ACCOUNTS, ADDRESSES | STG_CUSTOMER_360 | 154 | LEFT JOINs, QUALIFY ROW_NUMBER, CASE expressions, derived age/tenure/credit utilization |
| 2 | `02_stg_txn_summary.bteq` | TRANSACTIONS, TRANSACTION_TYPES, ACCOUNTS | STG_TXN_SUMMARY | 170 | Aggregations (SUM/AVG/COUNT), channel mix %, volatile table params, NULLIFZERO |
| 3 | `03_stg_risk_factors.bteq` | TRANSACTIONS, ACCOUNTS, CUSTOMERS, CUSTOMER_BUREAU_SCORES | STG_RISK_FACTORS | 356 | Work tables (WRK_DAILY_BALANCE, WRK_PAYMENT_HISTORY), STDDEV_POP, velocity calcs, multi-pass joins, correlated subquery |

**BTEQ Patterns Used:**
- `.SET ERRORLEVEL 3807 SEVERITY 0` — suppress "table does not exist" on DROP
- `.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE` — fail-fast error handling
- `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` — zero-row validation guard
- `CREATE TABLE ... AS (...) WITH DATA PRIMARY INDEX (...)` — CTAS pattern
- `COLLECT STATISTICS` after every table creation
- `ETL_RUN_LOG` audit inserts at each step
- Volatile tables for parameterized lookback windows
- `QUALIFY ROW_NUMBER() OVER (...)` — Teradata windowing

### 2.3 SAS Programs (Phase 2 — Analytics Layer)

| # | Program | Input | Output | Lines | SAS Techniques |
|---|---------|-------|--------|-------|----------------|
| 1 | `01_sas_customer_segments.sas` | STG_CUSTOMER_360 | CUSTOMER_SEGMENTS | 254 | PROC STDIZE (z-score normalization), PROC FASTCLUS (k-means k=5), DATA step feature engineering, cluster labeling |
| 2 | `02_sas_txn_analytics.sas` | STG_TXN_SUMMARY, STG_CUSTOMER_360 | TRANSACTION_ANALYTICS | 183 | PROC RANK (percentiles), PROC MEANS (IQR anomaly detection), DATA step trend classification |
| 3 | `03_sas_risk_scoring.sas` | STG_RISK_FACTORS, STG_CUSTOMER_360 | CUSTOMER_RISK_SCORES | 241 | PROC LOGISTIC (stepwise selection), weighted composite scoring, tier classification, risk driver identification |
| 4 | `04_sas_data_products.sas` | All 3 DPs + STG_CUSTOMER_360 | CUSTOMER_MASTER_PROFILE | 252 | 4-way MERGE with IN= variables, default handling, completeness reporting, PROC DATASETS cleanup |

### 2.4 SAS Macros

| Macro | File | Purpose | PySpark Equivalent |
|-------|------|---------|-------------------|
| `%connect_teradata()` | `macros/connect_teradata.sas` | Establish LIBNAME connections to 4 Teradata databases | `SparkSession.read` with JDBC or direct file reads |
| `%log_step()` | `macros/log_step.sas` | Structured audit trail logging with timestamps | Python `logging` module + audit DataFrame |
| `%validate_table()` | `macros/validate_table.sas` | Row count, key uniqueness, NOT NULL checks | Custom Python validation class |
| `%init_audit` | `macros/log_step.sas` | Initialize audit trail dataset | Audit DataFrame initialization |

### 2.5 Data Product Tables (Phase 3 — Output Layer)

| Table | Row Count (sample) | Primary Consumer | Description |
|-------|-------------------|------------------|-------------|
| `CUSTOMER_SEGMENTS` | 407 | Marketing, CRM | Behavioural clusters with LTV, engagement, action flags |
| `TRANSACTION_ANALYTICS` | 500 | Finance, Fraud | Per-customer spend trends, percentiles, anomaly flags |
| `CUSTOMER_RISK_SCORES` | 407 | Credit, Collections | Composite risk scores with probability of default |
| `CUSTOMER_MASTER_PROFILE` | 407 | Enterprise-wide | Golden record joining all products |

---

## 3. Target Architecture

### 3.1 PySpark Pipeline Structure

```
pyspark_pipeline/
├── config/
│   └── pipeline_config.py          # Replaces pipeline_config.cfg
├── utils/
│   ├── __init__.py
│   ├── logger.py                   # Replaces %log_step macro
│   ├── validator.py                # Replaces %validate_table macro
│   └── spark_session.py            # Replaces %connect_teradata macro
├── staging/
│   ├── __init__.py
│   ├── stg_customer_360.py         # Replaces 01_stg_customer_360.bteq
│   ├── stg_txn_summary.py          # Replaces 02_stg_txn_summary.bteq
│   └── stg_risk_factors.py         # Replaces 03_stg_risk_factors.bteq
├── analytics/
│   ├── __init__.py
│   ├── customer_segments.py        # Replaces 01_sas_customer_segments.sas
│   ├── txn_analytics.py            # Replaces 02_sas_txn_analytics.sas
│   ├── risk_scoring.py             # Replaces 03_sas_risk_scoring.sas
│   └── data_products.py            # Replaces 04_sas_data_products.sas
├── orchestration/
│   └── run_pipeline.py             # Replaces shell orchestrators
└── tests/
    ├── test_staging.py
    ├── test_analytics.py
    └── test_validation.py
```

### 3.2 Technology Stack

| Current | Target | Notes |
|---------|--------|-------|
| Teradata BTEQ | PySpark SQL / DataFrame API | All SQL transformations map directly |
| SAS 9.4 Base | PySpark DataFrame operations | DATA steps → DataFrame ops |
| SAS/STAT PROC FASTCLUS | PySpark MLlib KMeans | Same k-means algorithm |
| SAS/STAT PROC LOGISTIC | PySpark MLlib LogisticRegression | Stepwise selection needs manual feature selection |
| SAS/STAT PROC RANK | PySpark `percent_rank()` window function | Direct mapping |
| SAS/STAT PROC MEANS | PySpark `approxQuantile()` + `agg()` | IQR calculation via approx quantiles |
| SAS/STAT PROC STDIZE | PySpark MLlib StandardScaler | Z-score normalization |
| SAS PROC SQL | Spark SQL or DataFrame API | Direct mapping |
| SAS DATA step MERGE | PySpark `join()` with coalesce | IN= variables → join type + coalesce |
| Shell bash orchestration | Python orchestration script | Or Airflow/Prefect DAG |
| Teradata LIBNAME | Spark JDBC / Delta Lake / Parquet | Configurable data source |

---

## 4. Migration Strategy

### 4.1 Phased Approach

**Phase A: BTEQ Staging → PySpark SQL (Weeks 1-3)**
- Convert 3 BTEQ scripts to PySpark
- These are pure SQL transformations — most straightforward conversion
- Teradata-specific syntax mapping (QUALIFY, NULLIFZERO, volatile tables, etc.)
- Validate row counts and column values match original output

**Phase B: SAS Analytics → PySpark MLlib + DataFrame (Weeks 3-6)**
- Convert 4 SAS programs to PySpark
- Statistical procedures require MLlib equivalents
- Feature engineering DATA steps → DataFrame operations
- Validate statistical output parity (clustering centroids, logistic coefficients, etc.)

**Phase C: Macros & Utilities → Python Modules (Week 2, parallel)**
- Convert 3 SAS macros to Python utility classes
- Build audit/logging framework
- Build validation framework

**Phase D: Orchestration → Python/Airflow (Weeks 6-7)**
- Replace shell orchestrators with Python-based pipeline runner
- Add proper dependency management
- CI/CD integration

**Phase E: Testing & Validation (Weeks 7-8)**
- Full end-to-end regression testing
- Statistical output comparison
- Performance benchmarking

### 4.2 Migration Priority Order

1. **Utilities/Macros** (unblocks everything)
2. **01_stg_customer_360.bteq** → `stg_customer_360.py` (simplest BTEQ, proves the pattern)
3. **02_stg_txn_summary.bteq** → `stg_txn_summary.py` (adds volatile table, NULLIFZERO patterns)
4. **03_stg_risk_factors.bteq** → `stg_risk_factors.py` (most complex BTEQ, work tables, correlated subquery)
5. **01_sas_customer_segments.sas** → `customer_segments.py` (PROC FASTCLUS → MLlib KMeans)
6. **02_sas_txn_analytics.sas** → `txn_analytics.py` (PROC RANK, PROC MEANS → window functions)
7. **03_sas_risk_scoring.sas** → `risk_scoring.py` (PROC LOGISTIC → MLlib LogisticRegression)
8. **04_sas_data_products.sas** → `data_products.py` (4-way merge — depends on 5, 6, 7)
9. **Orchestration** (ties everything together)

---

## 5. Detailed Conversion Guide

### 5.1 BTEQ → PySpark Conversion Patterns

#### 5.1.1 Teradata-Specific SQL Mappings

| Teradata/BTEQ Pattern | PySpark Equivalent | Example |
|-----------------------|-------------------|---------|
| `QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) = 1` | `Window` function + `.filter()` | `w = Window.partitionBy("x").orderBy(desc("y")); df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1)` |
| `NULLIFZERO(x)` | `when(col("x") != 0, col("x"))` | Prevents division by zero |
| `ADD_MONTHS(date, -N)` | `add_months(col("date"), -N)` | Direct PySpark function |
| `MONTHS_BETWEEN(d1, d2)` | `months_between(col("d1"), col("d2"))` | Direct PySpark function |
| `CREATE VOLATILE TABLE ... ON COMMIT PRESERVE ROWS` | Broadcast variable or config dict | Runtime parameters |
| `CAST((CURRENT_DATE - dob) / 365.25 AS SMALLINT)` | `floor(datediff(current_date(), col("dob")) / 365.25).cast("int")` | Age derivation |
| `COLLECT STATISTICS` | N/A (Spark handles stats internally) | No equivalent needed |
| `.SET ERRORLEVEL 3807 SEVERITY 0` | Try/except or `overwrite` mode | Idempotent writes replace DROP/CREATE |
| `STDDEV_POP(x)` | `stddev_pop("x")` | Direct PySpark function |
| `CREATE TABLE ... AS (...) WITH DATA PRIMARY INDEX (x)` | `df.write.partitionBy("x").parquet(...)` | Or Delta Lake table |
| `ETL_RUN_LOG` inserts | Python audit logger | Custom logging class |

#### 5.1.2 BTEQ Script 01: STG_CUSTOMER_360

**Current (BTEQ):**
- Joins CUSTOMERS + ADDRESSES (latest HOME address via QUALIFY ROW_NUMBER) + ACCOUNTS (aggregated)
- Derives AGE, TENURE_MONTHS, CREDIT_UTILIZATION_PCT
- Filters CUSTOMER_STATUS IN ('A', 'I')

**PySpark Translation:**
```python
# 1. Read source tables
customers_df = spark.read.csv("data/01_source_tables/customers.csv", header=True, inferSchema=True)
accounts_df = spark.read.csv("data/01_source_tables/accounts.csv", header=True, inferSchema=True)
addresses_df = spark.read.csv("data/01_source_tables/addresses.csv", header=True, inferSchema=True)

# 2. Primary address (latest HOME, not expired)
addr_window = Window.partitionBy("customer_id").orderBy(desc("effective_date"))
primary_addr = (addresses_df
    .filter((col("address_type") == "HOME") &
            (col("expiration_date").isNull() | (col("expiration_date") > current_date())))
    .withColumn("rn", row_number().over(addr_window))
    .filter(col("rn") == 1)
    .select("customer_id", "address_line_1", "address_line_2", "city", "state_code", "zip_code"))

# 3. Aggregated account metrics
acct_agg = (accounts_df.groupBy("customer_id").agg(
    count("*").alias("num_accounts"),
    sum(when(col("account_status") == "O", 1).otherwise(0)).alias("num_active_accounts"),
    # ... etc
))

# 4. Join and derive
stg_customer_360 = (customers_df
    .filter(col("customer_status").isin("A", "I"))
    .join(primary_addr, "customer_id", "left")
    .join(acct_agg, "customer_id", "left")
    .withColumn("age", floor(datediff(current_date(), col("date_of_birth")) / 365.25).cast("int"))
    .withColumn("tenure_months", months_between(current_date(), col("customer_since")).cast("int"))
    # ... etc
)
```

#### 5.1.3 BTEQ Script 02: STG_TXN_SUMMARY

**Key Conversion Points:**
- Volatile table `VT_RUN_PARAMS` → Python config variables (`period_start`, `period_end`)
- `NULLIFZERO(COUNT(*))` → `when(col("count") != 0, col("count"))`
- Channel mix percentage calculations → same math with PySpark `when`/`sum`
- Top merchant category subquery with `QUALIFY ROW_NUMBER()` → Window function pattern

#### 5.1.4 BTEQ Script 03: STG_RISK_FACTORS

**Key Conversion Points:**
- Work table `WRK_DAILY_BALANCE` → intermediate DataFrame (cached)
- Work table `WRK_PAYMENT_HISTORY` → intermediate DataFrame (cached)
- 7 LEFT JOIN subqueries → individual DataFrames joined sequentially
- Correlated subquery for `NEW_MERCH_30D` → anti-join pattern or window function
- `STDDEV_POP(EOD_BALANCE)` → `stddev_pop("eod_balance")`

### 5.2 SAS → PySpark Conversion Patterns

#### 5.2.1 SAS Program 01: Customer Segments

| SAS Step | PySpark Equivalent |
|----------|-------------------|
| `PROC SQL` extract from STGDB | `spark.read` from staging Parquet/CSV |
| DATA step feature engineering (PRODUCT_BREADTH, TENURE_GROUP, AGE_GROUP, BALANCE_TIER) | `withColumn()` chains with `when()` |
| `PROC STDIZE method=std` | `pyspark.ml.feature.StandardScaler` (with `VectorAssembler`) |
| `PROC FASTCLUS maxclusters=5 maxiter=50` | `pyspark.ml.clustering.KMeans(k=5, maxIter=50)` |
| Cluster profile summary + label assignment | `groupBy("prediction").agg(...)` + ordered label mapping |
| `PROC SQL` merge labels + score computation | `join()` + `withColumn()` |
| `%validate_table` | Custom `validate_table()` function |
| `PROC APPEND` to Teradata | `df.write.mode("overwrite").parquet(...)` |

**Statistical Equivalence Notes:**
- KMeans results may differ slightly due to random initialization. Use `seed` parameter for reproducibility.
- StandardScaler uses mean/stddev — matches SAS `PROC STDIZE method=std`.

#### 5.2.2 SAS Program 02: Transaction Analytics

| SAS Step | PySpark Equivalent |
|----------|-------------------|
| Account-to-customer aggregation | `groupBy("customer_id").agg(...)` |
| `PROC RANK groups=100` | `percent_rank().over(Window.orderBy("total_debit_amt"))` scaled to 0-99 |
| `PROC MEANS` for median + IQR | `approxQuantile("total_debit_amt", [0.25, 0.5, 0.75], 0.01)` |
| IQR anomaly flag: > median + 3*IQR | `when(col("x") > median + 3*iqr, "Y").otherwise("N")` |
| Spend trend classification | `when()` chain based on NET_CASH_FLOW |

#### 5.2.3 SAS Program 03: Risk Scoring

| SAS Step | PySpark Equivalent |
|----------|-------------------|
| Missing value imputation | `fillna()` or `when(col("x") <= 0, 680)` |
| Feature normalization (bureau score) | Arithmetic: `(score - 300) / (850 - 300) * 100` |
| `PROC LOGISTIC descending selection=stepwise` | `pyspark.ml.classification.LogisticRegression` + manual feature selection via `ChiSqSelector` or correlation analysis |
| Composite score calculation | `withColumn()` chain with weights |
| Risk tier classification | `when()` chain: <20=LOW, <40=MODERATE, <60=ELEVATED, <80=HIGH, else CRITICAL |
| Risk driver identification (array scan) | UDF or `greatest()`-based approach |

**Statistical Equivalence Notes:**
- SAS stepwise selection (slentry=0.10, slstay=0.05) has no direct PySpark equivalent. Options:
  - Use all features (simpler, often performs similarly)
  - Use `ChiSqSelector` for feature selection
  - Implement manual forward/backward selection via cross-validation
- Logistic regression coefficients will differ slightly. Compare predicted probabilities, not coefficients.

#### 5.2.4 SAS Program 04: Data Products (Golden Record)

| SAS Step | PySpark Equivalent |
|----------|-------------------|
| 4 `PROC SQL` extracts | 4 DataFrame reads |
| `PROC SORT` by CUSTOMER_ID (×4) | Not needed (PySpark handles join ordering) |
| DATA step MERGE with `IN=` variables | Sequential `join("customer_id", "left")` with `coalesce()` for defaults |
| Default handling (if not `_seg` then ...) | `coalesce(col("segment_name"), lit("UNCLASSIFIED"))` |
| Completeness report | `agg()` with conditional sums |

---

## 6. Data Lineage & Dependencies

### 6.1 Execution Order & Dependencies

```
Source Tables (read-only)
│
├─ CUSTOMERS ─────┐
├─ ACCOUNTS ──────┤
├─ ADDRESSES ─────┘
│   │
│   └──► [01] stg_customer_360.py ──► STG_CUSTOMER_360
│                                          │
├─ TRANSACTIONS ──┐                        │
├─ TRANSACTION_   │                        │
│  TYPES ─────────┤                        │
├─ ACCOUNTS ──────┘                        │
│   │                                      │
│   └──► [02] stg_txn_summary.py ──► STG_TXN_SUMMARY
│                                          │
├─ TRANSACTIONS ──┐                        │
├─ TRANSACTION_   │                        │
│  TYPES ─────────┤                        │
├─ ACCOUNTS ──────┤                        │
├─ CUSTOMERS ─────┤                        │
├─ CUSTOMER_      │                        │
│  BUREAU_SCORES ─┘                        │
│   │                                      │
│   └──► [03] stg_risk_factors.py ──► STG_RISK_FACTORS
│                                          │
│            ┌─────────────────────────────┤
│            │                             │
│            ▼                             ▼
│   [04] customer_segments.py     [05] txn_analytics.py
│         (STG_CUSTOMER_360)        (STG_TXN_SUMMARY)
│              │                          │
│              ▼                          ▼
│   CUSTOMER_SEGMENTS         TRANSACTION_ANALYTICS
│              │                          │
│            ┌─┤                          │
│            │ │   STG_RISK_FACTORS───────┤
│            │ │   STG_CUSTOMER_360───┐   │
│            │ │          │           │   │
│            │ │          ▼           │   │
│            │ │  [06] risk_scoring.py│   │
│            │ │          │           │   │
│            │ │          ▼           │   │
│            │ │  CUSTOMER_RISK_SCORES│   │
│            │ │          │           │   │
│            ▼ ▼          ▼           ▼   ▼
│         [07] data_products.py (4-way join)
│                    │
│                    ▼
│         CUSTOMER_MASTER_PROFILE
```

### 6.2 Critical Path

The critical path determines the minimum execution time:

1. `stg_customer_360.py` (must complete before customer_segments and risk_scoring)
2. `stg_txn_summary.py` (must complete before txn_analytics)
3. `stg_risk_factors.py` (must complete before risk_scoring; depends on source tables only)
4. `customer_segments.py` (depends on STG_CUSTOMER_360)
5. `txn_analytics.py` (depends on STG_TXN_SUMMARY)
6. `risk_scoring.py` (depends on STG_RISK_FACTORS + STG_CUSTOMER_360)
7. `data_products.py` (depends on ALL upstream outputs)

**Parallelization Opportunities:**
- Steps 1, 2, 3 can run in parallel (they only read source tables)
- Steps 4, 5 can run in parallel (after their respective staging tables complete)
- Step 6 must wait for steps 1 and 3
- Step 7 must wait for steps 4, 5, and 6

See the generated data lineage chart (`migration_plan/lineage/data_lineage.png`) for a visual representation.

---

## 7. Validation Strategy

### 7.1 Approach

Each migrated script must produce output that matches the original BTEQ/SAS output. Validation runs compare:

1. **Row counts** — must match exactly
2. **Schema** — column names, types, nullability
3. **Numeric values** — within tolerance (1e-6 for floats, exact for integers)
4. **Categorical values** — exact match
5. **Statistical outputs** — cluster assignments (compare distributions, not 1:1 due to k-means randomness)

### 7.2 Validation Scripts

The `migration_plan/pyspark_validation/` directory contains runnable PySpark scripts that:

1. Load the sample CSV data from `data/01_source_tables/`
2. Execute the PySpark transformation logic
3. Compare output to the reference CSVs in `data/02_bteq_staging/` and `data/03_sas_data_products/`
4. Report pass/fail with detailed discrepancy analysis

| Script | Validates | Reference Data |
|--------|-----------|---------------|
| `validate_stg_customer_360.py` | BTEQ Script 01 | `data/02_bteq_staging/stg_customer_360.csv` |
| `validate_stg_txn_summary.py` | BTEQ Script 02 | `data/02_bteq_staging/stg_txn_summary.csv` |
| `validate_stg_risk_factors.py` | BTEQ Script 03 | `data/02_bteq_staging/stg_risk_factors.csv` |
| `validate_customer_segments.py` | SAS Program 01 | `data/03_sas_data_products/customer_segments.csv` |
| `validate_txn_analytics.py` | SAS Program 02 | `data/03_sas_data_products/transaction_analytics.csv` |
| `validate_risk_scoring.py` | SAS Program 03 | `data/03_sas_data_products/customer_risk_scores.csv` |
| `validate_data_products.py` | SAS Program 04 | `data/03_sas_data_products/customer_master_profile.csv` |
| `run_all_validations.py` | End-to-end | All reference data |

### 7.3 Acceptance Criteria

| Check | Threshold | Notes |
|-------|-----------|-------|
| Row count match | Exact | Must match reference data |
| Column count match | Exact | Same schema |
| Integer columns | Exact match | No tolerance |
| Decimal columns | ±0.01 | Floating point rounding |
| String columns | Exact match (case-insensitive) | |
| Date columns | Exact match | |
| Cluster assignments | Distribution within 5% | k-means is non-deterministic |
| Logistic regression PD | Mean absolute error < 0.05 | Model coefficients may vary |
| Risk tier distribution | Within 5% per tier | Driven by composite score |

---

## 8. Risk Assessment & Mitigations

### 8.1 Technical Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| k-means produces different cluster assignments | Medium | High | Compare distributions not individual assignments; use fixed seed; validate centroids are similar |
| Logistic regression coefficients differ | Medium | High | Compare predicted probabilities and AUC, not coefficients; SAS stepwise vs PySpark all-features may select different variables |
| Teradata-specific SQL functions have no direct Spark equivalent | Low | Medium | All identified functions have PySpark equivalents (see Section 5.1.1) |
| NULLIFZERO behavior difference | Low | Low | Explicit `when()` logic replicates exactly |
| Date arithmetic precision differences | Low | Medium | Test with known dates; document any rounding differences |
| SAS DATA step MERGE semantics differ from PySpark join | Medium | Medium | SAS MERGE with IN= is a sorted merge; PySpark join is hash-based — same results, different implementation |

### 8.2 Process Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| No SAS environment available for side-by-side testing | High | High | Use exported CSV reference data for comparison |
| Teradata not available for BTEQ testing | High | High | Use local PySpark with CSV data; validated against exported staging CSVs |
| Team unfamiliar with PySpark MLlib | Medium | Medium | Training sessions; detailed code comments; this migration guide |

---

## 9. Timeline & Effort Estimates

### 9.1 Effort Breakdown

| Phase | Task | Effort (days) | Dependencies |
|-------|------|--------------|--------------|
| A.1 | Utility modules (logger, validator, config) | 3 | None |
| A.2 | `stg_customer_360.py` + validation | 3 | A.1 |
| A.3 | `stg_txn_summary.py` + validation | 3 | A.1 |
| A.4 | `stg_risk_factors.py` + validation | 5 | A.1 |
| B.1 | `customer_segments.py` + validation | 5 | A.2 |
| B.2 | `txn_analytics.py` + validation | 3 | A.3 |
| B.3 | `risk_scoring.py` + validation | 5 | A.4 |
| B.4 | `data_products.py` + validation | 3 | B.1, B.2, B.3 |
| C.1 | Orchestration script | 3 | B.4 |
| C.2 | CI/CD pipeline setup | 2 | C.1 |
| D.1 | End-to-end regression testing | 5 | C.1 |
| D.2 | Performance benchmarking | 3 | C.1 |
| D.3 | Documentation & handoff | 2 | D.1 |
| **Total** | | **45 days** | |

### 9.2 Recommended Schedule (8 weeks)

| Week | Activities |
|------|-----------|
| 1 | Utility modules + stg_customer_360 |
| 2 | stg_txn_summary + stg_risk_factors |
| 3 | Complete risk_factors + start customer_segments |
| 4 | customer_segments + txn_analytics |
| 5 | risk_scoring |
| 6 | data_products + orchestration |
| 7 | End-to-end testing + performance |
| 8 | Documentation, handoff, buffer |

---

## 10. Appendix: SAS-to-PySpark Pattern Reference

### A. DATA Step → PySpark DataFrame

```sas
/* SAS */
data output;
    set input;
    if AGE < 25 then AGE_GROUP = 'GEN_Z';
    else if AGE < 41 then AGE_GROUP = 'MILLENNIAL';
    PRODUCT_BREADTH = mean((HAS_CHECKING='Y'), (HAS_SAVINGS='Y'));
run;
```

```python
# PySpark
output = input_df.withColumn(
    "age_group",
    when(col("age") < 25, "GEN_Z")
    .when(col("age") < 41, "MILLENNIAL")
    .otherwise("OTHER")
).withColumn(
    "product_breadth",
    (when(col("has_checking") == "Y", 1).otherwise(0) +
     when(col("has_savings") == "Y", 1).otherwise(0)) / 2
)
```

### B. PROC SQL → Spark SQL

```sas
/* SAS */
proc sql;
    create table output as
    select customer_id, avg(amount) as avg_amt
    from input group by customer_id;
quit;
```

```python
# PySpark
output = input_df.groupBy("customer_id").agg(avg("amount").alias("avg_amt"))
```

### C. PROC FASTCLUS → MLlib KMeans

```sas
/* SAS */
proc stdize data=input out=standardised method=std;
    var x1 x2 x3;
run;
proc fastclus data=standardised out=clustered maxclusters=5 maxiter=50;
    var x1 x2 x3;
run;
```

```python
# PySpark
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

assembler = VectorAssembler(inputCols=["x1", "x2", "x3"], outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
kmeans = KMeans(k=5, maxIter=50, featuresCol="features", predictionCol="cluster", seed=42)

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model = pipeline.fit(input_df)
clustered = model.transform(input_df)
```

### D. PROC LOGISTIC → MLlib LogisticRegression

```sas
/* SAS */
proc logistic data=input descending;
    model target = x1 x2 x3 / selection=stepwise;
    output out=scored predicted=prob;
run;
```

```python
# PySpark
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=["x1", "x2", "x3"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="target",
                        maxIter=100, regParam=0.01)

pipeline = Pipeline(stages=[assembler, lr])
model = pipeline.fit(input_df)
scored = model.transform(input_df)
# Extract probability of class 1
scored = scored.withColumn("prob", col("probability")[1])
```

### E. PROC RANK → Window Function

```sas
/* SAS */
proc rank data=input out=ranked groups=100;
    var spend;
    ranks spend_percentile;
run;
```

```python
# PySpark
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank, floor

w = Window.orderBy("spend")
ranked = input_df.withColumn(
    "spend_percentile",
    floor(percent_rank().over(w) * 100).cast("int")
)
```

### F. DATA Step MERGE → PySpark Join

```sas
/* SAS */
data master;
    merge base(in=_b) seg(in=_s) txn(in=_t);
    by customer_id;
    if _b;
    if not _s then segment_name = 'UNCLASSIFIED';
run;
```

```python
# PySpark
master = (base_df
    .join(seg_df, "customer_id", "left")
    .join(txn_df, "customer_id", "left")
    .withColumn("segment_name",
        coalesce(col("segment_name"), lit("UNCLASSIFIED")))
)
```
