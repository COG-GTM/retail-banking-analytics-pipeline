# Migration: Teradata BTEQ + SAS 9.4 → Databricks

This document tracks the modernization of the retail-banking analytics pipeline
off **Teradata BTEQ** and **SAS 9.4** onto **Databricks** (Delta Lake, PySpark,
Spark MLlib, Unity Catalog, Databricks Workflows / Asset Bundles).

**Guiding principle:** business logic is preserved *exactly*; only the execution
technology changes. Where the legacy code and the bundled demo reference CSVs
disagree, the port stays faithful to the **BTEQ / SAS source** (see per-ticket
notes).

Everything new lives under [`databricks/`](databricks/); the legacy Teradata/SAS
assets are left in place for reference. `export_data.py` (a DuckDB/scikit-learn
local data generator) is intentionally **not** migrated.

## Legacy → Databricks mapping

| Legacy Teradata DB | Unity Catalog schema (in catalog `retail_banking_analytics`) |
|--------------------|-------------------------------------------------------------|
| `CORE_BANKING_DB`   | `core_banking`   |
| `TXN_PROCESSING_DB` | `txn_processing` |
| `ETL_STAGING_DB`    | `etl_staging`    |
| `DATA_PRODUCTS_DB`  | `data_products`  |

| Legacy technology | Databricks replacement |
|-------------------|------------------------|
| Teradata DDL (`MULTISET`, `PRIMARY INDEX`, `COLLECT STATISTICS`, `CHARACTER SET`) | Delta `CREATE TABLE` |
| BTEQ `QUALIFY ROW_NUMBER()` | PySpark `Window` + `row_number()` |
| Teradata work tables (`WRK_*`) | cached DataFrames |
| SAS `PROC STDIZE` + `PROC FASTCLUS` | Spark MLlib `StandardScaler` + `KMeans` |
| SAS `PROC RANK` / `PROC MEANS` | `percent_rank()` / `approxQuantile` |
| SAS `PROC LOGISTIC` | Spark MLlib `LogisticRegression` |
| SAS 4-way data-step `MERGE` | PySpark left joins + `coalesce` defaults |
| `pipeline_config.cfg` + `{SAS004}` creds | widgets / job params + Databricks Secrets |
| `%log_step` / `%init_audit` | Delta audit table `etl_staging.etl_run_log` |
| `%validate_table` | `common.validation.validate_dataframe` (raises) |
| `run_full_pipeline.sh` / `run_bteq_pipeline.sh` / `run_sas_pipeline.sh` | Databricks Asset Bundle job |

---

## Ticket 1 — Unity Catalog foundation + Delta DDL

- **Objective:** replace the four Teradata databases with Unity Catalog schemas and
  convert all Teradata DDL to Delta.
- **Inputs:** `ddl/00_source_tables.sql`, `ddl/01_staging_tables.sql`,
  `ddl/02_data_product_tables.sql`.
- **Outputs:** [`databricks/ddl/00_unity_catalog_setup.sql`](databricks/ddl/00_unity_catalog_setup.sql),
  [`01_source_tables.sql`](databricks/ddl/01_source_tables.sql),
  [`02_staging_tables.sql`](databricks/ddl/02_staging_tables.sql),
  [`03_data_product_tables.sql`](databricks/ddl/03_data_product_tables.sql).
- **Implementation:** `CREATE CATALOG` + `CREATE SCHEMA` for the four schemas;
  every table is `CREATE TABLE ... USING DELTA`. Removed all Teradata-only syntax
  (`MULTISET`, `PRIMARY INDEX`, `COLLECT STATISTICS`, `SET`, `CHARACTER SET`,
  `CASESPECIFIC`) while keeping `DECIMAL`/`TIMESTAMP` types verbatim.
  `transaction_analytics` is `PARTITIONED BY (reporting_period)`. The source
  DDL also adds `core_banking.customer_bureau_scores`, which the risk BTEQ reads
  but the legacy source DDL omitted. Placeholders (`${catalog}`, `${schema}`) are
  rendered by `common/ddl.py`.
- **Validation:** `test_ddl.py` — placeholder substitution, comment-safe statement
  splitting, no executable Teradata-only syntax, `reporting_period` partitioning,
  and successful schema/table creation on local Spark+Delta.
- **Status:** ✅ Done

## Ticket 2 — Config + secrets

- **Objective:** remove `pipeline_config.cfg` and hard-coded `{SAS004}` passwords.
- **Inputs:** `config/pipeline_config.cfg`.
- **Outputs:** [`databricks/common/config.py`](databricks/common/config.py).
- **Implementation:** `load_config()` reads runtime params (`catalog`, schemas,
  `lookback_months`, `risk_score_threshold`, `secret_scope`, `run_date`) from
  Databricks **widgets / job parameters** via `dbutils`, falling back to
  environment variables and the legacy defaults so the code is unit-testable
  off-cluster. `PipelineConfig.secret()` resolves sensitive values from
  **Databricks Secrets** (`dbutils.secrets.get`), falling back to
  `RBA_SECRET_<KEY>` env vars for local dev. No credentials are stored in source
  or logged.
- **Validation:** `test_config.py` — legacy DB→schema mapping, three-level table
  helpers, defaults/overrides, widget & secret env fallbacks, missing-secret
  `KeyError`, and a source scan asserting no `password="{SAS004}"` literals
  survive under `databricks/`.
- **Status:** ✅ Done

## Ticket 3 — Shared logging + validation utilities

- **Objective:** reimplement `log_step`/`init_audit` (Delta audit log) and
  `validate_table` (checks that raise) as a PySpark utility module.
- **Inputs:** `sas/macros/log_step.sas`, `sas/macros/validate_table.sas`.
- **Outputs:** [`databricks/common/audit.py`](databricks/common/audit.py),
  [`databricks/common/validation.py`](databricks/common/validation.py).
- **Implementation:** `init_audit()` creates the Delta table
  `etl_staging.etl_run_log` (`job_name`, `step_name`, `status`, `message`,
  `row_count`, `start_ts`, `end_ts`, `log_ts`); `log_step()` appends a row per
  step. `validate_dataframe()` enforces minimum row count, required non-null
  columns, and key uniqueness, raising `DataValidationError` on failure.
- **Validation:** `test_validation.py` — pass case + row-count, duplicate-key, and
  null failures raising; `test_pipeline_e2e.py` asserts `SUCCESS` audit rows.
- **Status:** ✅ Done

## Ticket 4 — `stg_customer_360` (BTEQ → PySpark)

- **Objective:** port `bteq/01_stg_customer_360.bteq`.
- **Inputs:** `core_banking.customers`, `core_banking.accounts`, `core_banking.addresses`.
- **Outputs:** `etl_staging.stg_customer_360` (Delta) —
  [`jobs/stg_customer_360.py`](databricks/jobs/stg_customer_360.py).
- **Implementation:** `QUALIFY ROW_NUMBER()` for the latest non-expired `HOME`
  address becomes a `Window` (`effective_date desc, address_id desc`). Age uses
  the BTEQ truncation `CAST(datediff/365.25 AS SMALLINT)`; tenure uses integer
  `months_between`; account-portfolio flags and active-account counts are
  aggregated; credit utilization is guarded against zero limits.
- **Validation:** `test_stg_customer_360.py` — closed-customer exclusion,
  active/inactive retention, age truncation, tenure, portfolio flags, credit-util
  zero-limit guard, latest-HOME-address selection.
- **Note:** the demo reference rounds age; the port keeps BTEQ truncation.
- **Status:** ✅ Done

## Ticket 5 — `stg_txn_summary` (BTEQ → PySpark)

- **Objective:** port `bteq/02_stg_txn_summary.bteq`.
- **Inputs:** `txn_processing.transactions`, `txn_processing.transaction_types`,
  `core_banking.accounts`.
- **Outputs:** `etl_staging.stg_txn_summary` (Delta) —
  [`jobs/stg_txn_summary.py`](databricks/jobs/stg_txn_summary.py).
- **Implementation:** posted-only (`status_code = 'P'`) filtering, configurable
  `lookback_months` window, account-level aggregates, deterministic top merchant
  category (spend desc, category asc), channel-mix percentages, days-since-last.
- **Validation:** `test_stg_txn_summary.py` (posted/lookback filtering, top
  category, channel %, counts) **plus** `test_pipeline_e2e.py`, which asserts this
  table matches the legacy reference CSV cell-for-cell (row count + per-row
  `txn_count_total` and `amt_total_debit`).
- **Status:** ✅ Done

## Ticket 6 — `stg_risk_factors` (BTEQ → PySpark)

- **Objective:** port `bteq/03_stg_risk_factors.bteq`; replace the
  `WRK_DAILY_BALANCE` / `WRK_PAYMENT_HISTORY` work tables with cached DataFrames.
- **Inputs:** `core_banking.customers`, `core_banking.accounts`,
  `core_banking.customer_bureau_scores`, `txn_processing.transactions`,
  `txn_processing.transaction_types`.
- **Outputs:** `etl_staging.stg_risk_factors` (Delta) —
  [`jobs/stg_risk_factors.py`](databricks/jobs/stg_risk_factors.py).
- **Implementation:** `build_wrk_daily_balance` (last posted txn per account/day,
  3-month lookback) and `build_wrk_payment_history` (CREDIT/LOAN payment behaviour,
  24-month) are `.cache()`d DataFrames. Preserves overdraft/NSF, large withdrawals,
  30/90-day average balances + volatility, credit utilization, latest bureau score,
  debit velocity (7/30-day), and merchant risk indicators. `months_since_last_late`
  falls back to account age, then to the `999` sentinel when there is no payment
  history at all.
- **Validation:** `test_stg_risk_factors.py` — overdraft/large-withdrawal/NSF/intl/
  high-risk-merchant/new-merchant counts, latest-bureau selection, and both
  `months_since_last_late` fallbacks.
- **Status:** ✅ Done

## Ticket 7 — `customer_segments` (SAS → Spark MLlib)

- **Objective:** port `sas/01_sas_customer_segments.sas`; replace `PROC STDIZE` +
  `PROC FASTCLUS` with `StandardScaler` + `KMeans(k=5)`.
- **Inputs:** `etl_staging.stg_customer_360`.
- **Outputs:** `data_products.customer_segments` (Delta) —
  [`jobs/customer_segments.py`](databricks/jobs/customer_segments.py).
- **Implementation:** feature engineering (`product_breadth`, `tenure_group`,
  `age_group`, `balance_tier`, `log_balance`, `acct_ratio`,
  `digital_adoption_score = 0`) matches SAS. Clustering uses
  `VectorAssembler` → `StandardScaler(withMean, withStd)` → `KMeans(k=5, seed=42)`,
  preserving SAS's mixed use of standardized and raw engineered features. Segment
  labels, LTV/engagement scores and cross-sell/upsell/retention flags preserved;
  `model_version = SEG_V3.2`.
- **Validation:** `test_customer_segments.py` — feature engineering, balance-tier
  boundaries, and a `KMeans` run yielding exactly 5 valid segment labels/ids;
  e2e domain check that segment names ⊆ the known label set.
- **Note:** exact cluster assignment differs from the demo CSV because MLlib KMeans
  ≠ SAS FASTCLUS; logic and configuration are faithful.
- **Status:** ✅ Done

## Ticket 8 — `transaction_analytics` (SAS → PySpark)

- **Objective:** port `sas/02_sas_txn_analytics.sas`; replace `PROC RANK` /
  `PROC MEANS` with `percent_rank` / `approxQuantile` IQR anomaly detection.
- **Inputs:** `etl_staging.stg_txn_summary`.
- **Outputs:** `data_products.transaction_analytics` (Delta, partitioned by
  `reporting_period`) — [`jobs/transaction_analytics.py`](databricks/jobs/transaction_analytics.py).
- **Implementation:** spend-trend classification (`UP`/`DOWN`/`STABLE` vs
  `AVG_TRANSACTION_SIZE * 5`), revenue contribution
  (`fee_income + total_debit * 0.02`), `PROC RANK groups=100` → `percent_rank()*100`,
  and `PROC MEANS` median/IQR → `approxQuantile`; anomaly only when `IQR > 0` and
  debit spend > median + `3*IQR`. Top spend category preserves SAS
  `MAX(TOP_MERCHANT_CATEGORY)` (alphabetical). `model_version = TXN_V2.1`,
  `reporting_period = yyyy-MM`.
- **Validation:** `test_transaction_analytics.py` — trend + revenue, UP/DOWN/STABLE,
  alphabetical top category across accounts, IQR anomaly flag, reporting period.
- **Status:** ✅ Done

## Ticket 9 — `customer_risk_scores` (SAS → Spark MLlib)

- **Objective:** port `sas/03_sas_risk_scoring.sas`; replace `PROC LOGISTIC` with
  `LogisticRegression`, keep weighted composite scoring + risk tiers.
- **Inputs:** `etl_staging.stg_risk_factors`, `etl_staging.stg_customer_360`.
- **Outputs:** `data_products.customer_risk_scores` (Delta) —
  [`jobs/risk_scoring.py`](databricks/jobs/risk_scoring.py).
- **Implementation:** bureau imputation (≤0/null → 680), normalized bureau score,
  balance-trend and velocity ratios (default 1 when denominators ≤ 0),
  `LogisticRegression` for probability of default (base-rate fallback with a single
  label), five clipped risk components, the weighted composite
  (0.30/0.25/0.15/0.20/0.10), risk tiers (`LOW`…`CRITICAL`), watch-list and
  review-required flags. The top-two risk drivers are computed with **native
  column expressions** (no Python UDF, so executors need no local package import),
  preserving SAS's strict-`>` tie-break where `CREDIT_UTILIZATION` outranks the tied
  `BUREAU_SCORE`. `model_version = RISK_V4.0`.
- **Validation:** `test_risk_scoring.py` — bureau/ratio imputation, default proxy,
  composite/tier, driver tie-break, velocity-dominant driver, review-required flag,
  single-label fallback, and a two-label `LogisticRegression` fit.
- **Note:** probabilities differ from the demo CSV (MLlib ≠ PROC LOGISTIC); scoring
  logic is faithful.
- **Status:** ✅ Done

## Ticket 10 — Data products + orchestration

- **Objective:** port the `sas/04_sas_data_products.sas` 4-way `MERGE` to PySpark
  joins, and replace the three shell orchestrators with a Databricks Workflow.
- **Inputs:** `stg_customer_360`, `customer_segments`, `transaction_analytics`,
  `customer_risk_scores`.
- **Outputs:** `data_products.customer_master_profile` (Delta) —
  [`jobs/master_profile.py`](databricks/jobs/master_profile.py); the workflow in
  [`databricks/databricks.yml`](databricks/databricks.yml) +
  [`resources/retail_banking_pipeline.job.yml`](databricks/resources/retail_banking_pipeline.job.yml).
- **Implementation:** the SAS `MERGE ... BY customer_id` becomes left joins from the
  active-customer base with `coalesce` defaults (`UNCLASSIFIED`, `UNKNOWN`, `N`,
  zeros, empty category); only the current `reporting_period` transaction row is
  joined. `model_version = MASTER_V1.5`. The Asset Bundle job wires tickets 4→10 in
  dependency order (setup → 3 staging jobs → 3 analytics jobs → master profile),
  parameterized by `catalog`/`min_rows`/`run_date`, all using the shared
  logging/validation. `orchestration/pipeline.py` runs the same steps locally;
  `orchestration/run_local.py` + `load_sample_data.py` execute the whole pipeline on
  the bundled fixtures against local Spark+Delta. Notebook wrappers
  (`notebooks/04..10`) delegate to the tested job functions.
- **Validation:** `test_master_profile.py` (missing-upstream defaults, populated
  joins, current-period filter) and `test_pipeline_e2e.py` (end-to-end run: exact
  reference row counts, schemas, key uniqueness/no-null keys, audit success).
- **Status:** ✅ Done

---

## Verification summary

- **Tests:** `51 passed`, **95% line coverage** (`databricks/tests/`), run with
  local Spark 3.5.3 + Delta 3.2.1 on JDK 17 / Python 3.12.
- **End-to-end:** the full ported pipeline runs on the bundled sample data and
  reproduces the legacy reference **row counts exactly**
  (`stg_customer_360` 478, `stg_txn_summary` 1251, `stg_risk_factors` 478,
  `customer_segments` 407, `transaction_analytics` 500, `customer_risk_scores` 407,
  `customer_master_profile` 407). `stg_txn_summary` also matches the reference
  cell-for-cell.
- **Reference divergences** (age rounding, KMeans vs FASTCLUS assignments, MLlib vs
  PROC LOGISTIC probabilities, driver tie-break) are all traced to demo-generator /
  algorithm differences; the ports remain faithful to the BTEQ/SAS source.

See [`databricks/README.md`](databricks/README.md) for how to run it locally and on
Databricks.
