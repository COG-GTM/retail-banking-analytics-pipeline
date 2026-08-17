# dbt Project: Retail Banking Analytics

A dbt re-implementation of the existing **BTEQ (Phase 1)** and **SAS (Phase 2)**
pipeline as a single, unified, testable dbt project targeting **Databricks**
(`dbt-databricks`, Unity Catalog + Delta). It is added **alongside** the
existing `bteq/` and `sas/` directories so outputs can be validated against the
current pipeline; nothing in those directories is removed.

Unity Catalog layout (medallion, aligned with the PySpark migration under
`databricks/`): catalog `retail_banking`, schemas `bronze` (ingested legacy
source tables), `silver` (staging / intermediate) and `gold` (data products).

## What maps to what

| Legacy artifact | dbt object | Materialization | Target schema |
|---|---|---|---|
| `bteq/01_stg_customer_360.bteq` | `models/staging/stg_customer_360.sql` | view | `silver` |
| `bteq/02_stg_txn_summary.bteq` | `models/staging/stg_txn_summary.sql` | view | `silver` |
| `bteq/03_stg_risk_factors.bteq` | `models/staging/stg_risk_factors.sql` | view | `silver` |
| BTEQ `WRK_DAILY_BALANCE` work table | `models/intermediate/int_wrk_daily_balance.sql` | ephemeral | (inlined) |
| BTEQ `WRK_PAYMENT_HISTORY` work table | `models/intermediate/int_wrk_payment_history.sql` | ephemeral | (inlined) |
| `sas/02_sas_txn_analytics.sas` | `models/marts/transaction_analytics.sql` | table (Delta) | `gold` |
| `sas/04_sas_data_products.sas` | `models/marts/customer_master_profile.sql` | table (Delta) | `gold` |
| `sas/01` SQL feature engineering (STEP 1-2) | `models/intermediate/int_customer_segment_features.sql` | view | `silver` |
| `sas/01` k-means output `CUSTOMER_SEGMENTS` | `seeds/customer_segments.csv` | seed | `gold` |
| `sas/03` logistic-regression output `CUSTOMER_RISK_SCORES` | `seeds/customer_risk_scores.csv` | seed | `gold` |

Sources for the six upstream operational tables are declared in
`models/sources.yml` with `not_null` / `unique` key tests and `accepted_values`
tests on status/category enum columns.

## Lineage

```
sources (core_banking / txn_processing)
        │
        ├─▶ stg_customer_360 ─┬─▶ int_customer_segment_features ─▶ [EXTERNAL k-means] ─▶ seed: customer_segments ─┐
        │                     │                                                                                    │
        │                     └────────────────────────────────────────────────────────────────────────────────┤
        ├─▶ stg_txn_summary ─────▶ transaction_analytics ───────────────────────────────────────────────────────┤
        │                                                                                                          ├─▶ customer_master_profile
        └─▶ stg_risk_factors ◀── int_wrk_daily_balance (ephemeral)                                                 │
                              ◀── int_wrk_payment_history (ephemeral)                                              │
                  │                                                                                                │
                  └──────────▶ [EXTERNAL logistic regression] ─▶ seed: customer_risk_scores ──────────────────────┘
```

## The two ML steps remain outside dbt

dbt SQL is not used to reproduce statistical model math. Two steps stay external:

1. **Customer segmentation** — `sas/01_sas_customer_segments.sas` uses
   `PROC STDIZE` + `PROC FASTCLUS` (k-means, k=5). dbt implements only the
   SQL-expressible feature engineering as `int_customer_segment_features`
   (product breadth, tenure/age/balance tiers, log balance, account ratio). An
   external ML/Python step (or a dbt-python model on a warehouse that supports
   it) consumes that view, runs standardization + clustering, and produces the
   `customer_segments` output that is modelled here as a seed.
2. **Risk scoring** — `sas/03_sas_risk_scoring.sas` uses `PROC LOGISTIC`
   (probability of default). The `stg_risk_factors` feature table is built in
   dbt, but the logistic-regression scoring stays external and its output is
   modelled here as the `customer_risk_scores` seed.

> Note: `transaction_analytics` percentile ranking (SAS `PROC RANK`) and IQR
> anomaly detection (SAS `PROC MEANS`) **are** expressed in SQL (`ntile` and
> `percentile_cont`), since they do not require model fitting.

To swap a seed for a live external output, replace the `ref('customer_segments')`
/ `ref('customer_risk_scores')` calls in `customer_master_profile.sql` with a
`source()` pointing at the table the external job writes.

## Dialect notes (Databricks SQL)

The project targets `dbt-databricks`. BTEQ-only constructs were removed or
translated, and remaining Teradata-isms rewritten to Databricks SQL:

- `QUALIFY` — **kept** (Databricks SQL supports it natively). The one nested
  Teradata `QUALIFY` (window over a window in `stg_txn_summary` `top_cat`) was
  rewritten as an aggregate + `row_number()` subquery.
- `MULTISET` / `PRIMARY INDEX` / `COLLECT STATISTICS` / `WITH DATA` — **dropped**
  (handled by dbt materialization; tables are Delta by default).
- `.SET` / `.IF ERRORCODE` / `.LABEL` / `.GOTO` / `.LOGON` / volatile tables /
  `ETL_RUN_LOG` audit inserts — **dropped** (orchestration handled by dbt).
- `NULLIFZERO(x)` → `nullif(x, 0)`.
- `ADD_MONTHS` / `MONTHS_BETWEEN` — **kept** (same semantics on Databricks).
- Teradata date arithmetic `date - N` → `date_sub(date, N)`; `date1 - date2`
  → `datediff(date1, date2)`.
- `CURRENT_TIMESTAMP(6)` → `current_timestamp()`.
- Correlated `NOT IN` subquery inside an aggregate (new-merchant detection in
  `stg_risk_factors`) → left join against a `prior_merchants` CTE (Databricks
  does not support correlated subqueries in aggregate expressions).
- Hand-rolled `YYYY-MM` string assembly → `date_format(current_date, 'yyyy-MM')`.
- `cast(... as varchar(n))` metadata columns → `cast(... as string)`.
- `percentile_cont(...) within group (...)` and `ntile(100)` — **kept**
  (supported on Databricks SQL / DBR 11+).

## Configuration

Unity Catalog schema names and the lookback window live in the `vars:` /
`models:` blocks of `dbt_project.yml` (`src_schema`=`bronze`,
`stg_schema`=`silver`, `dp_schema`=`gold`, `lookback_months`), mirroring the
medallion layout used by the Databricks PySpark migration (`databricks/`).
A `generate_schema_name` macro override makes models land in the exact
schemas (no `target_custom` prefix).

Connection settings come from `profiles.yml` via environment variables
(`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`,
`DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA`). No secrets are stored in the repo;
set `DATABRICKS_TOKEN` in your environment before running.

## Usage

```bash
# from the repo root
python -m venv .venv && source .venv/bin/activate
pip install dbt-databricks

# connection env vars (never commit the token)
export DATABRICKS_HOST='adb-<workspace-id>.<n>.azuredatabricks.net'
export DATABRICKS_HTTP_PATH='/sql/1.0/warehouses/<warehouse-id>'
export DATABRICKS_TOKEN='********'

# validate the project (no warehouse needed)
dbt parse   --project-dir dbt --profiles-dir dbt

# load the external ML-output seeds
dbt seed    --project-dir dbt --profiles-dir dbt

# build staging + marts and run tests
dbt build   --project-dir dbt --profiles-dir dbt

# generate + serve docs / lineage
dbt docs generate --project-dir dbt --profiles-dir dbt
```

## Validating against the legacy pipeline

The CSVs under `data/` are the current pipeline's outputs and can be used as
expected-output fixtures: build the dbt marts, then compare row counts and key
columns against `data/02_bteq_staging/*.csv` (staging) and
`data/03_sas_data_products/*.csv` (data products).
```
