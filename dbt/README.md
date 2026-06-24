# dbt Project: Retail Banking Analytics

A dbt re-implementation of the existing **BTEQ (Phase 1)** and **SAS (Phase 2)**
pipeline as a single, unified, testable dbt project targeting **Teradata**
(`dbt-teradata`). It is added **alongside** the existing `bteq/` and `sas/`
directories so outputs can be validated against the current pipeline; nothing in
those directories is removed.

## What maps to what

| Legacy artifact | dbt object | Materialization | Target DB |
|---|---|---|---|
| `bteq/01_stg_customer_360.bteq` | `models/staging/stg_customer_360.sql` | view | `ETL_STAGING_DB` |
| `bteq/02_stg_txn_summary.bteq` | `models/staging/stg_txn_summary.sql` | view | `ETL_STAGING_DB` |
| `bteq/03_stg_risk_factors.bteq` | `models/staging/stg_risk_factors.sql` | view | `ETL_STAGING_DB` |
| BTEQ `WRK_DAILY_BALANCE` work table | `models/intermediate/int_wrk_daily_balance.sql` | ephemeral | (inlined) |
| BTEQ `WRK_PAYMENT_HISTORY` work table | `models/intermediate/int_wrk_payment_history.sql` | ephemeral | (inlined) |
| `sas/02_sas_txn_analytics.sas` | `models/marts/transaction_analytics.sql` | table | `DATA_PRODUCTS_DB` |
| `sas/04_sas_data_products.sas` | `models/marts/customer_master_profile.sql` | table | `DATA_PRODUCTS_DB` |
| `sas/01` SQL feature engineering (STEP 1-2) | `models/intermediate/int_customer_segment_features.sql` | view | `ETL_STAGING_DB` |
| `sas/01` k-means output `CUSTOMER_SEGMENTS` | `seeds/customer_segments.csv` | seed | `DATA_PRODUCTS_DB` |
| `sas/03` logistic-regression output `CUSTOMER_RISK_SCORES` | `seeds/customer_risk_scores.csv` | seed | `DATA_PRODUCTS_DB` |

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

## Dialect notes (Teradata)

The project targets `dbt-teradata` to minimize rewrites against the existing
Teradata SQL. BTEQ-only constructs were removed or translated:

- `QUALIFY` — **kept** (Teradata supports it natively).
- `MULTISET` / `PRIMARY INDEX` / `COLLECT STATISTICS` / `WITH DATA` — **dropped**
  (handled by dbt materialization, not the SELECT).
- `.SET` / `.IF ERRORCODE` / `.LABEL` / `.GOTO` / `.LOGON` / volatile tables /
  `ETL_RUN_LOG` audit inserts — **dropped** (orchestration handled by dbt).
- `NULLIFZERO(x)` → `nullif(x, 0)`.
- `ADD_MONTHS` / `MONTHS_BETWEEN` — **kept** as native Teradata date functions
  (they are valid Teradata SQL, not BTEQ-only). The Teradata cast shorthand
  `expr (INTEGER)` was rewritten to `cast(expr as integer)`. For a cross-warehouse
  port, swap these for dbt's cross-database `{{ dbt.dateadd(...) }}` /
  `{{ dbt.datediff(...) }}` macros (the `dbt-teradata` implementations compile
  back to `ADD_MONTHS` / interval arithmetic).

## Configuration

Database names and the lookback window mirror `config/pipeline_config.cfg`
(`DB_CORE`, `DB_TXN`, `DB_STG`, `DB_DP`, `LOOKBACK_MONTHS`) and live in the
`vars:` / `models:` blocks of `dbt_project.yml`. A `generate_schema_name` macro
override makes models land in the exact Teradata databases (no `target_custom`
prefix).

Connection settings come from `profiles.yml` via environment variables
(`TD_SERVER`, `TD_USERNAME`, `TD_PASSWORD`, `TD_LOGMECH`, `TD_DATABASE`) — the
same variables exported by `config/pipeline_config.cfg`. No secrets are stored
in the repo; set `TD_PASSWORD` in your environment before running.

## Usage

```bash
# from the repo root
python -m venv .venv && source .venv/bin/activate
pip install dbt-teradata

# load connection env vars (and set TD_PASSWORD)
source config/pipeline_config.cfg
export TD_PASSWORD='********'

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
