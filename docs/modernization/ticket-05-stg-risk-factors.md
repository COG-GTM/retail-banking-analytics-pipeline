# TICKET-05 (MBA-2206) — `03_stg_risk_factors.bteq` → Snowflake SQL / dbt

## What was migrated

| Teradata asset | Snowflake / dbt replacement |
|---|---|
| `ETL_STAGING_DB.WRK_DAILY_BALANCE` work table + `DROP` cleanup | `dbt/models/intermediate/int_daily_balance.sql` (ephemeral → inlined CTE) |
| `ETL_STAGING_DB.WRK_PAYMENT_HISTORY` work table + `DROP` cleanup | `dbt/models/intermediate/int_payment_history.sql` (ephemeral → inlined CTE) |
| `CREATE MULTISET TABLE ... STG_RISK_FACTORS AS (...)` | `dbt/models/staging/stg_risk_factors.sql` (materialized `table`) |
| `ddl/01_staging_tables.sql` STG_RISK_FACTORS DDL | `ddl/snowflake/01_stg_risk_factors.sql` |
| `.SET ERRORLEVEL` / `.IF ERRORCODE <> 0 THEN .EXIT` | dbt run failure semantics (a failing statement aborts the invocation) |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | `assert_not_empty_and_log()` post-hook + singular test `assert_stg_risk_factors_grain` |
| `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG` | `log_etl_run()` macro (`dbt/macros/etl_run_log.sql`) |
| `COLLECT STATISTICS` | dropped (Snowflake maintains statistics automatically) |
| `PRIMARY INDEX (CUSTOMER_ID)` | dropped (no Snowflake equivalent; clustering not justified at this volume) |

## Expression mapping

| Teradata | Snowflake |
|---|---|
| `ADD_MONTHS(d, -n)` | `DATEADD(month, -n, d)` |
| `CURRENT_DATE - n` | `DATEADD(day, -n, CURRENT_DATE)` |
| `CURRENT_TIMESTAMP(6)` | `CURRENT_TIMESTAMP()::TIMESTAMP_NTZ` |
| `MONTHS_BETWEEN(a, b) (INTEGER)` | `TRUNC(MONTHS_BETWEEN(a, b))::INT` — Teradata's cast truncates toward zero while Snowflake's `CAST` rounds, so the truncation is explicit |
| `DECIMAL(p,s)` | `NUMBER(p,s)` |
| `QUALIFY ROW_NUMBER() OVER (...)`, `STDDEV_POP` | supported natively, identical semantics |
| correlated `NOT IN (SELECT ...)` inside `COUNT(DISTINCT CASE ...)` | anti-join against a `prior_merchants` CTE (Snowflake does not allow a correlated sub-query in that position) |

The anti-join is semantically identical to the Teradata predicate: the original
sub-query filtered out `MERCHANT_NAME IS NULL`, so no `NOT IN`/NULL three-valued
logic applied, and `COUNT(DISTINCT ...)` ignores NULL merchant names on both sides.

## Window / boundary semantics

* Velocity and balance windows are inclusive lower bounds relative to
  `CURRENT_DATE` in both dialects (`>= CURRENT_DATE - 7` ≡
  `>= DATEADD(day, -7, CURRENT_DATE)`), so partition boundaries are unchanged.
* `ROW_NUMBER()` deduplication of the daily balance orders by `TRANSACTION_TS DESC`;
  ties are resolved non-deterministically in both engines. Where a single account
  has two transactions with an identical timestamp on the same day, the chosen
  `EOD_BALANCE` may differ; this is a pre-existing property of the Teradata job.
* Customers with sparse or no activity keep a row in the output: all feature
  joins are `LEFT JOIN`s and every measure is wrapped in `COALESCE`, so the
  defaults (`0`, `100.00` on-time pct, `999` months since last late) are
  preserved exactly as in BTEQ.
* `AVG(CASE WHEN ... THEN eod_balance END)` ignores NULLs identically in both
  engines, so a customer with balances only outside the 30-day window still gets
  the BTEQ default of `0.00` after `COALESCE`.

## Floating point / rounding tolerance

Teradata `CAST(<decimal> AS DECIMAL(p,s))` rounds half up, Snowflake rounds half
away from zero, and Snowflake's division produces a wider intermediate scale.
The agreed reconciliation tolerance is therefore:

* exact for counts and the bureau score,
* 0.01 absolute for currency amounts (`NUMBER(_,2)`),
* 0.0001 absolute for `CREDIT_UTIL_RATIO` and `BALANCE_VOLATILITY`.

`dbt/analyses/reconcile_stg_risk_factors.sql` implements this comparison against
a landed copy of the Teradata output (`STG_RISK_FACTORS_TD_BASELINE`) and returns
zero rows when the port reconciles.

## Assumptions about unmerged predecessor tickets

* **TICKET-01 (Snowflake DDL):** sources are declared in
  `dbt/models/staging/_sources.yml` assuming `CORE_BANKING_DB` → schema
  `CORE_BANKING`, `TXN_PROCESSING_DB` → `TXN_PROCESSING`, `ETL_STAGING_DB` →
  `ETL_STAGING`, all inside one environment-suffixed database
  (`SF_DATABASE_ANALYTICS`). Column names and types are unchanged from the
  Teradata DDL.
* **TICKET-02 (environment/roles/secrets):** `dbt/profiles.yml` only reads
  `SF_*` environment variables; no credentials are stored in the repo.
* **TICKET-03 (shared run-log mechanism):** `log_etl_run()` implements the same
  contract (job, step, status, row count, start/end timestamps) and can be
  swapped for the shared macro without touching the models.

## Running

```bash
pip install -r dbt/requirements.txt
cd dbt && dbt deps
dbt run  --profiles-dir . --select +stg_risk_factors
dbt test --profiles-dir . --select stg_risk_factors assert_stg_risk_factors_grain
```
