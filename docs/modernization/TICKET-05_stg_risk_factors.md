# TICKET-05 (MBA-2206): `03_stg_risk_factors.bteq` -> Snowflake SQL / dbt

Migration of the risk feature engineering step from Teradata BTEQ to Snowflake,
implemented as dbt models.

## Artifacts

| Path | Purpose |
|------|---------|
| `dbt/models/intermediate/int_daily_balance.sql` | Ephemeral replacement for `WRK_DAILY_BALANCE` |
| `dbt/models/intermediate/int_payment_history.sql` | Ephemeral replacement for `WRK_PAYMENT_HISTORY` |
| `dbt/models/staging/stg_risk_factors.sql` | Builds `ETL_STAGING.STG_RISK_FACTORS` |
| `dbt/models/staging/_stg_risk_factors__models.yml` | Column docs, grain uniqueness and range/not-null tests |
| `dbt/models/staging/_core_banking__sources.yml`, `_txn_processing__sources.yml` | Source definitions |
| `dbt/tests/assert_stg_risk_factors_not_empty.sql` | Zero-row guard |
| `dbt/macros/log_etl_run.sql` | `ETL_RUN_LOG` post-hook |
| `dbt/analyses/recon_stg_risk_factors.sql` | Teradata/Snowflake reconciliation query |

## Running

```bash
cd dbt
dbt deps
dbt build --select +stg_risk_factors      # builds the model, then runs its tests
dbt compile --select recon_stg_risk_factors
```

## BTEQ -> Snowflake/dbt construct mapping

| BTEQ / Teradata | Snowflake / dbt |
|-----------------|-----------------|
| `CREATE MULTISET TABLE ETL_STAGING_DB.WRK_*` + `DROP TABLE` cleanup | Ephemeral dbt models inlined as CTEs - nothing is persisted, so no orphaned work tables can remain |
| `CREATE MULTISET TABLE ... AS (...) WITH DATA PRIMARY INDEX (CUSTOMER_ID)` | `materialized = 'table'`; Snowflake has no primary index (micro-partitions are automatic) |
| `.SET ERRORLEVEL 3807 SEVERITY 0`, `.IF ERRORCODE <> 0 THEN .EXIT` | dbt fails the model and the whole invocation on any SQL error |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` + validation `SELECT` | Singular test `assert_stg_risk_factors_not_empty` (`dbt build` fails the run) |
| `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG ...` | `log_etl_run()` macro as a model post-hook |
| `COLLECT STATISTICS` | Dropped - Snowflake maintains statistics automatically |
| `ADD_MONTHS(CURRENT_DATE, -n)` | `dateadd('month', -n, current_date())` |
| `CURRENT_DATE - n` | `dateadd('day', -n, current_date())` |
| `MONTHS_BETWEEN(a, b) (INTEGER)` | `cast(round(months_between(a, b)) as integer)` - Teradata rounds on a decimal-to-integer cast, Snowflake truncates, so the rounding is explicit |
| `STDDEV_POP(...)`, `QUALIFY ROW_NUMBER() OVER (...) = 1` | Supported natively, unchanged |
| Correlated `NOT IN (SELECT ... WHERE t2.ACCOUNT_ID = t.ACCOUNT_ID)` inside `COUNT(DISTINCT ...)` | Rewritten as a de-duplicated anti-join (`prior_merchants` left join, `is null` filter) - Snowflake does not allow correlated subqueries inside aggregate expressions |
| `CAST(x AS DECIMAL(p,s))` | `cast(x as number(p,s))` |
| `CURRENT_TIMESTAMP(6)` | `cast(current_timestamp() as timestamp_ntz(6))` |

## Window / null semantics verified

* **Velocity windows.** `DEBIT_VELOCITY_7D` / `_30D` are conditional sums inside
  a 30-day filtered aggregate, not window functions, so partition-boundary
  behaviour is identical in both engines. Customers with no debits in the window
  do not appear in the aggregate and are defaulted to `0.00` by the outer
  `COALESCE`, matching Teradata.
* **Volatility.** `STDDEV_POP` over a single row returns `0` in both engines and
  `NULL` over an empty group; the outer `COALESCE(..., 0.0000)` normalises the
  latter. `AVG(CASE WHEN ... END)` ignores NULLs in both engines, so a customer
  with balances only outside the 30-day window keeps a NULL 30-day average that
  is defaulted to `0.00`.
* **Sparse activity.** The driving table is `CUSTOMERS` with `LEFT JOIN`s only,
  so every active/inactive customer produces exactly one row regardless of
  transaction activity - the row-for-row reconciliation requirement.
* **De-duplication.** `int_daily_balance` keeps one row per account/day via
  `QUALIFY ROW_NUMBER()`; `prior_merchants` is `SELECT DISTINCT`, so neither
  fans out the joins.

## Reconciliation tolerance

Integer and count features (`*_CNT`, `MONTHS_SINCE_LAST_LATE`,
`EXTERNAL_CREDIT_SCORE`) must match Teradata exactly. Currency and ratio
features are stored at the same scale as the Teradata columns
(`NUMBER(15,2)` / `NUMBER(18,2)` / `NUMBER(10,4)` / `NUMBER(5,4)`), so the
agreed tolerance is one unit in the last stored decimal place (±0.01 for
amounts and percentages, ±0.0001 for `BALANCE_VOLATILITY` and
`CREDIT_UTIL_RATIO`), which absorbs Teradata/Snowflake differences in
intermediate floating-point accumulation of `STDDEV_POP` and `AVG`.

## Assumptions

* Snowflake object naming follows TICKET-01: `RETAIL_BANKING_<ENV>` with
  `CORE_BANKING`, `TXN_PROCESSING`, `ETL_STAGING` and `DATA_PRODUCTS` schemas.
  This ticket does not create databases, schemas or roles; source locations are
  overridable through the `source_*_database` / `source_*_schema` vars in
  `dbt/dbt_project.yml`.
* Credentials/profile (`retail_banking_analytics` dbt profile) come from
  TICKET-02.
* `ETL_RUN_LOG` keeps the BTEQ column list plus `DURATION_SEC`, per TICKET-03.
* `CORE_BANKING.CUSTOMER_BUREAU_SCORES` has no DDL in the repo (the BTEQ script
  calls it a simulated lookup); the columns `CUSTOMER_ID`,
  `EXTERNAL_CREDIT_SCORE` and `REPORT_DATE` are taken from the BTEQ query.
* The legacy BTEQ script is left in place for parallel-run reconciliation and is
  only annotated as superseded.
