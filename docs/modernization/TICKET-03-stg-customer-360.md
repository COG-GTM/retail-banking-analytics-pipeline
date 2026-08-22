# TICKET-03 (MBA-2204) — `01_stg_customer_360.bteq` → Snowflake SQL / dbt

Migrates the customer-360 denormalization from Teradata BTEQ to a dbt model on
Snowflake. Target relation is unchanged in name: `STG_CUSTOMER_360` in the
staging database.

## Artifacts

| Artifact | Purpose |
|---|---|
| `dbt/models/staging/stg_customer_360.sql` | The migrated transformation (`alias=STG_CUSTOMER_360`) |
| `dbt/models/staging/_sources.yml` | Snowflake sources replacing `CORE_BANKING_DB` tables |
| `dbt/models/staging/schema.yml` | Uniqueness / not-null / accepted-values tests |
| `dbt/tests/assert_stg_customer_360_not_empty.sql` | Zero-row assertion (replaces `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99`) |
| `dbt/tests/assert_stg_customer_360_credit_utilization_bounds.sql` | Divide-by-zero / NULL handling assertion |
| `dbt/macros/pipeline_run_log.sql` | Shared run-log mechanism replacing `ETL_STAGING_DB.ETL_RUN_LOG` insert |
| `dbt/analyses/recon_stg_customer_360.sql` | Row-count + column checksum reconciliation vs the Teradata extract |

## Construct mapping

| Teradata / BTEQ | Snowflake / dbt |
|---|---|
| `.LOGON ${TD_SERVER}/${TD_USERNAME},` | dbt profile with key-pair/OAuth auth (TICKET-02); no credentials in repo |
| `.SET ERRORLEVEL 3807 SEVERITY 0`, `DROP TABLE` then `CREATE TABLE AS` | dbt `table` materialization (`create or replace table`) |
| `.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE` | dbt run failure semantics — a failed node fails the run and skips downstream models |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | `assert_model_not_empty()` post-hook (raises at run time) plus a singular dbt test |
| `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG ...` | `log_pipeline_run()` post-hook writing job/step, status, row count, start/end and duration |
| `COLLECT STATISTICS COLUMN (...)` | Dropped — Snowflake maintains statistics automatically |
| `PRIMARY INDEX (CUSTOMER_ID)`, `MULTISET` | Dropped — no Snowflake equivalent; micro-partitioning is automatic |
| `QUALIFY ROW_NUMBER() OVER (...) = 1` | Same syntax; Snowflake supports `QUALIFY` natively |
| `CURRENT_DATE - DATE_OF_BIRTH` (date arithmetic yields days) | `datediff(day, DATE_OF_BIRTH, current_date())` |
| `MONTHS_BETWEEN(CURRENT_DATE, CUSTOMER_SINCE)` | `months_between(current_date(), CUSTOMER_SINCE)` |
| `CAST(x AS SMALLINT)` / `CAST(x AS INTEGER)` | `cast(x as number(5,0))` / `cast(x as number(9,0))` — both engines round rather than truncate |
| `CAST(x AS DECIMAL(5,2))` | `cast(x as number(5,2))` |
| `CURRENT_TIMESTAMP(6)` | `cast(current_timestamp() as timestamp_ntz(6))` |
| `TRIM(a) || COALESCE(', ' || TRIM(b), '')` | Identical; NULL-propagating concatenation behaves the same in Snowflake |

Semantics preserved verbatim: the `LEFT JOIN`s to the primary-address and
account-aggregate subqueries, the `HOME`/non-expired address filter, the
`MAX(CASE ...)` product flags, `SUM(COALESCE(...))` balance rollups, the
`CUSTOMER_STATUS IN ('A','I')` filter and the `TOTAL_CREDIT_LIMIT > 0`
divide-by-zero guard returning `0.00`.

## Assumptions

* Snowflake database/schema names come from TICKET-01 and are supplied as dbt
  vars (`core_banking_database`, `staging_database`, `..._schema`), defaulting to
  the `_DEV` environment suffix. No databases, schemas or roles are created here.
* Warehouse, role and secret resolution come from TICKET-02; `dbt/profiles.example.yml`
  shows the expected shape with all values read from environment variables.
* The shared run-log relation is `ETL_RUN_LOG` in the staging schema
  (`run_log_relation` var); the macro creates it with `create table if not exists`
  so the model is runnable before the shared logging ticket lands.

## Rounding / engine differences to verify during reconciliation

* `AGE`: Teradata and Snowflake both round on cast to an integer type; values
  exactly on a `.5` boundary are the only possible break.
* `TENURE_MONTHS`: Snowflake `MONTHS_BETWEEN` returns a fractional month count
  with the same end-of-month conventions as Teradata; the cast rounds to whole
  months as before.

Run the reconciliation with the Teradata extract loaded as
`STG_CUSTOMER_360_TD` in the staging schema:

```bash
cd dbt
dbt build --select stg_customer_360
dbt compile --select recon_stg_customer_360   # then execute the compiled SQL
```
