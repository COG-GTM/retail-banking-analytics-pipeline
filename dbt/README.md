# dbt (Snowflake) models

Snowflake replacement for the Teradata BTEQ staging layer. Added by
MBA-2204 / TICKET-03, which migrates `bteq/01_stg_customer_360.bteq`.

## Contents

| Path | Purpose |
|------|---------|
| `models/staging/stg_customer_360.sql` | Customer-360 denormalization (was `01_stg_customer_360.bteq`) |
| `models/staging/_core_banking__sources.yml` | Source definitions for CUSTOMERS / ACCOUNTS / ADDRESSES |
| `models/staging/_staging__models.yml` | Column docs and dbt tests (uniqueness, not-null, accepted values) |
| `tests/assert_stg_customer_360_not_empty.sql` | Zero-row guard (was `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99`) |
| `macros/log_etl_run.sql` | Run-log post-hook (was the `ETL_RUN_LOG` audit insert) |
| `analyses/recon_stg_customer_360.sql` | Teradata/Snowflake reconciliation query (row count + column checksums) |

## Running

```bash
cd dbt
dbt build --select stg_customer_360        # runs the model, then its tests
dbt compile --select recon_stg_customer_360
```

Connection details come from a `retail_banking_analytics` profile provisioned by
TICKET-02 (Snowflake environment, role and secret management). Source and
run-log locations are overridable via the vars in `dbt_project.yml`.

## BTEQ construct mapping

| BTEQ | Snowflake / dbt |
|------|-----------------|
| `.SET ERRORLEVEL` / `.IF ERRORCODE <> 0 THEN .EXIT` | dbt run fails the model and the invocation |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | singular test `assert_stg_customer_360_not_empty` |
| `INSERT INTO ETL_RUN_LOG ...` | `log_etl_run()` macro as a model post-hook |
| `COLLECT STATISTICS` | dropped (automatic in Snowflake) |
| `CREATE MULTISET TABLE ... AS ... PRIMARY INDEX` | `materialized = 'table'` |
| `QUALIFY ROW_NUMBER() OVER (...) = 1` | supported natively, unchanged |
