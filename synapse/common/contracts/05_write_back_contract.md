# C5 — Truncate-and-load write-back pattern for `DATA_PRODUCTS_DB`

| | |
|---|---|
| **Owner** | [T9](../../tickets/TICKET-09.md) — implements `synapse/common/writeback.py` (+ `common.schema`, used by `conform_to_contract`) |
| **Consumers** | T7 (`CUSTOMER_SEGMENTS`), T8 (`TRANSACTION_ANALYTICS`, `CUSTOMER_RISK_SCORES`), T9 (`CUSTOMER_MASTER_PROFILE`) |
| **Status** | **FROZEN v1.0** (prerequisite contract) |
| **Replaces** | SAS pattern in every program: `proc sql; connect to teradata (...); execute (DELETE FROM DATA_PRODUCTS_DB.<T> [WHERE ...]) by teradata; quit; proc append base=DPDB.<T> data=WORK.<FINAL> force; run;` and the post-load `COLLECT STATISTICS` pass-through in `sas/04_sas_data_products.sas` |

## 1. Interface — `common.writeback` (stub [`stubs/writeback.pyi`](stubs/writeback.pyi))

```python
conform_to_contract(df, table) -> DataFrame
truncate_and_load(cfg, df, table, job_name, delete_predicate=None, predicate_params=None, update_stats=True) -> int
```

## 2. Required sequence (inside `truncate_and_load`)

1. **Guard:** `table` must be `DATA_PRODUCTS_DB.<name>` from C1 (anything else → `ValueError`).
2. **Conform:** `conform_to_contract(df, table)` — select exactly the C1 columns in C1 order, cast to the C1 Spark types (`DecimalType(p,s)`, `DateType`, `TimestampType`, `StringType` for VARCHAR/CHAR, `ShortType`/`IntegerType`/`LongType`), fail on any missing column, drop extra columns (log their names; this replaces `PROC APPEND ... FORCE`, which silently dropped extra variables).
3. **Clear target** via `common.sql_connector.execute_sql`:
   - `delete_predicate is None` → `TRUNCATE TABLE [DATA_PRODUCTS_DB].[<T>]`
   - otherwise → `DELETE FROM [DATA_PRODUCTS_DB].[<T>] WHERE <delete_predicate>` with `predicate_params` bound as `?` parameters.
4. **Append** the DataFrame: on Synapse `df.write.mode("append").synapsesql("<db>.DATA_PRODUCTS_DB.<T>")`; locally Spark JDBC `mode("append")` (`cfg.sql_mode == "jdbc"`) or Parquet under `cfg.local_table_dir` (`"local"`). **Never `overwrite`.**
5. **Statistics:** if `update_stats`, `UPDATE STATISTICS [DATA_PRODUCTS_DB].[<T>]` (replaces `COLLECT STATISTICS`).
6. **Audit:** `log_step(cfg, job_name, "LOAD", "SUCCESS", row_count=<rows appended>)`; on any failure log `LOAD`/`ERROR` and re-raise.
7. Return rows appended.

Callers **must** validate (C3) *before* calling; a failed validation must leave the target untouched.

Failure semantics: as in SAS, the clear and the append are not atomic. If the append fails, the
target may be empty/partial until the job is re-run; jobs are idempotent per as-of date, so a
re-run is the recovery. (A swap-table pattern is explicitly out of scope for v1.)

## 3. Per-table usage

| Table | Writer | Clear step | Source of rule |
|---|---|---|---|
| `DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS` | T7 | full `TRUNCATE` | `sas/01_sas_customer_segments.sas` STEP 8 (`DELETE FROM ...` no WHERE) |
| `DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS` | T8 | `delete_predicate="REPORTING_PERIOD = ?"`, `predicate_params=[reporting_period]` | `sas/02_sas_txn_analytics.sas` STEP 7 (deletes current period only; history of other periods is kept) |
| `DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES` | T8 | full `TRUNCATE` | `sas/03_sas_risk_scoring.sas` STEP 6 |
| `DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE` | T9 | full `TRUNCATE` | `sas/04_sas_data_products.sas` STEP 5 |

T9's `data_products.py` additionally runs `UPDATE STATISTICS` on `CUSTOMER_SEGMENTS` and
`CUSTOMER_RISK_SCORES` (SAS STEP 6 collected stats on those too) via `execute_sql`.

## 4. Staging-layer equivalent (T4–T6, T-SQL only)

Staging procs follow the same idea natively in T-SQL — `TRUNCATE TABLE ETL_STAGING_DB.<STG>`
then `INSERT INTO ETL_STAGING_DB.<STG> (<C1 column list>) SELECT ...`, then
`UPDATE STATISTICS` — and never `DROP`/`CREATE` the contract table (the BTEQ
`DROP TABLE` + `CREATE TABLE AS ... WITH DATA` is replaced, because the table definition is
owned by T2).
