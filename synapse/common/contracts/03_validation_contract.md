# C3 — Validation contract (row count, key uniqueness, NOT NULL)

| | |
|---|---|
| **Owner** | [T9](../../tickets/TICKET-09.md) — implements `synapse/common/validation.py` and `synapse/common/sql/usp_validate_table.sql` |
| **Consumers** | T3, T4, T5, T6 (SQL helper); T7, T8, T9 (Python helper) |
| **Status** | **FROZEN v1.0** (prerequisite contract) |
| **Replaces** | `sas/macros/validate_table.sas` (`%validate_table(lib=, table=, key_cols=, not_null=, min_rows=)` + `&VALIDATION_RC` + `%abort cancel`); BTEQ zero-row guards `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` |

## 1. Semantics (must mirror `%validate_table` exactly, in this order)

| # | Check | On failure | SAS behaviour preserved |
|---|---|---|---|
| 1 | `row_count >= min_rows` | **fail** (stop, later checks skipped) | `ERROR:` + `VALIDATION_RC=1` + `%return` |
| 2 | no duplicate groups on `key_cols` (`GROUP BY key_cols HAVING COUNT(*) > 1`) | **fail** | `ERROR:` + `VALIDATION_RC=1` + `%return` |
| 3 | each `not_null` column has 0 NULLs | **warning only** — logged, does **not** fail | SAS only emits `WARNING:` for NULLs |

A failed validation must stop the job **before** any write-back (SAS validated `WORK.*` and
then `%abort cancel` before the `DELETE` + `PROC APPEND`).

Note on the BTEQ guards: each script runs `SELECT COUNT(*) ... ; .IF ACTIVITYCOUNT = 0 THEN .EXIT 99;`.
Because `SELECT COUNT(*)` always returns one row, that guard can never fire as written. The
target implements the evident intent (fail when the staging table is empty) via
`usp_validate_table @MinRows = 1`, raising error `50099`.

## 2. Thresholds

| Caller | `min_rows` |
|---|---|
| Staging procs T4–T6, source load T3 | `1` |
| Spark jobs T7–T9 | `common.validation.default_min_rows(env)` = `1000` for `UAT`/`PROD`, `1` for `DEV` (env compared case-insensitively). SAS hard-coded `min_rows=1000`, which the 500-customer sample data cannot satisfy. |

## 3. Error numbers (T-SQL `THROW`)

| Number | Meaning |
|---|---|
| 50010 | invalid audit status passed to `usp_log_step` (C2) |
| 50099 | table has 0 rows (BTEQ `.EXIT 99` equivalent) |
| 50100 | `0 < row_count < min_rows` |
| 50101 | duplicate key groups found |

Python raises `common.validation.ValidationError` (with the `ValidationResult` attached) for the same conditions.

## 4. Interfaces

### 4.1 T-SQL — `ETL_STAGING_DB.usp_validate_table` (file `synapse/common/sql/usp_validate_table.sql`)
```
ETL_STAGING_DB.usp_validate_table
    @RunId        VARCHAR(64),
    @JobName      VARCHAR(100),
    @SchemaName   SYSNAME,
    @TableName    SYSNAME,
    @KeyCols      VARCHAR(4000) = NULL,   -- comma-separated, e.g. 'CUSTOMER_ID,ACCOUNT_ID'
    @NotNullCols  VARCHAR(4000) = NULL,   -- comma-separated
    @MinRows      BIGINT        = 1,
    @Env          VARCHAR(10)   = 'DEV',
    @AsOfDate     DATE          = NULL,
    @RowCount     BIGINT        = NULL OUTPUT
```
- Builds dynamic SQL with `QUOTENAME` on every identifier (no raw concatenation of names).
- Logs via `usp_log_step`: one `VALIDATE`/`WARNING` row per NULL-violating column, then `VALIDATE`/`SUCCESS` (with `ROW_COUNT`) or `VALIDATE`/`ERROR` before `THROW`.

### 4.2 Python — `common.validation` (file `synapse/common/validation.py`, stub [`stubs/validation.pyi`](stubs/validation.pyi))
```python
default_min_rows(env: str) -> int
validate_dataframe(cfg, df, table_label, job_name, key_cols=(), not_null=(), min_rows=None, raise_on_fail=True) -> ValidationResult
validate_sql_table(cfg, table, job_name, key_cols=(), not_null=(), min_rows=None, raise_on_fail=True) -> ValidationResult
class ValidationResult: table_label, row_count, duplicate_key_groups, null_counts, passed, warnings
class ValidationError(Exception): result
```
`min_rows=None` means `default_min_rows(cfg.env)`. Spark jobs validate the **final DataFrame**
(the SAS `WORK.*_FINAL` equivalent) with `validate_dataframe`, then call C5 write-back.

## 5. Per-job validation parameters (from the SAS / BTEQ sources)

| Job | Object validated | `key_cols` | `not_null` |
|---|---|---|---|
| T3 load | each source table | — | — |
| T4 | `ETL_STAGING_DB.STG_CUSTOMER_360` | `CUSTOMER_ID` | `CUSTOMER_ID` |
| T5 | `ETL_STAGING_DB.STG_TXN_SUMMARY` | `CUSTOMER_ID,ACCOUNT_ID` | `CUSTOMER_ID,ACCOUNT_ID` |
| T6 | `ETL_STAGING_DB.STG_RISK_FACTORS` | `CUSTOMER_ID` | `CUSTOMER_ID` |
| T7 | segments final DF | `CUSTOMER_ID` | `CUSTOMER_ID SEGMENT_NAME SEGMENT_ID` |
| T8 txn | txn analytics final DF | `CUSTOMER_ID` | `CUSTOMER_ID REPORTING_PERIOD TOTAL_TRANSACTIONS` |
| T8 risk | risk final DF | `CUSTOMER_ID` | `CUSTOMER_ID COMPOSITE_RISK_SCORE RISK_TIER` |
| T9 | master profile final DF | `CUSTOMER_ID` | `CUSTOMER_ID FULL_NAME CUSTOMER_STATUS` |

(Key checks for T4–T6 are additions — BTEQ had none — and are cheap safety nets; they must pass on the reference data.)
