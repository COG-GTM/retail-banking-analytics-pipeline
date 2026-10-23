# TICKET-04 — `01_stg_customer_360.bteq` → T-SQL stored procedure

| | |
|---|---|
| **Ticket** | T4 |
| **Branch** | `migrate/ticket-04-stg-customer-360` |
| **Owned paths** | `synapse/sql/staging/usp_stg_customer_360.sql`, `synapse/tests/ticket_04/**` |
| **Contracts OWNED** | none (implements `ETL_STAGING_DB.usp_stg_customer_360` from C7 §2) |
| **Contracts CONSUMED** | C1 (`CUSTOMERS`, `ACCOUNTS`, `ADDRESSES` → `STG_CUSTOMER_360`), C2 (`usp_log_step`), C3 (`usp_validate_table`), C5 §4 (staging load pattern), C7 |
| **Depends on** | **T2 + T9 contract freeze** (Wave 2). Runtime: T2 DDL, T9 helper procs, T3 data |
| **Blocks (runtime only)** | T7, T8 (`risk_scoring`), T9 (`data_products`) read `STG_CUSTOMER_360` |

## Objective

Port `bteq/01_stg_customer_360.bteq` to a dedicated-SQL-pool stored procedure that rebuilds
`ETL_STAGING_DB.STG_CUSTOMER_360` with the same business logic, replacing BTEQ control flow
with T-SQL error handling and audit/validation helper calls.

## Source files (read-only references)

- `bteq/01_stg_customer_360.bteq` (the logic to port)
- `ddl/01_staging_tables.sql` (`STG_CUSTOMER_360` definition; target shape is C1 §3.7)
- `ddl/00_source_tables.sql` (`CUSTOMERS`, `ACCOUNTS`, `ADDRESSES`)
- `bteq/run_bteq_pipeline.sh` (error semantics of the orchestrator)

## Target output

`synapse/sql/staging/usp_stg_customer_360.sql` — re-runnable script (`IF OBJECT_ID('ETL_STAGING_DB.usp_stg_customer_360') IS NOT NULL DROP PROCEDURE ...; GO; CREATE PROCEDURE ...`) with the frozen signature:
```
ETL_STAGING_DB.usp_stg_customer_360 @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL
```

## Translation requirements

| BTEQ / Teradata construct (in `bteq/01_stg_customer_360.bteq`) | Required T-SQL |
|---|---|
| `.LOGON`, `.SET WIDTH`, `.SET SESSION TRANSACTION BTET`, `.LOGOFF`, `.EXIT 0` | removed (connection is the pipeline's linked service) |
| `.SET ERRORLEVEL 3807 SEVERITY 0` + `DROP TABLE` + `.GOTO STEP1_CONTINUE` | removed — table is owned by T2; proc does `TRUNCATE TABLE ETL_STAGING_DB.STG_CUSTOMER_360` |
| `CREATE MULTISET TABLE ... AS (SELECT ...) WITH DATA PRIMARY INDEX (CUSTOMER_ID)` | `INSERT INTO ETL_STAGING_DB.STG_CUSTOMER_360 (<C1 column list>) SELECT ...` |
| `CURRENT_DATE` | `@AsOfDate` (`SET @AsOfDate = COALESCE(@AsOfDate, CAST(SYSUTCDATETIME() AS DATE))`) |
| `CAST((CURRENT_DATE - c.DATE_OF_BIRTH) / 365.25 AS SMALLINT)` | `CAST(DATEDIFF(DAY, c.DATE_OF_BIRTH, @AsOfDate) / 365.25 AS SMALLINT)` (T-SQL truncates toward zero; confirm this matches the Teradata cast behaviour and note it in a comment) |
| `CAST(MONTHS_BETWEEN(CURRENT_DATE, c.CUSTOMER_SINCE) AS INTEGER)` | Teradata `MONTHS_BETWEEN` is fractional and the cast truncates; `DATEDIFF(MONTH, ...)` counts boundaries instead. Use `DATEDIFF(MONTH, c.CUSTOMER_SINCE, @AsOfDate) - CASE WHEN DAY(@AsOfDate) < DAY(c.CUSTOMER_SINCE) THEN 1 ELSE 0 END` (document end-of-month edge cases in a comment) |
| `TRIM(a.ADDRESS_LINE_1) \|\| COALESCE(', ' \|\| TRIM(a.ADDRESS_LINE_2), '')` | Teradata `\|\|` returns NULL if any operand is NULL — **do not use `CONCAT`** (it treats NULL as ''). Use `LTRIM(RTRIM(a.ADDRESS_LINE_1)) + COALESCE(', ' + LTRIM(RTRIM(a.ADDRESS_LINE_2)), '')` (NULL-propagating `+`, `CONCAT_NULL_YIELDS_NULL` default ON) |
| `QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY EFFECTIVE_DATE DESC) = 1` | derived table computing `ROW_NUMBER()` as `rn`, outer `WHERE rn = 1` |
| `(EXPIRATION_DATE IS NULL OR EXPIRATION_DATE > CURRENT_DATE)` | same with `@AsOfDate` |
| `CAST(... / ... * 100 AS DECIMAL(5,2))` | same |
| `CURRENT_TIMESTAMP(6)` for `LOAD_TS` | `SYSUTCDATETIME()` (UTC, C1) |
| `COLLECT STATISTICS COLUMN (...)` (4 columns) | `UPDATE STATISTICS ETL_STAGING_DB.STG_CUSTOMER_360` (stats objects created by T2) |
| `.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE` | `BEGIN TRY ... END TRY BEGIN CATCH` → `usp_log_step ... 'ERROR'` with `ERROR_MESSAGE()` → `THROW` |
| Validation `SELECT COUNT(*) ...; .IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | `EXEC ETL_STAGING_DB.usp_validate_table ... @KeyCols = 'CUSTOMER_ID', @NotNullCols = 'CUSTOMER_ID', @MinRows = 1` (C3 §1 explains why the BTEQ guard never fired) |
| `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG (...) SELECT '01_stg_customer_360','FULL_LOAD','SUCCESS',COUNT(*),...` | `usp_log_step @JobName = '01_stg_customer_360', @StepName = 'FULL_LOAD'` START at entry and SUCCESS with row count at end (C2 §4) |
| `WHERE c.CUSTOMER_STATUS IN ('A','I')` | unchanged |

Keep column aliases and comments close to the original so reviewers can diff logic side by side.
Avoid CTEs inside CTAS (dedicated-pool restriction) — not needed here since you use `INSERT ... SELECT`.

## Known divergences vs. reference CSV (`data/02_bteq_staging/stg_customer_360.csv`, 478 rows)

The CSV was produced by a DuckDB re-implementation that computes `AGE` as a calendar-year
difference and `TENURE_MONTHS` as month-boundary counts. A faithful port of the BTEQ logic will
differ on many rows for those two columns (TENURE_MONTHS is +1 in the CSV when `DAY(as_of) < DAY(CUSTOMER_SINCE)`).
**Port the BTEQ semantics**, and report agreement for those two columns; every other column must match.

## Acceptance criteria

- [ ] Proc compiles and runs in the local SQL Server container (port **14334**) against C1-shaped source tables loaded from `data/01_source_tables/*.csv`, with `@AsOfDate = '2026-04-10'`, producing **478** rows.
- [ ] All columns except `AGE`, `TENURE_MONTHS`, `LOAD_TS` match `data/02_bteq_staging/stg_customer_360.csv` exactly (decimals to 0.01); `AGE`/`TENURE_MONTHS` agreement reported and differences explained by the rule above.
- [ ] `ETL_RUN_LOG` contains `FULL_LOAD` START + SUCCESS (row count 478) and a `VALIDATE` SUCCESS row for the run id.
- [ ] Failure path tested: e.g. drop a source table → proc logs `ERROR` and re-throws.
- [ ] Re-running the proc is idempotent (still 478 rows).
- [ ] No Teradata syntax remains (`QUALIFY`, `MONTHS_BETWEEN`, `||`, `.IF`, `COLLECT STATISTICS`).
- [ ] For local tests, a minimal stand-in for `usp_log_step`/`usp_validate_table` matching the C2/C3 signatures may live in `synapse/tests/ticket_04/` until T9 merges; do not create them under `synapse/common/`.

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T4] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_04/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_04/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
