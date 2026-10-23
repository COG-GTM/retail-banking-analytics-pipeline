# TICKET-06 — `03_stg_risk_factors.bteq` → T-SQL stored procedure

| | |
|---|---|
| **Ticket** | T6 |
| **Branch** | `migrate/ticket-06-stg-risk-factors` |
| **Owned paths** | `synapse/sql/staging/usp_stg_risk_factors.sql`, `synapse/tests/ticket_06/**` |
| **Contracts OWNED** | none (implements `ETL_STAGING_DB.usp_stg_risk_factors` from C7 §2) |
| **Contracts CONSUMED** | C1 (`CUSTOMERS`, `ACCOUNTS`, `TRANSACTIONS`, `TRANSACTION_TYPES`, `CUSTOMER_BUREAU_SCORES` → `STG_RISK_FACTORS`), C2, C3, C5 §4, C7 |
| **Depends on** | **T2 + T9 contract freeze** (Wave 2). Runtime: T2 DDL, T9 helper procs, T3 data |
| **Blocks (runtime only)** | T8 (`risk_scoring`) reads `STG_RISK_FACTORS` |

## Objective

Port `bteq/03_stg_risk_factors.bteq` — the most complex BTEQ script (two work tables, eight
sub-aggregations, a correlated sub-query inside an aggregate) — to a stored procedure that
rebuilds `ETL_STAGING_DB.STG_RISK_FACTORS`.

## Source files (read-only references)

- `bteq/03_stg_risk_factors.bteq` (logic to port)
- `ddl/01_staging_tables.sql` (`STG_RISK_FACTORS`; target shape C1 §3.9)
- `ddl/00_source_tables.sql`; `export_data.py` line 69 (`customer_bureau_scores` shape — no Teradata DDL exists; C1 §3.4)

## Target output

`synapse/sql/staging/usp_stg_risk_factors.sql` (re-runnable), frozen signature:
```
ETL_STAGING_DB.usp_stg_risk_factors @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL
```

## Translation requirements

| BTEQ / Teradata construct | Required T-SQL |
|---|---|
| Work tables `ETL_STAGING_DB.WRK_DAILY_BALANCE`, `WRK_PAYMENT_HISTORY` (DROP / CREATE ... WITH DATA / COLLECT STATS / DROP at end) | session temp tables `#WRK_DAILY_BALANCE`, `#WRK_PAYMENT_HISTORY` created with `CREATE TABLE #x WITH (DISTRIBUTION = HASH(CUSTOMER_ID), HEAP) AS SELECT ...` (no CTEs inside the CTAS); dropped at the end and in `CATCH`. They are **not** contract tables |
| `QUALIFY ROW_NUMBER() OVER (PARTITION BY t.ACCOUNT_ID, t.TRANSACTION_DATE ORDER BY t.TRANSACTION_TS DESC) = 1` | derived table with `rn`, `WHERE rn = 1` |
| `QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY REPORT_DATE DESC) = 1` (bureau) | same pattern |
| `ADD_MONTHS(CURRENT_DATE, -3 / -6 / -12 / -24)` | `DATEADD(MONTH, -n, @AsOfDate)` |
| `CURRENT_DATE - 7 / - 30 / - 90` | `DATEADD(DAY, -n, @AsOfDate)` |
| `MONTHS_BETWEEN(t.TRANSACTION_DATE, acct.OPEN_DATE) (INTEGER)` (Teradata-style cast) | truncated fractional months: `DATEDIFF(MONTH, acct.OPEN_DATE, t.TRANSACTION_DATE) - CASE WHEN DAY(t.TRANSACTION_DATE) < DAY(acct.OPEN_DATE) THEN 1 ELSE 0 END` — factor into one expression reused for on-time / late / last-late |
| `ADD_MONTHS(acct.OPEN_DATE, n + 1)` | `DATEADD(MONTH, n + 1, acct.OPEN_DATE)` |
| `CAST(MONTHS_BETWEEN(CURRENT_DATE, COALESCE(MAX(...), acct.OPEN_DATE)) AS INTEGER)` | same truncated-months expression against `@AsOfDate` |
| `STDDEV_POP(EOD_BALANCE)` | `STDEVP(EOD_BALANCE)` |
| `tt.DESCRIPTION LIKE '%NSF%'` | unchanged (CI collation, C6) |
| **Correlated `NOT IN` sub-query inside `COUNT(DISTINCT CASE ...)`** (new merchants in last 30 days never used before on that account) | T-SQL forbids sub-queries inside aggregates. Pre-compute prior merchants per account (`TRANSACTION_DATE < @AsOfDate - 30`, `MERCHANT_NAME IS NOT NULL`) into `#PRIOR_MERCHANTS (ACCOUNT_ID, MERCHANT_NAME)`, `LEFT JOIN` it on `(ACCOUNT_ID, MERCHANT_NAME)`, and count `DISTINCT CASE WHEN t.TRANSACTION_DATE >= @AsOfDate - 30 AND pm.MERCHANT_NAME IS NULL THEN t.MERCHANT_NAME END`. Preserve original semantics: the prior-merchant lookup in BTEQ is **not** limited to the 6-month window or to posted status; NULL merchant names are never counted |
| `CHANNEL_CODE = 'INTL'`; high-risk categories `'GAMBLING','WIRE_TRANSFER_INTL','CRYPTO_EXCHANGE','PAWN_SHOP'` | unchanged |
| `CAST(credit.TOTAL_CREDIT_BAL / credit.TOTAL_CREDIT_LIMIT AS DECIMAL(5,4))` | unchanged (overflows above 9.9999 exactly as Teradata would; do not silently clamp) |
| `COALESCE(..., 0 / 0.00 / 999 / 100.00)` defaults | unchanged |
| `CREATE MULTISET TABLE STG_RISK_FACTORS AS (...) WITH DATA` | `TRUNCATE` + `INSERT INTO ... (<C1 column list>) SELECT ...` |
| `CURRENT_TIMESTAMP(6)` | `SYSUTCDATETIME()` |
| `COLLECT STATISTICS` | `UPDATE STATISTICS ETL_STAGING_DB.STG_RISK_FACTORS` |
| `.IF ERRORCODE` / `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | `TRY/CATCH` + `THROW`; `usp_validate_table @KeyCols = 'CUSTOMER_ID', @NotNullCols = 'CUSTOMER_ID', @MinRows = 1` |
| `ETL_RUN_LOG` insert (`'03_stg_risk_factors','FULL_LOAD','SUCCESS'`) | `usp_log_step` START + SUCCESS/ERROR (C2 §4); optional intermediate `BUILD` steps for each work table |
| `WHERE c.CUSTOMER_STATUS IN ('A','I')` | unchanged |

## Known divergences vs. reference CSV (`data/02_bteq_staging/stg_risk_factors.csv`, 478 rows)

- `MONTHS_SINCE_LAST_LATE`: the CSV uses month-boundary counts (`DATEDIFF` semantics) instead of truncated `MONTHS_BETWEEN`; expect off-by-one on many rows — report agreement.
- `NEW_MERCHANT_CNT_30D`: the CSV generator skipped the prior-merchant `NOT IN` exclusion; the faithful port will produce smaller values — report agreement.
- In the sample data every `PAYMENT_LATE_CNT` is 0.
- All other columns should match within 0.01.

## Acceptance criteria

- [ ] Runs in the local SQL Server container (port **14336**) on `data/01_source_tables/*.csv` with `@AsOfDate = '2026-04-10'`, producing **478** rows.
- [ ] All columns except `MONTHS_SINCE_LAST_LATE`, `NEW_MERCHANT_CNT_30D`, `LOAD_TS` match the reference CSV within 0.01; agreement for those two reported with the reasons above.
- [ ] A focused unit test proves the new-merchant rewrite matches the BTEQ `NOT IN` semantics on a hand-built fixture (merchant seen before 30 days ago → not counted; first seen in last 30 days → counted; NULL merchant → not counted).
- [ ] Temp tables are cleaned up on success and failure; audit rows present; failure path re-throws; re-run idempotent.
- [ ] No Teradata syntax remains (`QUALIFY`, `MONTHS_BETWEEN`, `ADD_MONTHS`, `STDDEV_POP`, `(INTEGER)` casts, `.IF`, `COLLECT STATISTICS`).
- [ ] Local stand-ins for C2/C3 helper procs (if needed before T9 merges) live only in `synapse/tests/ticket_06/`.

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T6] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_06/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_06/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
