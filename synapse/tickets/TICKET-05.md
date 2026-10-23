# TICKET-05 — `02_stg_txn_summary.bteq` → T-SQL stored procedure

| | |
|---|---|
| **Ticket** | T5 |
| **Branch** | `migrate/ticket-05-stg-txn-summary` |
| **Owned paths** | `synapse/sql/staging/usp_stg_txn_summary.sql`, `synapse/tests/ticket_05/**` |
| **Contracts OWNED** | none (implements `ETL_STAGING_DB.usp_stg_txn_summary` from C7 §2) |
| **Contracts CONSUMED** | C1 (`TRANSACTIONS`, `TRANSACTION_TYPES`, `ACCOUNTS` → `STG_TXN_SUMMARY`), C2, C3, C5 §4, C7 |
| **Depends on** | **T2 + T9 contract freeze** (Wave 2). Runtime: T2 DDL, T9 helper procs, T3 data |
| **Blocks (runtime only)** | T8 (`txn_analytics`) reads `STG_TXN_SUMMARY` |

## Objective

Port `bteq/02_stg_txn_summary.bteq` to a stored procedure that rebuilds
`ETL_STAGING_DB.STG_TXN_SUMMARY` (one row per customer/account over a configurable lookback).

## Source files (read-only references)

- `bteq/02_stg_txn_summary.bteq` (logic to port)
- `ddl/01_staging_tables.sql` (`STG_TXN_SUMMARY`; target shape C1 §3.8)
- `ddl/00_source_tables.sql` (`TRANSACTIONS`, `TRANSACTION_TYPES`, `ACCOUNTS`)
- `config/pipeline_config.cfg` (`LOOKBACK_MONTHS=12`, substituted into the BTEQ via `envsubst`)

## Target output

`synapse/sql/staging/usp_stg_txn_summary.sql` (re-runnable), frozen signature:
```
ETL_STAGING_DB.usp_stg_txn_summary @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL, @LookbackMonths INT = 12
```

## Translation requirements

| BTEQ / Teradata construct | Required T-SQL |
|---|---|
| `${LOOKBACK_MONTHS}` (envsubst) | `@LookbackMonths` parameter |
| `CREATE VOLATILE TABLE VT_RUN_PARAMS AS (SELECT ADD_MONTHS(CURRENT_DATE, -N) AS PERIOD_START, CURRENT_DATE AS PERIOD_END) WITH DATA ON COMMIT PRESERVE ROWS` | local variables `@PeriodStart = DATEADD(MONTH, -@LookbackMonths, @AsOfDate)`, `@PeriodEnd = @AsOfDate` (`DATEADD` clamps to month end like `ADD_MONTHS` for e.g. Mar 31 → Feb 28; note any other edge-case difference in a comment) |
| `CROSS JOIN VT_RUN_PARAMS rp` / `rp.PERIOD_START` in SELECT + GROUP BY | variables in SELECT; drop them from `GROUP BY` |
| `DROP TABLE` + `.GOTO` + `CREATE MULTISET TABLE ... AS ... WITH DATA PRIMARY INDEX (CUSTOMER_ID, ACCOUNT_ID)` | `TRUNCATE TABLE` + `INSERT INTO ... (<C1 column list>) SELECT ...` |
| `NULLIFZERO(COUNT(*))` | `NULLIF(COUNT(*), 0)` |
| `CAST(CURRENT_DATE - MAX(t.TRANSACTION_DATE) AS INTEGER)` | `DATEDIFF(DAY, MAX(t.TRANSACTION_DATE), @AsOfDate)` |
| `top_cat` subquery: `QUALIFY ROW_NUMBER() OVER (PARTITION BY t2.ACCOUNT_ID ORDER BY SUM(ABS(t2.AMOUNT)) OVER (PARTITION BY t2.ACCOUNT_ID, t2.MERCHANT_CATEGORY) DESC) = 1` | Rewrite as: aggregate `SUM(ABS(AMOUNT))` per `(ACCOUNT_ID, MERCHANT_CATEGORY)` in the window (posted, non-null category), then `ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY total DESC, MERCHANT_CATEGORY)` in a derived table / `#temp` table, keep `rn = 1`. The added `MERCHANT_CATEGORY` tie-breaker makes the result deterministic (Teradata's was arbitrary on ties) — note it in a comment |
| `BETWEEN rp.PERIOD_START AND rp.PERIOD_END` | `BETWEEN @PeriodStart AND @PeriodEnd` (inclusive, unchanged) |
| `MAX(top_cat.MERCHANT_CATEGORY)` + `top_cat.MERCHANT_CATEGORY` in `GROUP BY` | keep equivalent semantics |
| `CHANNEL_CODE IN ('MOB')` for `PCT_MOBILE`; `'ATM'`, `'POS'`, `'WEB'` | unchanged |
| `* 100.0 / ...` then `CAST(... AS DECIMAL(5,2))` | unchanged |
| `CURRENT_TIMESTAMP(6)` | `SYSUTCDATETIME()` |
| `COLLECT STATISTICS` x3 | `UPDATE STATISTICS ETL_STAGING_DB.STG_TXN_SUMMARY` |
| `.IF ERRORCODE` / `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | `TRY/CATCH` + `THROW`; `usp_validate_table @KeyCols = 'CUSTOMER_ID,ACCOUNT_ID', @NotNullCols = 'CUSTOMER_ID,ACCOUNT_ID', @MinRows = 1` |
| `ETL_RUN_LOG` insert (`'02_stg_txn_summary','FULL_LOAD','SUCCESS'`) | `usp_log_step` START + SUCCESS/ERROR (C2 §4) |

## Reference data

`data/02_bteq_staging/stg_txn_summary.csv` — **1,251** rows, `SUMMARY_PERIOD_START = 2025-04-10`,
`SUMMARY_PERIOD_END = 2026-04-10` for as-of `2026-04-10`. Some integer columns in the CSV are
written as `48.0` (float formatting from pandas); cast when comparing.

## Acceptance criteria

- [ ] Runs in the local SQL Server container (port **14335**) on `data/01_source_tables/*.csv` with `@AsOfDate = '2026-04-10'`, `@LookbackMonths = 12`, producing **1,251** rows.
- [ ] Counts, amounts, averages (to 0.01), maxima, `DISTINCT_MERCHANTS`, channel percentages and `DAYS_SINCE_LAST_TXN` match the reference CSV; `TOP_MERCHANT_CATEGORY` agreement reported (ties may legitimately differ).
- [ ] `@LookbackMonths` is honoured (test with 3 → fewer transactions, period start `2026-01-10`).
- [ ] Audit rows (`FULL_LOAD` START/SUCCESS 1,251, `VALIDATE` SUCCESS) present; failure path logs `ERROR` and re-throws; re-run idempotent.
- [ ] No Teradata syntax remains (`VOLATILE`, `QUALIFY`, `NULLIFZERO`, `ADD_MONTHS`, `.IF`, `COLLECT STATISTICS`).
- [ ] Local stand-ins for C2/C3 helper procs (if needed before T9 merges) live only in `synapse/tests/ticket_05/`.

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T5] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_05/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_05/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
