# TICKET-02 — Port Teradata DDL to dedicated SQL pool T-SQL

| | |
|---|---|
| **Ticket** | T2 |
| **Branch** | `migrate/ticket-02-ddl` |
| **Owned paths** | `synapse/sql/ddl/**`, `synapse/tests/ticket_02/**`, and (as contract owner) `synapse/common/contracts/01_schema_contract.md` |
| **Contracts OWNED** | **C1 [schema](../common/contracts/01_schema_contract.md)** — prerequisite contract |
| **Contracts CONSUMED** | C2 §2 (`ETL_RUN_LOG` column shape, owned by T9 — you create the table), C6 (collation) |
| **Depends on** | nothing — Wave 1, can start immediately |
| **Blocks** | live integration/merge of T3–T10 (they code against C1 meanwhile) |

## Objective

Translate the three Teradata DDL files into idempotent T-SQL for an Azure Synapse **dedicated
SQL pool**, converting `MULTISET` / `PRIMARY INDEX` / `COLLECT STATISTICS` into `DISTRIBUTION`
+ `CLUSTERED COLUMNSTORE INDEX` + `CREATE STATISTICS`, and add the two tables the pipeline uses
but never defines.

**First step (contract freeze):** re-read C1 against the Teradata DDL below. If you find a
defect, fix `01_schema_contract.md` in the **first commit** of your PR and call it out at the top
of the PR description so the integrator can notify consumers. Otherwise state "C1 v1.0 confirmed".

## Source files (read-only references)

- `ddl/00_source_tables.sql` — `CORE_BANKING_DB.CUSTOMERS`, `ACCOUNTS`, `ADDRESSES`; `TXN_PROCESSING_DB.TRANSACTIONS` (monthly `RANGE_N` partitioning), `TRANSACTION_TYPES` (`UNIQUE PRIMARY INDEX`)
- `ddl/01_staging_tables.sql` — `ETL_STAGING_DB.STG_CUSTOMER_360`, `STG_TXN_SUMMARY`, `STG_RISK_FACTORS` + `COLLECT STATISTICS`
- `ddl/02_data_product_tables.sql` — `DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS`, `TRANSACTION_ANALYTICS` (`PARTITION BY COLUMN`), `CUSTOMER_RISK_SCORES`, `CUSTOMER_MASTER_PROFILE` + `COLLECT STATISTICS`
- `export_data.py` lines 64-80 — column lists incl. `core_banking.customer_bureau_scores (customer_id BIGINT, external_credit_score INTEGER, report_date DATE)`
- `bteq/01_stg_customer_360.bteq`, `bteq/02_stg_txn_summary.bteq` — extra `COLLECT STATISTICS` columns
- `bteq/03_stg_risk_factors.bteq` — uses `CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES` and `ETL_STAGING_DB.ETL_RUN_LOG` (no DDL exists for either)

## Target outputs

```
synapse/sql/ddl/
├── README.md                       # replace placeholder: run order, conventions, Teradata→Synapse mapping table
├── 00_schemas.sql                  # CREATE SCHEMA x4 (guarded with IF NOT EXISTS / sys.schemas)
├── 01_source_tables.sql            # 6 source tables incl. CUSTOMER_BUREAU_SCORES
├── 02_staging_tables.sql           # 3 STG tables + ETL_RUN_LOG
├── 03_data_product_tables.sql      # 4 DP tables
├── 04_statistics.sql               # all CREATE STATISTICS (named stat_<TABLE>_<COL>)
└── deploy_order.txt                # one file per line, in execution order
```

## Translation requirements

| Teradata | T-SQL (dedicated SQL pool) |
|---|---|
| `CREATE MULTISET TABLE db.t, NO FALLBACK (...)` | `CREATE TABLE db.t (...) WITH (DISTRIBUTION = ..., CLUSTERED COLUMNSTORE INDEX)` — guarded by `IF OBJECT_ID('db.t') IS NULL` (idempotent; never drop data on re-run) |
| `PRIMARY INDEX (CUSTOMER_ID)` | `DISTRIBUTION = HASH(CUSTOMER_ID)` — exact choice per table in C1 §3 (e.g. `ACCOUNTS`/`TRANSACTIONS` hash on `ACCOUNT_ID`, `TRANSACTION_TYPES` `REPLICATE` + `CLUSTERED INDEX`, `ETL_RUN_LOG` `ROUND_ROBIN` + `HEAP`) |
| `UNIQUE PRIMARY INDEX` | no constraint (C1 §2); uniqueness is checked by validation helpers |
| `PARTITION BY RANGE_N(TRANSACTION_DATE ... EACH INTERVAL '1' MONTH)` | yearly `PARTITION (TRANSACTION_DATE RANGE RIGHT FOR VALUES ('2021-01-01', …, '2030-01-01'))` |
| `PARTITION BY COLUMN(REPORTING_PERIOD ...)` | removed (CCI is columnar) |
| `CHARACTER SET LATIN NOT CASESPECIFIC` | plain `VARCHAR`/`CHAR` (CI collation set at DB level by T1) |
| `FORMAT 'YYYY-MM-DD'` | removed |
| `TIMESTAMP(6)` | `DATETIME2(6)` |
| `INTEGER` | `INT` |
| `DEFAULT 'N'` / `DEFAULT 'US'` | kept as constant `DEFAULT` |
| `COLLECT STATISTICS COLUMN (c) ON t` | `CREATE STATISTICS stat_<T>_<c> ON db.t (c)` guarded by `sys.stats` existence check; full list in C1 §3 |
| Comments describing producers/consumers | keep as T-SQL comments |

Also create `ETL_STAGING_DB.ETL_RUN_LOG` **exactly** as C2 §2 (incl. `LOG_ID BIGINT IDENTITY(1,1)`)
and `CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES` per C1 §3.4. Use `GO` batch separators so files run in
SSMS / `sqlcmd` / Synapse Script activities.

## Acceptance criteria

- [ ] Every table/column/type/nullability/default/distribution/index/partition in C1 is implemented, in C1 column order — verified by a test that parses `01_schema_contract.md` and compares to `INFORMATION_SCHEMA.COLUMNS` after running the DDL in the local SQL Server container (shim strips the dedicated-pool-only `WITH (...)`/`PARTITION` clauses for local execution; a second test asserts those clauses are present in the source text).
- [ ] All scripts are idempotent: running `deploy_order.txt` twice succeeds and leaves data intact.
- [ ] All 13 C1 tables + `ETL_RUN_LOG` + all named statistics exist after a run.
- [ ] The six `data/01_source_tables/*.csv` files load into the created source tables without type errors (proves column order/types fit the real extracts).
- [ ] No Teradata-only syntax remains (`MULTISET`, `PRIMARY INDEX`, `COLLECT STATISTICS`, `FALLBACK`, `CHARACTER SET`, `CASESPECIFIC`, `FORMAT '`) — enforced by a grep test.
- [ ] PR description states "C1 v1.0 confirmed" or lists the C1 amendments made in the first commit.

Local SQL Server host port for this ticket: **14332**.

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T2] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_02/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_02/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
