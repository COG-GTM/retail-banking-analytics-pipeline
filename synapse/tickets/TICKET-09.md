# TICKET-09 — Shared utilities (`synapse/common/`) + `04_sas_data_products.sas` golden record

| | |
|---|---|
| **Ticket** | T9 |
| **Branch** | `migrate/ticket-09-common-golden-record` |
| **Owned paths** | `synapse/common/**` except `synapse/common/__init__.py` (prep-created, keep empty) and contract files it does not own; `synapse/spark/data_products.py`; `synapse/tests/ticket_09/**` |
| **Contracts OWNED** | **C2 [audit logging](../common/contracts/02_audit_logging_contract.md)**, **C3 [validation](../common/contracts/03_validation_contract.md)**, **C4 [connectivity & secrets](../common/contracts/04_connectivity_and_secrets_contract.md)**, **C5 [write-back](../common/contracts/05_write_back_contract.md)** — prerequisite contracts |
| **Contracts CONSUMED** | C1 (all tables; `common.schema` must mirror it), C6 (resource / Key Vault names), C7 (CLI + runtime defaults for `common.config`; job contract for `data_products.py`) |
| **Depends on** | nothing for the shared library — Wave 1, start immediately. `data_products.py` runtime depends on T4, T7, T8 outputs (codes against C1 meanwhile) |
| **Blocks** | live integration/merge of T3–T8 and T10 (they code against C2–C5 meanwhile) |

## Objective

1. Build the shared Python package `synapse/common/` and the two T-SQL helper procs that replace
   the SAS macro library and BTEQ audit inserts, exactly per contracts C2–C5 and the stubs in
   `synapse/common/contracts/stubs/`.
2. Port `sas/04_sas_data_products.sas` (golden-record assembly, `MODEL_VERSION = 'MASTER_V1.5'`)
   to `synapse/spark/data_products.py`.

**First step (contract freeze):** re-read C2–C5 against `sas/macros/*.sas` and the BTEQ audit
pattern. If you find a defect, fix the contract file(s) you own (and note the stub change needed)
in the **first commit** of your PR and call it out at the top of the PR description. Otherwise
state "C2–C5 v1.0 confirmed". Ship the library first (land it as early as possible — every Wave 2
ticket integrates against it).

## Source files (read-only references)

- `sas/macros/connect_teradata.sas` — 4 LIBNAMEs (`COREDB`, `TXNDB`, `STGDB`, `DPDB`), `{SAS004}` passwords, `logmech`
- `sas/macros/log_step.sas` — `%log_step(step=, status=, msg=, rowcount=)`, `%init_audit` → `WORK.PIPELINE_AUDIT`
- `sas/macros/validate_table.sas` — `%validate_table(lib=, table=, key_cols=, not_null=, min_rows=1)` → `&VALIDATION_RC`
- `bteq/01_stg_customer_360.bteq`, `bteq/02_stg_txn_summary.bteq`, `bteq/03_stg_risk_factors.bteq` — `ETL_RUN_LOG` insert blocks
- `sas/04_sas_data_products.sas` — golden record
- `sas/01_sas_customer_segments.sas`, `sas/02_sas_txn_analytics.sas`, `sas/03_sas_risk_scoring.sas` — the `DELETE` + `PROC APPEND` write-back pattern C5 replaces
- `config/pipeline_config.cfg` — runtime parameter defaults

## Target outputs

```
synapse/common/
├── config.py            # C4 §4, C7 — stub: contracts/stubs/config.pyi
├── keyvault.py          # C4 §2   — stub: contracts/stubs/keyvault.pyi
├── sql_connector.py     # C4 §3   — stub: contracts/stubs/sql_connector.pyi
├── audit.py             # C2      — stub: contracts/stubs/audit.pyi
├── validation.py        # C3      — stub: contracts/stubs/validation.pyi
├── writeback.py         # C5      — stub: contracts/stubs/writeback.pyi
├── schema.py            # C1 machine-readable — stub: contracts/stubs/schema.pyi
└── sql/
    ├── usp_log_step.sql        # C2 §3.1
    └── usp_validate_table.sql  # C3 §4.1
synapse/spark/data_products.py  # golden record job (C7 §3)
```
Public names and signatures must match the `.pyi` stubs exactly (a test should import each module
and compare `inspect.signature` against the stub). Update `synapse/common/README.md` usage notes.

## Translation requirements — shared library

| SAS / BTEQ | Target |
|---|---|
| `%connect_teradata(server=, username=, auth=)` + LIBNAMEs | `common.config` + `common.keyvault` + `common.sql_connector` (managed identity / Entra token; Key Vault convention C4 §2; schema constants `SCHEMA_CORE/TXN/STG/DP`) |
| `password="{SAS004}..."`, `logmech=LDAP` | removed — token auth only; no passwords anywhere |
| `proc sql; connect to teradata ...; execute (...) by teradata;` | `sql_connector.execute_sql(cfg, sql, params)` (pyodbc, ODBC Driver 18, access token) |
| LIBNAME reads (`from STGDB.STG_...`) | `sql_connector.read_table(spark, cfg, "SCHEMA.TABLE", ...)` (`synapsesql` on Synapse; JDBC / local Parquet/CSV modes for tests) |
| `%init_audit` / `%log_step` / `WORK.PIPELINE_AUDIT` | `audit.init_audit` / `audit.log_step` / `audit.audit_step` writing `ETL_STAGING_DB.ETL_RUN_LOG` (C2), console line `[PIPELINE] ...` |
| BTEQ `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG ...` | `ETL_STAGING_DB.usp_log_step` (C2 §3.1) |
| `%validate_table` + `&VALIDATION_RC` + `%abort cancel` | `validation.validate_dataframe` / `validate_sql_table` raising `ValidationError`; `ETL_STAGING_DB.usp_validate_table` with `THROW 50099/50100/50101` (C3). NOT NULL = warning only, as in SAS |
| `min_rows=1000` | `default_min_rows(env)` → 1000 UAT/PROD, 1 DEV (C3 §2) |
| `DELETE FROM DATA_PRODUCTS_DB.x` + `PROC APPEND ... FORCE` | `writeback.truncate_and_load` + `conform_to_contract` (C5); never `overwrite` mode |
| `COLLECT STATISTICS` pass-through | `UPDATE STATISTICS` via `execute_sql` |

`usp_validate_table` must build dynamic SQL with `QUOTENAME` and `sp_executesql` only.

## Translation requirements — `data_products.py` (`sas/04_sas_data_products.sas`)

| SAS | Spark requirement |
|---|---|
| `JOB_NAME` `04_MASTER_PROFILE`, `MODEL_VERSION = MASTER_V1.5` | constants; `TARGET_TABLE = "DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE"` |
| STEP 1 `WORK.BASE` from `STG_CUSTOMER_360` where `CUSTOMER_STATUS = 'A'`: `FULL_NAME = trim(FIRST_NAME) \|\| ' ' \|\| trim(LAST_NAME)`, `NUM_ACCOUNTS → TOTAL_ACCOUNTS`, `NUM_ACTIVE_ACCOUNTS → ACTIVE_ACCOUNTS`, plus `AGE, STATE_CODE, CUSTOMER_SINCE, TENURE_MONTHS, CUSTOMER_STATUS, TOTAL_BALANCE, TOTAL_CREDIT_LIMIT, CREDIT_UTILIZATION_PCT` | same (SAS `trim()` of a missing value gives blank, so the name is never NULL when one part is present — replicate with `coalesce(trim(x), '')`) |
| `WORK.SEGMENTS` from `CUSTOMER_SEGMENTS` (name, LTV, engagement, 3 flags) | `read_table` |
| `WORK.TXN` from `TRANSACTION_ANALYTICS` **where `EFFECTIVE_DATE = today()`**: `TOTAL_TRANSACTIONS → MONTHLY_TRANSACTIONS`, `TOTAL_DEBIT_AMT → MONTHLY_SPEND`, `NET_CASH_FLOW`, `TOP_SPEND_CATEGORY`, `DIGITAL_TXN_PCT` | filter `EFFECTIVE_DATE = cfg.as_of_date` |
| `WORK.RISK` from `CUSTOMER_RISK_SCORES` | `read_table` |
| STEP 2 sorted 4-way `MERGE ... by CUSTOMER_ID; if _base;` | left joins from BASE on `CUSTOMER_ID` (keys are unique per C3; assert it) |
| `if not _seg`: `SEGMENT_NAME='UNCLASSIFIED'`, LTV 0, engagement 0, flags `'N'` | same |
| `if not _txn`: transactions 0, spend 0, net cash 0, `TOP_SPEND_CATEGORY = ''`, digital 0 | same |
| `if not _risk`: `COMPOSITE_RISK_SCORE` NULL, `RISK_TIER = 'UNKNOWN'`, PD NULL, `WATCH_LIST_FLAG = 'N'` | same |
| `EFFECTIVE_DATE = today()`, `LOAD_TS = datetime()` | `cfg.as_of_date`, UTC now |
| STEP 3 data-quality report (segment / risk-tier distributions, completeness counts) | compute the same aggregates and write them as audit `MESSAGE` text (`BUILD` step) |
| STEP 4 `%validate_table(key_cols=CUSTOMER_ID, not_null=CUSTOMER_ID FULL_NAME CUSTOMER_STATUS, min_rows=1000)` | `validate_dataframe` (C3 §5) |
| STEP 5 `DELETE` + `PROC APPEND` | `truncate_and_load(cfg, df, TARGET_TABLE, JOB_NAME)` |
| STEP 6 `COLLECT STATISTICS` on master, segments, risk | `UPDATE STATISTICS` on those tables via `execute_sql` |
| STEP 7 print `WORK.PIPELINE_AUDIT` | `audit.get_run_log(cfg)` printed to driver log |

Reference: `data/03_sas_data_products/customer_master_profile.csv` (407 rows, as of 2026-04-10).

## Acceptance criteria

- [ ] All modules import and match the stub signatures exactly (signature-comparison test).
- [ ] Unit tests (local, no Azure): `keyvault.get_secret` env-var path + caching + no value logging; `audit` memory/stdout sinks, `audit_step` logs ERROR and re-raises; `validation` row-count / duplicate-key fail and NOT NULL warn-only semantics with `default_min_rows`; `writeback.conform_to_contract` column order/casts/missing-column error/extra-column drop for all four DP tables; `truncate_and_load` issues `TRUNCATE` vs. `DELETE ... WHERE` correctly and never uses overwrite (mock `execute_sql` and the writer).
- [ ] `common.schema` agrees with `01_schema_contract.md` (test parses the markdown).
- [ ] `usp_log_step` and `usp_validate_table` run in the local SQL Server container (port **14339**) against a C2-shaped `ETL_RUN_LOG`; error numbers 50010/50099/50100/50101 verified; identifiers are `QUOTENAME`d.
- [ ] `data_products.run()` on local PySpark with inputs served from the reference CSVs (`stg_customer_360.csv` + the three DP CSVs, with `EFFECTIVE_DATE = 2026-04-10`) yields **407** rows matching `customer_master_profile.csv` on all deterministic columns (within 0.01).
- [ ] `python synapse/spark/data_products.py --help` works; `sys.exit(main())` guard present.
- [ ] PR description states the contract-freeze outcome (confirmed or amended).

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T9] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_09/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_09/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
