# TICKET-03 — Source ingestion into ADLS Gen2 + `COPY INTO`

| | |
|---|---|
| **Ticket** | T3 |
| **Branch** | `migrate/ticket-03-ingestion` |
| **Owned paths** | `synapse/ingestion/**`, `synapse/tests/ticket_03/**` |
| **Contracts OWNED** | none (implements entry point `usp_load_source_tables` from C7 §2) |
| **Contracts CONSUMED** | C1 (source tables §3.1–3.6), C2 (`usp_log_step`), C3 (`usp_validate_table`), C4 §2/§5 (Key Vault names, landing paths, MI auth), C6 (containers), C7 (signature) |
| **Depends on** | **T2 + T9 contract freeze** (Wave 2). Runtime: T2 DDL (tables to load), T9 helper procs, T1 storage |
| **Blocks (runtime only)** | T4–T6 need loaded source tables for end-to-end runs |

## Objective

Replace "data already lives in Teradata" with an explicit landing zone: put the six source
extracts into the ADLS Gen2 `landing` container and load them into the dedicated SQL pool source
schemas with `COPY INTO`, as phase 0 of the daily pipeline. Also make and document the decision
on the dev data generator.

## Source files (read-only references)

- `export_data.py` **lines 64-80** — the six source tables and their column lists/order:
  `core_banking.customers`, `core_banking.accounts`, `core_banking.addresses`,
  `txn_processing.transactions`, `txn_processing.transaction_types`,
  `core_banking.customer_bureau_scores`, and the `source_exports` list mapping them to
  `customers.csv`, `accounts.csv`, `addresses.csv`, `transaction_types.csv`, `transactions.csv`,
  `customer_bureau_scores.csv`
- `data/01_source_tables/*.csv` — the six committed extracts (500 customers, 1,251 accounts,
  1,000 addresses, 8 transaction types, 80,528 transactions, 500 bureau scores; header row,
  lower-case column names, empty field = NULL, timestamps like `2026-04-10 11:28:08.100684`,
  quoted merchant names containing commas)
- `ddl/00_source_tables.sql` — source table definitions (target shape is C1)

## Target outputs

```
synapse/ingestion/
├── README.md                     # replace placeholder: flow, landing layout, how to run locally, run_demo decision
├── source_manifest.json          # 6 entries: schema, table, csv file name, ordered column list (from export_data.py 64-80 / C1)
├── upload_sources.py             # CLI: upload data/01_source_tables/*.csv to landing/source/<SCHEMA>/<TABLE>/load_date=<d>/ (DefaultAzureCredential)
└── usp_load_source_tables.sql    # ETL_STAGING_DB.usp_load_source_tables (C7 §2)
```

## Requirements

1. **Landing layout** exactly per C4 §5: `landing/source/<SCHEMA>/<TABLE>/load_date=<YYYY-MM-DD>/<file>.csv`.
2. **`usp_load_source_tables`** (signature frozen in C7 §2): for each requested table — `TRUNCATE` target, build and `EXEC sp_executesql` a `COPY INTO <SCHEMA>.<TABLE> (<explicit column list>) FROM 'https://<@StorageAccount>.dfs.core.windows.net/landing/source/<SCHEMA>/<TABLE>/load_date=<@LoadDate>/' WITH (FILE_TYPE = 'CSV', FIRSTROW = 2, FIELDQUOTE = '"', FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0A', ENCODING = 'UTF8', CREDENTIAL = (IDENTITY = 'Managed Identity'))` (COPY INTO does not accept variables for the path, hence dynamic SQL; quote all identifiers/literals safely), then `UPDATE STATISTICS`, then `usp_validate_table @MinRows = 1`.
3. Audit via `usp_log_step` with `JOB_NAME = '00_load_source_tables'`; one `LOAD` row per table with its row count, plus the `FULL_LOAD` START/SUCCESS/ERROR rows (C2 §4).
4. Column order in the manifest and COPY column lists must equal the CSV header order (which matches `export_data.py` lines 64-80 and C1).
5. `upload_sources.py`: args `--env`, `--load-date`, `--source-dir` (default `data/01_source_tables`), `--key-vault-name`; resolves the storage account from Key Vault secret `adls-account-name` (or `RBAP_SECRET_ADLS_ACCOUNT_NAME`); uses `azure-storage-file-datalake` + `DefaultAzureCredential`; `--dry-run` prints target paths without uploading. It may import `common.keyvault` (C4) **or** read the env var directly — do not reimplement Key Vault logic beyond that.
6. **`local/duckdb/run_demo` decision:** `export_data.py` imports `local/duckdb/run_demo.py`, which is **not present in the repo**, so the CSVs cannot currently be regenerated. Write a short decision record in `synapse/ingestion/README.md`: recommended position is that `run_demo` stays out of the Synapse pipeline as an optional dev-only generator, the committed `data/01_source_tables/*.csv` are the DEV/UAT seed, and `export_data.py` is left untouched. Flag it as an open question for the reviewer to confirm.

## Acceptance criteria

- [ ] `upload_sources.py --dry-run --env DEV --load-date 2026-04-10` prints exactly six correct landing paths.
- [ ] Local test (SQL Server container, port **14333**): since `COPY INTO` is Synapse-only, the test asserts the generated `COPY INTO` text (column lists, path, options) for all six tables, and separately loads the six CSVs with an equivalent local loader into C1-shaped tables, matching row counts 500 / 1,251 / 1,000 / 8 / 80,528 / 500.
- [ ] Manifest column lists equal the CSV headers and C1 (test).
- [ ] No storage keys / SAS tokens anywhere; MI auth only.
- [ ] `run_demo` decision recorded and surfaced in the PR description as an open question.

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T3] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_03/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_03/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
