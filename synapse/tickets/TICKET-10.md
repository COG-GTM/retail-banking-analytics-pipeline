# TICKET-10 — Synapse Pipelines orchestration + 02:00 ET daily trigger

| | |
|---|---|
| **Ticket** | T10 |
| **Branch** | `migrate/ticket-10-orchestration` |
| **Owned paths** | `synapse/pipelines/**`, `synapse/tests/ticket_10/**`, and (as contract owner) `synapse/common/contracts/07_entry_points_contract.md` |
| **Contracts OWNED** | C7 [entry points & runtime parameters](../common/contracts/07_entry_points_contract.md) |
| **Contracts CONSUMED** | C1 (post-run row counts), C2 (pipeline-level audit rows), C4 §6 (linked services), C6 (workspace, pools, containers, artifacts path) |
| **Depends on** | **T2 + T9 contract freeze** (Wave 2). Codes entirely against C7; end-to-end run needs all other tickets merged + a deployed workspace (T1) |
| **Blocks** | nothing |

## Objective

Replace the three shell orchestrators and the cron entry with Synapse Pipelines that run the
four phases with the same dependency rules, plus a schedule trigger at **02:00 ET daily**.

## Source files (read-only references)

- `orchestration/run_full_pipeline.sh` — phases (BTEQ → SAS → post-run validation), flags `--skip-bteq`, `--skip-sas`, `--dry-run`, abort-on-failure, row-count check of the 4 DP tables, elapsed-time summary, 30-day log gzip
- `bteq/run_bteq_pipeline.sh` — sequential `01_stg_customer_360` → `02_stg_txn_summary` → `03_stg_risk_factors`, fail on non-zero exit or `*** Failure`/`*** Error` in the log
- `sas/run_sas_pipeline.sh` — sequential `01_sas_customer_segments` → `02_sas_txn_analytics` → `03_sas_risk_scoring` → `04_sas_data_products`; rc ≥ 2 = failure, rc 1 = warning
- `docs/pipeline_flow.md` — Scheduling: cron `0 2 * * *`; timeline 02:00 BTEQ, 02:20 SAS, 03:00 validation, 03:05 products available
- `config/pipeline_config.cfg` — `RUN_DATE`, `RUN_TIMESTAMP`, `LOOKBACK_MONTHS`

## Target outputs (Synapse Git-integration layout)

```
synapse/pipelines/
├── README.md                              # replace placeholder: phases, params, deploy, per-env overrides, runbook
├── linkedService/ls_keyvault.json, ls_adls.json, ls_sqlpool.json          # C4 §6 (MI auth)
├── sparkJobDefinition/sjd_customer_segments.json, sjd_txn_analytics.json,
│                      sjd_risk_scoring.json, sjd_data_products.json        # C7 §3/§5; reference file common.zip
├── pipeline/pl_rbap_daily.json, pl_rbap_ingest.json, pl_rbap_staging.json,
│            pl_rbap_analytics.json, pl_rbap_post_validation.json           # C7 §5
├── trigger/tr_rbap_daily_0200_et.json
├── template-parameters-definition.json   # per-env overrides (Env, pool/KV/storage names) for workspace deployment
└── deploy/
    └── package_artifacts.sh               # zips synapse/common -> common.zip (no __pycache__), lists upload targets for spark/jobs/*.py
```

## Requirements

1. **`pl_rbap_daily`** parameters per C7 §5: `Env`, `AsOfDate`, `LookbackMonths` (12), `SkipIngest`, `SkipStaging` (≙ `--skip-bteq`), `SkipAnalytics` (≙ `--skip-sas`), `DryRun` (≙ `--dry-run`: only log the planned steps, run nothing). Compute an effective as-of date = `AsOfDate` if provided, else the trigger date converted to `Eastern Standard Time` (`convertFromUtc(..., 'Eastern Standard Time')`), and pass it to every activity. `RunId` = `@pipeline().RunId`.
2. **Phases and dependencies** (Execute Pipeline activities, each phase only on success of the previous; failure stops the run, as in `set -euo pipefail` + abort messages):
   - **Phase 0 – ingest** (`pl_rbap_ingest`, new): resolve `adls-account-name` from Key Vault (Web activity with MI, or a parameter) → Stored procedure `ETL_STAGING_DB.usp_load_source_tables(@RunId, @Env, @LoadDate, @StorageAccount)`.
   - **Phase 1 – staging** (`pl_rbap_staging`): `usp_stg_customer_360` → `usp_stg_txn_summary` (`@LookbackMonths`) → `usp_stg_risk_factors`, **sequential** like `run_bteq_pipeline.sh` (document that the three are data-independent and could run in parallel, but keep sequential by default).
   - **Phase 2 – analytics** (`pl_rbap_analytics`): SJD activities `sjd_customer_segments` → `sjd_txn_analytics` → `sjd_risk_scoring` → `sjd_data_products`, sequential like `run_sas_pipeline.sh` (`data_products` must wait for all three; document the optional parallel fan-in). Args per C7 §1/§3 (`--env --run-id --as-of-date [--key-vault-name]`).
   - **Phase 3 – post-run validation** (`pl_rbap_post_validation`): Script activity in **`Query`** mode (not `NonQuery`, otherwise no result rows) running the `UNION ALL` row-count query of the four `DATA_PRODUCTS_DB` tables from `run_full_pipeline.sh`; fail if any count is 0; log a `pl_rbap_daily` row via `usp_log_step` (`@Engine = 'PIPELINE'`) with elapsed time.
3. **Audit:** `pl_rbap_daily` logs `FULL_LOAD` START at the beginning and SUCCESS/ERROR at the end (`JOB_NAME = 'pl_rbap_daily'`, C2 §4) — failure path via an "on failure" branch.
4. **Trigger** `tr_rbap_daily_0200_et`: ScheduleTrigger, frequency Day, interval 1, `hours [2]`, `minutes [0]`, `timeZone: "Eastern Standard Time"` (DST-aware), starts `pl_rbap_daily`. Ship it **stopped**/disabled by default.
5. **Per-environment values:** a workspace-exported trigger/pipeline has one fixed value, so `Env`, Key Vault, pool and storage names must be overridable at deploy time via `template-parameters-definition.json` (document DEV/UAT/PROD values from C6).
6. **SJDs:** Spark pool `sprbap`, main file `abfss://artifacts@<acct>.dfs.core.windows.net/spark/jobs/<job>.py`, reference file `.../spark/common.zip`, conservative executor sizing for DEV. Note in the README that job files must end with `sys.exit(main())` or Synapse reports success on a no-op.
7. **Log retention:** the 30-day gzip in `run_full_pipeline.sh` is replaced by Log Analytics retention (T1) — document; no code.
8. JSON must be valid Synapse artifact JSON (name, properties, activities, dependsOn, typeProperties).

## Acceptance criteria

- [ ] All JSON files parse; a test in `synapse/tests/ticket_10/` validates: artifact names match C7 §5; every proc name/parameter and every SJD file path/argument matches C7 §2–§3; phase ordering/dependency conditions (`Succeeded`) as specified; post-run Script activity uses `Query`; trigger is daily 02:00 `Eastern Standard Time`.
- [ ] `--skip-bteq` / `--skip-sas` / `--dry-run` equivalents exist as parameters and are wired with If Condition activities.
- [ ] Linked services use managed identity (no keys/passwords/connection strings with secrets).
- [ ] `deploy/package_artifacts.sh` builds `common.zip` from `synapse/common/` excluding `__pycache__`/tests, and is shellcheck-clean.
- [ ] `synapse/pipelines/README.md` maps each old shell step/flag to its new activity/parameter and documents deployment + per-env overrides.
- [ ] No deployment to a live workspace (needs separate go-ahead).

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T10] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_10/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_10/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
