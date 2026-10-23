# TICKET-01 — Provision Synapse workspace + infrastructure as code

| | |
|---|---|
| **Ticket** | T1 |
| **Branch** | `migrate/ticket-01-infra` |
| **Owned paths** | `synapse/infra/**`, `synapse/tests/ticket_01/**` |
| **Contracts OWNED** | C6 [environment & naming](../common/contracts/06_environment_and_naming_contract.md) |
| **Contracts CONSUMED** | C4 §2 [Key Vault secret names](../common/contracts/04_connectivity_and_secrets_contract.md) (T1 *writes* these secrets), C1 §2 (database collation requirement) |
| **Depends on** | nothing — Wave 1, can start immediately |
| **Blocks (runtime only)** | live deployment of T3, T10; nobody is blocked for coding |

## Objective

Author Bicep templates that provision the complete Azure Synapse target platform for **DEV, UAT
and PROD**, parameterised per environment, replacing the on-prem Teradata + SAS servers and the
single-file `config/pipeline_config.cfg`.

## Source files (read-only references)

- `config/pipeline_config.cfg` — env header `DEV | UAT | PROD`, `TD_SERVER`, `DB_*`, `SAS_*`, paths, `LOOKBACK_MONTHS=12`, `RISK_SCORE_THRESHOLD=700`, `LOG_LEVEL`
- `README.md` (Prerequisites section: Teradata TTU, SAS 9.4 M7, bash)
- `docs/pipeline_flow.md` (scheduling / timeline — informs sizing only)

## Target outputs

```
synapse/infra/
├── README.md                    # replace the placeholder: how to build, what-if, deploy; post-deploy steps
├── main.bicep                   # entry point (resource-group scope)
├── modules/
│   ├── storage.bicep            # ADLS Gen2 (HNS) + containers synapse, landing, artifacts
│   ├── keyvault.bicep           # RBAC-mode vault + C4 §2 config secrets
│   ├── synapse.bicep            # workspace (system MI), firewall, Entra admin, dedicated SQL pool, Spark pool
│   ├── monitoring.bicep         # Log Analytics + diagnostic settings
│   └── rbac.bicep               # role assignments from C6 §4
├── params/
│   ├── dev.bicepparam
│   ├── uat.bicepparam
│   └── prod.bicepparam
└── scripts/
    └── post_deploy_grant.sql    # CREATE USER [<workspace>] FROM EXTERNAL PROVIDER; role membership in rbapdw
```
(File split inside `synapse/infra/` is your call; the resource names and outputs are not.)

## Requirements

1. Names, containers, pools, Key Vault and linked-service-relevant values exactly per **C6 §2**; globally-unique names are parameters with the C6 defaults.
2. Dedicated SQL pool `rbapdw` with collation **`SQL_Latin1_General_CP1_CI_AS`** (preserves Teradata `NOT CASESPECIFIC` comparisons the staging SQL relies on, e.g. `LIKE '%NSF%'`).
3. Spark pool `sprbap`: Spark **3.4**, autoscale + auto-pause per C6 §3.
4. Per-env sizing in `params/*.bicepparam` (C6 §3 suggested defaults).
5. Key Vault: write the non-credential secrets from **C4 §2** (`sql-dedicated-endpoint`, `sql-dedicated-database`, `adls-account-name`, `adls-landing-container`, `adls-artifacts-container`) from Bicep outputs. The workspace SQL admin password is a `@secure()` parameter (stored as `synapse-sql-admin-password`), never committed and never defaulted.
6. RBAC per **C6 §4** (workspace MI → Storage Blob Data Contributor + Key Vault Secrets User). Document the in-database grant (`post_deploy_grant.sql`) since Bicep cannot create database users.
7. Outputs: workspace name, SQL endpoint, dev endpoint, storage account name, Key Vault name/URI, Spark pool name — consumed by T10's deploy helper.
8. No secrets, subscription IDs, tenant IDs or object IDs hard-coded; use parameters.
9. Map every variable of `config/pipeline_config.cfg` to its new home (table in `synapse/infra/README.md`, consistent with C6 §5).

## Acceptance criteria

- [ ] `bicep build synapse/infra/main.bicep` (or `az bicep build`) succeeds with **zero errors**; `bicep lint` warnings addressed or justified in the README.
- [ ] `bicep build-params` succeeds for all three `params/*.bicepparam` files.
- [ ] Resource names, containers, pool names, collation, Spark version and secret names match C6 / C4 exactly (a test in `synapse/tests/ticket_01/` greps the compiled ARM JSON for them).
- [ ] No plaintext secrets anywhere in the diff.
- [ ] `synapse/infra/README.md` documents build, `what-if`, deploy, post-deploy grant, and the `pipeline_config.cfg` mapping.
- [ ] Nothing is actually deployed to Azure (deployment needs a separate, explicit go-ahead because it creates billable resources).

## Out of scope

Synapse workspace *artifacts* (linked services, pipelines, triggers, SJDs) — T10. Table DDL — T2.

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T1] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_01/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_01/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
