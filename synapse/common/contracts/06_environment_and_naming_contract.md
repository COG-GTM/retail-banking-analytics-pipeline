# C6 — Environment & Azure resource naming

| | |
|---|---|
| **Owner** | [T1](../../tickets/TICKET-01.md) — implements it in Bicep under `synapse/infra/` |
| **Consumers** | T3 (storage/containers), T9 (Key Vault + secret names via C4), T10 (workspace, pools, linked services, artifacts path, trigger) |
| **Status** | **FROZEN v1.0** |
| **Replaces** | `config/pipeline_config.cfg` (single env file with `Environment: DEV \| UAT \| PROD` header) |

## 1. Environments

`DEV`, `UAT`, `PROD` (upper-case in config/parameters; lower-case `<env>` inside resource names).

## 2. Resource names

Globally-unique names (storage, Key Vault, workspace) are Bicep parameters whose defaults follow
the pattern below; consumers must resolve the *actual* values from Key Vault secrets (C4 §2) or
pipeline parameters, never hard-code them.

| Resource | Name / default | Notes |
|---|---|---|
| Resource group | `rg-rbap-<env>` | created outside or by a subscription-scope module (T1's choice) |
| Synapse workspace | `syn-rbap-<env>` | system-assigned MI; Entra admin = param `sqlEntraAdminObjectId` |
| Dedicated SQL pool | `rbapdw` | collation **`SQL_Latin1_General_CP1_CI_AS`** (case-insensitive = Teradata `NOT CASESPECIFIC`) |
| Apache Spark pool | `sprbap` | Spark **3.4**, autoscale, auto-pause |
| ADLS Gen2 account | `strbap<env><uniqueSuffix>` | HNS enabled; secret `adls-account-name` |
| Containers | `synapse` (workspace default FS), `landing` (C4 §5), `artifacts` (Spark job files + `common.zip`) | |
| Key Vault | `kv-rbap-<env>` | RBAC authorization mode; secrets per C4 §2 |
| Log Analytics | `log-rbap-<env>` | workspace diagnostics (replaces `LOG_DIR` + gzip rotation) |
| Linked services | `ls_keyvault`, `ls_adls`, `ls_sqlpool` | Synapse artifacts owned by T10 (C4 §6) |
| Spark artifacts path | `abfss://artifacts@<acct>.dfs.core.windows.net/spark/jobs/<job>.py`, `.../spark/common.zip` | uploaded by T10's deploy helper |

## 3. Suggested sizing (T1 may tune; parameterised per env)

| | DEV | UAT | PROD |
|---|---|---|---|
| Dedicated SQL pool SKU | `DW100c` | `DW200c` | `DW500c` |
| Spark node size / autoscale | Small, 3–5 | Small, 3–10 | Medium, 3–10 |
| Spark auto-pause (min) | 15 | 15 | 30 |

## 4. RBAC (granted by T1)

| Principal | Role | Scope |
|---|---|---|
| Workspace MI | Storage Blob Data Contributor | storage account |
| Workspace MI | Key Vault Secrets User | Key Vault |
| Deployment principal | Key Vault Secrets Officer | Key Vault (to write the C4 secrets) |
| Workspace MI | `db_owner` (or narrower) in `rbapdw` | created by a post-deploy SQL step documented by T1 (`CREATE USER [<workspace>] FROM EXTERNAL PROVIDER`) |

## 5. `config/pipeline_config.cfg` mapping

| Old variable | Target |
|---|---|
| `TD_SERVER`, `TD_USERNAME`, `TD_LOGMECH` | Key Vault `sql-dedicated-endpoint` / `sql-dedicated-database` + managed identity (C4) |
| `DB_CORE`, `DB_TXN`, `DB_STG`, `DB_DP` | schemas of the same names in `rbapdw` (C1) |
| `SAS_HOME`, `SAS_CONFIG`, `SAS_BATCH`, `SAS_AUTOEXEC` | retired (Spark pool `sprbap`) |
| `PIPELINE_HOME`, `BTEQ_DIR`, `SAS_DIR` | `artifacts` container + Synapse workspace artifacts |
| `LOG_DIR`, `ARCHIVE_DIR` | `ETL_RUN_LOG` (C2) + Log Analytics `log-rbap-<env>` |
| `RUN_DATE`, `RUN_TIMESTAMP` | pipeline `AsOfDate` / `@pipeline().RunId` (C7) |
| `LOOKBACK_MONTHS`, `RISK_SCORE_THRESHOLD`, `LOG_LEVEL` | runtime parameters (C7 §4) |
