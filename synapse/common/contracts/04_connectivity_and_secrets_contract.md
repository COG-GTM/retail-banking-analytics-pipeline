# C4 — Synapse SQL connector + Key Vault secret-retrieval convention

| | |
|---|---|
| **Owner** | [T9](../../tickets/TICKET-09.md) — implements `synapse/common/config.py`, `synapse/common/keyvault.py`, `synapse/common/sql_connector.py` |
| **Depends on** | C6 (resource / Key Vault / secret names, owned by T1) |
| **Consumers** | T7, T8, T9 (reads + write-back), T3 (ingestion: landing paths, storage name, COPY INTO auth), T10 (linked services) |
| **Status** | **FROZEN v1.0** (prerequisite contract) |
| **Replaces** | `sas/macros/connect_teradata.sas` (4 LIBNAMEs with `{SAS004}` encoded passwords, `logmech=LDAP`), explicit `connect to teradata (...)` pass-through blocks in every SAS program, `TD_SERVER`/`TD_USERNAME`/`TD_LOGMECH` in `config/pipeline_config.cfg`, BTEQ `.LOGON` |

## 1. Authentication rules

- **Microsoft Entra ID only.** On Synapse, code authenticates as the **workspace system-assigned managed identity**. No SQL logins/passwords in code, config, notebooks, or pipeline JSON.
- Locally / in CI, code uses `azure.identity.DefaultAzureCredential`, or env-var overrides (§3) for tests that never touch Azure.
- The workspace SQL admin password (needed only to create the workspace) is a T1 `@secure()` Bicep parameter stored in Key Vault as `synapse-sql-admin-password`; **no pipeline code reads it**.

## 2. Key Vault convention

- Vault: `kv-rbap-<env>` (lower-case env), URL `https://kv-rbap-<env>.vault.azure.net/`. Because vault names are global, the name is overridable via `RBAP_KEY_VAULT_NAME` env var / `--key-vault-name` job arg / pipeline parameter.
- Synapse linked service name: `ls_keyvault`.
- Secret names (Key Vault allows `[0-9a-zA-Z-]`). These hold **non-credential connection settings** so every consumer resolves them from one place:

| Secret name | Example value (DEV) | Written by | Read by |
|---|---|---|---|
| `sql-dedicated-endpoint` | `syn-rbap-dev.sql.azuresynapse.net` | T1 (Bicep output) | T9 `sql_connector`, T3 upload script, T10 |
| `sql-dedicated-database` | `rbapdw` | T1 | same |
| `adls-account-name` | `strbapdevxxxxxx` | T1 | T3, T9, T10 |
| `adls-landing-container` | `landing` | T1 | T3 |
| `adls-artifacts-container` | `artifacts` | T1 | T10 |
| `synapse-sql-admin-password` | (secret) | T1 | **nobody** at runtime |
| `alert-webhook-url` | (optional) | manual | T10 failure notification |

### 2.1 Retrieval — `common.keyvault.get_secret(cfg, name)` (stub [`stubs/keyvault.pyi`](stubs/keyvault.pyi))

Resolution order:
1. Environment variable `RBAP_SECRET_<NAME>` where `<NAME>` = secret name upper-cased with `-` → `_` (e.g. `RBAP_SECRET_SQL_DEDICATED_ENDPOINT`). Used by local tests.
2. On Synapse: `mssparkutils.credentials.getSecret(cfg.key_vault_name, name, "ls_keyvault")`.
3. Elsewhere: `azure.keyvault.secrets.SecretClient(cfg.key_vault_url, DefaultAzureCredential()).get_secret(name).value`.

Values are cached per process and **never logged** (log only the secret name).

## 3. SQL connectivity — `common.sql_connector` (stub [`stubs/sql_connector.pyi`](stubs/sql_connector.pyi))

| Function | Purpose | Mechanism on Synapse | Local / test mechanism |
|---|---|---|---|
| `read_table(spark, cfg, table, columns=None, where=None)` | Read a contract table (`"SCHEMA.TABLE"`) into a DataFrame. Replaces LIBNAME reads (`from STGDB.STG_CUSTOMER_360`). | Dedicated SQL pool Spark connector: `spark.read.synapsesql("<db>.<SCHEMA>.<TABLE>")` | `cfg.sql_mode == "jdbc"`: Spark JDBC against SQL Server; `"local"`: reads `cfg.local_table_dir/<SCHEMA>.<TABLE>/` (Parquet) or the reference CSVs |
| `execute_sql(cfg, sql, params=None) -> int` | Run a non-query statement (TRUNCATE, DELETE, UPDATE STATISTICS, audit INSERT). Replaces `execute (...) by teradata`. | `pyodbc` + "ODBC Driver 18 for SQL Server", access token from `mssparkutils.credentials.getToken("DW")` passed via `SQL_COPT_SS_ACCESS_TOKEN` (1256) | `pyodbc` with `DefaultAzureCredential` token (scope `https://database.windows.net/.default`) or `RBAP_SQL_CONN_STR` (tests, e.g. SQL Server container) |
| `query_sql(cfg, sql, params=None) -> list[tuple]` | Small result sets (row counts, audit trail). | same | same |
| `get_connection(cfg)` | Context manager yielding a pyodbc connection. | same | same |

Rules:
- Always parameterise values (`?` placeholders); quote identifiers with `[...]`.
- The Spark connector **cannot** run arbitrary SQL; any TRUNCATE/DELETE/DDL goes through `execute_sql`.
- **Never** write with `.synapsesql(...)` in `overwrite` mode (it drops and re-creates the table, losing HASH/CCI/statistics). Writes go through C5 only.
- Jobs call these as module attributes (`from common import sql_connector as sqlc; sqlc.read_table(...)`) so tests can monkeypatch a single seam.

## 4. Configuration object — `common.config.load_config(...)` (stub [`stubs/config.pyi`](stubs/config.pyi))

Builds an immutable `PipelineConfig` from: CLI args (C7) → env vars (`RBAP_*`) → per-env defaults
(C6/C7). Fields include `env`, `run_id`, `as_of_date`, `key_vault_name`, `key_vault_url`,
`sql_endpoint`, `sql_database`, `storage_account`, `lookback_months`, `risk_score_threshold`,
`log_level`, `sql_mode`, `audit_sink`. SQL endpoint / database / storage account are resolved
lazily through `get_secret` (so config loads without Azure access in unit tests).

## 5. Ingestion / landing-zone conventions (T3)

- Container: `landing` in the storage account named by secret `adls-account-name`.
- Path per source table and load date:
  `landing/source/<SCHEMA>/<TABLE>/load_date=<YYYY-MM-DD>/<file>.csv`
  where `<file>` is the lower-case export name used by `export_data.py` / `data/01_source_tables/`
  (`customers`, `accounts`, `addresses`, `transaction_types`, `transactions`, `customer_bureau_scores`).
  Example: `https://<acct>.dfs.core.windows.net/landing/source/CORE_BANKING_DB/CUSTOMERS/load_date=2026-04-10/customers.csv`.
- File format: CSV, UTF-8, header row, comma delimiter, `"` quote, empty field = NULL, dates `YYYY-MM-DD`, timestamps `YYYY-MM-DD HH:MM:SS[.ffffff]`.
- `COPY INTO` authenticates with `CREDENTIAL = (IDENTITY = 'Managed Identity')` (workspace MI has *Storage Blob Data Contributor*, granted by T1). No SAS tokens or account keys.

## 6. Synapse linked services (implemented by T10 in `synapse/pipelines/linkedService/`)

| Name | Type | Auth |
|---|---|---|
| `ls_keyvault` | Azure Key Vault | workspace MI |
| `ls_adls` | ADLS Gen2 | workspace MI |
| `ls_sqlpool` | Azure Synapse dedicated SQL pool (`rbapdw`) | workspace MI |
