# Snowflake Environment, Roles and Secret Management

Ticket: **MBA-2203 (TICKET-02)** — replaces the Teradata LDAP service account and
the `{SAS004}` passwords that used to live in `sas/macros/connect_teradata.sas`.

## What replaces what

| Teradata / SAS (legacy) | Snowflake / Azure (target) |
|---|---|
| Single Teradata system, shared AMPs | Three warehouses sized per workload, each with a resource monitor |
| One LDAP service account `svc_etl_pipeline` with read/write everywhere | Four functional roles (`RB_LOADER`, `RB_TRANSFORMER`, `RB_ANALYST`, `RB_ADMIN`) |
| `TD_LOGMECH=LDAP` username/password | Key-pair (`SNOWFLAKE_JWT`) service principals, plus Entra ID external OAuth |
| `{SAS004}` passwords in `connect_teradata.sas`, host in `pipeline_config.cfg` | Azure Key Vault secrets resolved at runtime (`scripts/keyvault.sh`, `snowflake/connection/snowflake_credentials.py`) |
| `%connect_teradata` LIBNAME macro | Synapse linked service `LS_Snowflake_RetailAnalytics` + `SnowflakeCredentials` |

## Warehouses

| Warehouse | Size | Auto-suspend | Clusters | Workload | Monitor |
|---|---|---|---|---|---|
| `WH_RB_ELT_<env>` | MEDIUM | 60 s | 1–2 (ECONOMY) | Staging transformations migrated from BTEQ | `RM_RB_ELT_<env>` |
| `WH_RB_SPARK_<env>` | LARGE | 120 s | 1–3 (STANDARD) | Synapse Spark read/write of the migrated SAS analytics | `RM_RB_SPARK_<env>` |
| `WH_RB_ADHOC_<env>` | SMALL | 60 s | 1–2 (ECONOMY) | Analyst / BI queries on data products | `RM_RB_ADHOC_<env>` |

Every warehouse is created suspended with auto-resume on, so an idle environment
costs nothing. Resource monitors notify at 75/90 % and suspend at 100 % of the
monthly credit quota configured in `config/snowflake_config.cfg`.

## Least-privilege grant matrix

`R` = read (SELECT), `W` = write (INSERT/UPDATE/DELETE/TRUNCATE), `C` = create
objects, `–` = no privilege at all.

| Role | `CORE_BANKING_DB` | `TXN_PROCESSING_DB` | `ETL_STAGING_DB` | `DATA_PRODUCTS_DB` | Warehouses |
|---|---|---|---|---|---|
| `RB_LOADER_<env>` | R | R | R + W + C | **–** | `WH_RB_ELT` (USAGE, OPERATE) |
| `RB_TRANSFORMER_<env>` | R | R | R + W + C | R + W + C | `WH_RB_ELT`, `WH_RB_SPARK` |
| `RB_ANALYST_<env>` | – | – | – | R | `WH_RB_ADHOC` (USAGE) |
| `RB_ADMIN_<env>` | – | – | MONITOR | MONITOR | MONITOR on all three |

Role hierarchy: `RB_LOADER`, `RB_TRANSFORMER` and `RB_ANALYST` are siblings
granted to `RB_ADMIN`, which is granted to `SYSADMIN`. No pipeline role inherits
another workload's privileges — in particular the loader has no grant of any
kind on the data product database, which `05_verify_least_privilege.sql`
asserts on every provisioning run.

Grants use `ALL`/`FUTURE SCHEMAS` and `ALL`/`FUTURE TABLES` so new objects
created by the migrated pipelines inherit the model without a manual grant.

## Service principals and authentication

| Principal | Default role | Auth | Key Vault secrets |
|---|---|---|---|
| `SVC_SYNAPSE_SPARK_<env>` | `RB_TRANSFORMER_<env>` | Key pair (`SNOWFLAKE_JWT`) | `snowflake-svc-synapse-private-key`, `-passphrase`, `-public-key` |
| `SVC_RB_LOADER_<env>` | `RB_LOADER_<env>` | Key pair (`SNOWFLAKE_JWT`) | `snowflake-svc-loader-private-key`, `-passphrase`, `-public-key` |
| `SVC_RB_ADMIN_<env>` | `ACCOUNTADMIN` (provisioning only) | Key pair (`SNOWFLAKE_JWT`) | `snowflake-svc-admin-private-key`, `-passphrase` |
| Human analysts | `RB_ANALYST_<env>` | Entra ID external OAuth (`RB_ENTRA_OAUTH_<env>`) | n/a — token-based |

All three service users are created with `TYPE = SERVICE`, which cannot hold a
password; `04_service_principals.sql` additionally runs `UNSET PASSWORD` so any
pre-existing user is stripped. `EXTERNAL_OAUTH_ANY_ROLE_MODE = 'DISABLE'` keeps
OAuth callers inside the role they were issued.

Key Vault itself is reached with the caller's Azure identity — the Synapse
workspace managed identity in Synapse, an interactive `az login` on a
workstation — so there is no bootstrap secret to store anywhere.

## Provisioning

```bash
source config/snowflake_config.cfg
./snowflake/admin/run_snowflake_admin.sh --env DEV --dry-run   # print the plan
./snowflake/admin/run_snowflake_admin.sh --env DEV             # apply
```

The runner applies `00`–`04` in order and then `05_verify_least_privilege.sql`.
All scripts are re-runnable: objects use `CREATE ... IF NOT EXISTS` followed by
an `ALTER` that re-applies desired state, and grants are idempotent.

## Credential rotation

Rotation is zero-downtime: Snowflake accepts two public keys per user, so the
new key is trusted before consumers switch and the old key is only removed once
nothing uses it. Default cadence is `SF_KEY_ROTATION_DAYS` (90 days), and the
same procedure is the break-glass response to a suspected key compromise (run
all four phases back to back).

| Phase | Command | Effect |
|---|---|---|
| 1. Generate | `scripts/rotate_snowflake_key.sh --user synapse --phase generate` | New 2048-bit key pair; private key, passphrase and public key stored as `*-pending` secrets in Key Vault. The private key never leaves memory except as a `0600` temp file that is shredded on exit. |
| 2. Publish | `... --phase publish` | Sets `RSA_PUBLIC_KEY_2` on the user. Both the old and new keys now authenticate. |
| 3. Cut over | `... --phase cutover` | Promotes the `*-pending` secrets to the canonical secret names. Synapse activities and Spark jobs pick up the new key on their next Key Vault read; running activities keep working on the old key. |
| 4. Retire | `... --phase retire` | Moves the new key into `RSA_PUBLIC_KEY`, unsets `RSA_PUBLIC_KEY_2` and deletes the `*-pending` secrets. The old key stops working here. |

Verification between phases 2 and 3 — run against the environment being
rotated, and only continue when it succeeds:

```bash
python -c "
from snowflake_credentials import SnowflakeCredentials
import snowflake.connector
c = SnowflakeCredentials.for_principal('synapse')
conn = snowflake.connector.connect(**c.connector_options(database='DATA_PRODUCTS_DB'))
print(conn.cursor().execute('SELECT CURRENT_USER(), CURRENT_ROLE()').fetchone())
"
```

Rotation was exercised end to end in DEV with `--dry-run` for the destructive
steps; see the PR for MBA-2203 for the validation notes.

### If a phase fails

* Failure in 1 or 2 — no consumer is affected; delete the `*-pending` secrets
  and start again.
* Failure in 3 — re-run `--phase cutover`; the old key is still published in
  slot 1, so authentication keeps working.
* Failure in 4 — re-run `--phase retire`. Do not delete `*-pending` secrets by
  hand until `DESC USER <user>` shows the new fingerprint in
  `RSA_PUBLIC_KEY_FP`.

## No credential literals in the repository

`scripts/scan_for_credentials.sh` fails the build on `{SAS004}` blobs, PEM
private key bodies, literal `password=`/`client_secret=` assignments, AWS keys
and Azure SAS tokens. Run it before pushing:

```bash
./scripts/scan_for_credentials.sh
```

Legacy Teradata assets kept for the parallel-run window (`bteq/`, `sas/`) now
read `TD_SERVER`, `TD_USERNAME`, `TD_LOGMECH` and `TD_PASSWORD` from Key Vault
via `load_teradata_credentials` in `config/pipeline_config.cfg`;
`%connect_teradata` aborts if those variables are absent instead of falling back
to a hardcoded host or an embedded password.
