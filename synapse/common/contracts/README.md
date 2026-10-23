# Shared contracts (frozen before dispatch)

These documents define every interface that crosses a ticket boundary, so the 10 ticket
sessions can work in parallel without talking to each other. Stub signatures (names and
parameters only, **no implementation**) are in [`stubs/`](stubs/).

| ID | Contract | Owner (implements + sole amender) | Consumers | Status |
|---|---|---|---|---|
| C1 | [Schema: schemas, tables, columns, types, distribution](01_schema_contract.md) | **T2** | T3–T10 | FROZEN v1.0 — **prerequisite** |
| C2 | [Audit logging: `ETL_RUN_LOG` + `usp_log_step` / `common.audit`](02_audit_logging_contract.md) | **T9** (T2 creates the table) | T3–T10 | FROZEN v1.0 — **prerequisite** |
| C3 | [Validation: row count / key uniqueness / NOT NULL](03_validation_contract.md) | **T9** | T3–T9 | FROZEN v1.0 — **prerequisite** |
| C4 | [Synapse SQL connector + Key Vault secret retrieval](04_connectivity_and_secrets_contract.md) | **T9** | T3, T7, T8, T9, T10 | FROZEN v1.0 — **prerequisite** |
| C5 | [Truncate-and-load write-back for `DATA_PRODUCTS_DB`](05_write_back_contract.md) | **T9** | T7, T8, T9 | FROZEN v1.0 — **prerequisite** |
| C6 | [Environment & Azure resource naming](06_environment_and_naming_contract.md) | T1 | T3, T9, T10 | FROZEN v1.0 |
| C7 | [Job entry points & runtime parameters](07_entry_points_contract.md) | T10 | T3–T9 (producers) | FROZEN v1.0 |

C1–C5 are the contracts the user scoped as shared; C6 and C7 were added so infra names and
orchestration entry points are also fixed up front.

## Stubs

| Stub | Module T9 will create | Contract |
|---|---|---|
| [`stubs/config.pyi`](stubs/config.pyi) | `synapse/common/config.py` | C4, C7 |
| [`stubs/keyvault.pyi`](stubs/keyvault.pyi) | `synapse/common/keyvault.py` | C4 |
| [`stubs/sql_connector.pyi`](stubs/sql_connector.pyi) | `synapse/common/sql_connector.py` | C4 |
| [`stubs/audit.pyi`](stubs/audit.pyi) | `synapse/common/audit.py` | C2 |
| [`stubs/validation.pyi`](stubs/validation.pyi) | `synapse/common/validation.py` | C3 |
| [`stubs/writeback.pyi`](stubs/writeback.pyi) | `synapse/common/writeback.py` | C5 |
| [`stubs/schema.pyi`](stubs/schema.pyi) | `synapse/common/schema.py` | C1 (machine-readable) |
| [`stubs/spark_job.pyi`](stubs/spark_job.pyi) | every `synapse/spark/*.py` job (T7, T8, T9) | C7 |
| [`stubs/sql_procedures.sql`](stubs/sql_procedures.sql) | all T-SQL procs (T3, T4–T6, T9) | C2, C3, C7 |

## Freeze and change process

1. Contracts are authored and frozen by the prep PR. **T2 and T9 own the prerequisite contracts (C1–C5)**; their sessions start by re-reading them against the Teradata/SAS sources and, if a defect is found, amend the contract **in the first commit of their own PR** and flag it in the PR description. Consumers must not start live integration until the T2/T9 freeze is confirmed (see [`../../tickets/PARALLELIZATION.md`](../../tickets/PARALLELIZATION.md)).
2. After that, contracts are immutable for the wave. A ticket that needs a change must **not** edit a contract it does not own and must not work around it silently: it codes to the frozen contract and writes a "Contract change request" section in its PR description.
3. Only the owning ticket edits its contract file. Nobody edits `stubs/` except the integrator (stubs mirror the contracts).
