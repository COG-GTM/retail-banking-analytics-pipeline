# Azure Synapse Analytics Migration Target

This directory is the target for migrating the Teradata BTEQ + SAS 9.4 retail banking
pipeline (repo root: `bteq/`, `sas/`, `ddl/`, `orchestration/`) to **Azure Synapse
Analytics**. The original Teradata/SAS code stays in place, untouched, as the
reference implementation.

> **Status:** scaffolding, shared contracts and ticket briefs only. No migration logic
> has been written yet. The work is split into 10 tickets that are meant to be
> dispatched as 10 parallel sessions. See [`tickets/PARALLELIZATION.md`](tickets/PARALLELIZATION.md).

## Target architecture

```
                 ┌────────────────────────────── Azure Synapse workspace (syn-rbap-<env>) ─────────────────────────────┐
 source extracts │                                                                                                   │
 (CSV / Parquet) │   Synapse Pipelines  pl_rbap_daily  (schedule trigger 02:00 America/New_York)        [T10]       │
       │         │     Phase 0 ingest ─▶ Phase 1 staging ─▶ Phase 2 analytics ─▶ Phase 3 post-run validation        │
       ▼         │         │                  │                    │                         │                       │
 ┌────────────┐  │  ┌──────▼──────────────────▼────────┐   ┌───────▼────────────────┐        │                       │
 │ ADLS Gen2  │──┼─▶│ Dedicated SQL pool  (rbapdw)     │◀─▶│ Apache Spark pool       │        │                       │
 │ landing/   │  │  │  CORE_BANKING_DB / TXN_PROC...   │   │ (sprbap, Spark 3.4)     │        │                       │
 │ artifacts/ │  │  │  ETL_STAGING_DB  (T-SQL procs)   │   │ PySpark + Spark MLlib   │        │                       │
 └────────────┘  │  │  DATA_PRODUCTS_DB                │   │ jobs in synapse/spark/  │        │                       │
   [T1, T3]      │  │  [T2 DDL, T3 COPY INTO, T4-T6]   │   │ [T7, T8, T9]            │        │                       │
                 │  └──────────────────────────────────┘   └─────────────────────────┘        │                       │
                 │                    ▲  ETL_RUN_LOG audit, validation, write-back helpers [T9 synapse/common/]       │
                 └────────────────────┼──────────────────────────────────────────────────────────────────────────────┘
                          Azure Key Vault (kv-rbap-<env>)  — endpoints / names, accessed via workspace managed identity [T1]
```

| Layer | Teradata / SAS today | Synapse target | Owner |
|---|---|---|---|
| Landing zone | Operational Teradata DBs | ADLS Gen2 `landing` container + `COPY INTO` the source tables in the dedicated SQL pool | T1 (storage), T3 (ingestion) |
| Source / staging / product tables | `CORE_BANKING_DB`, `TXN_PROCESSING_DB`, `ETL_STAGING_DB`, `DATA_PRODUCTS_DB` databases | Same names as **schemas** in one dedicated SQL pool database; `DISTRIBUTION` + `CLUSTERED COLUMNSTORE INDEX` + `CREATE STATISTICS` | T2 |
| Staging transforms | 3 BTEQ scripts | 3 T-SQL stored procedures in `ETL_STAGING_DB` | T4, T5, T6 |
| Statistical / ML layer | SAS 9.4 (PROC STDIZE/FASTCLUS/RANK/MEANS/LOGISTIC, DATA step MERGE) | PySpark + Spark MLlib on an Apache Spark pool | T7, T8, T9 |
| Shared utilities | SAS macros `%connect_teradata`, `%log_step`, `%validate_table`; BTEQ `ETL_RUN_LOG` inserts | Python package `synapse/common/` + T-SQL helper procs `synapse/common/sql/` | T9 |
| Orchestration | `orchestration/run_full_pipeline.sh`, `bteq/run_bteq_pipeline.sh`, `sas/run_sas_pipeline.sh`, cron 02:00 | Synapse Pipelines + schedule trigger | T10 |
| Secrets / connections | `{SAS004}` encoded passwords, `TD_SERVER`/`TD_USERNAME` env vars | Azure Key Vault + workspace managed identity (Entra ID auth, no passwords in code) | T1 (vault), T9 (retrieval helper) |
| Infra | On-prem Teradata + SAS servers, `config/pipeline_config.cfg` | Bicep IaC, parameterised DEV / UAT / PROD | T1 |

## Directory layout and ownership

| Path | Purpose | Owning ticket |
|---|---|---|
| `infra/` | Bicep IaC for workspace, pools, ADLS Gen2, Key Vault, RBAC | T1 |
| `sql/ddl/` | Dedicated SQL pool DDL (schemas, tables, statistics) | T2 |
| `ingestion/` | Landing-zone upload + `COPY INTO` load procedure | T3 |
| `sql/staging/` | Staging stored procedures (one file per proc) | T4, T5, T6 (one file each) |
| `spark/` | PySpark job entry points (one file per job) | T7, T8, T9 (one file each) |
| `common/` | Shared Python utilities + T-SQL helper procs | T9 |
| `common/contracts/` | Frozen cross-ticket interface contracts + stub signatures | see [`common/contracts/README.md`](common/contracts/README.md) |
| `pipelines/` | Synapse workspace artifacts: pipelines, triggers, linked services, Spark job definitions | T10 |
| `tests/ticket_NN/` | Per-ticket tests (each ticket only writes its own folder) | ticket NN |
| `tickets/` | Ticket briefs + dispatch manifest | prep (read-only for tickets) |

## Tickets

| # | Ticket | Owning directory / files | Status |
|---|---|---|---|
| [T1](tickets/TICKET-01.md) | Provision Synapse workspace + infra as IaC (DEV/UAT/PROD) | `synapse/infra/` | Not started |
| [T2](tickets/TICKET-02.md) | Port Teradata DDL to dedicated SQL pool T-SQL | `synapse/sql/ddl/` | Not started |
| [T3](tickets/TICKET-03.md) | Source ingestion into ADLS Gen2 + `COPY INTO` | `synapse/ingestion/` | Not started |
| [T4](tickets/TICKET-04.md) | `01_stg_customer_360.bteq` → T-SQL stored procedure | `synapse/sql/staging/usp_stg_customer_360.sql` | Not started |
| [T5](tickets/TICKET-05.md) | `02_stg_txn_summary.bteq` → T-SQL stored procedure | `synapse/sql/staging/usp_stg_txn_summary.sql` | Not started |
| [T6](tickets/TICKET-06.md) | `03_stg_risk_factors.bteq` → T-SQL stored procedure | `synapse/sql/staging/usp_stg_risk_factors.sql` | Not started |
| [T7](tickets/TICKET-07.md) | `01_sas_customer_segments.sas` → PySpark + MLlib KMeans | `synapse/spark/customer_segments.py` | Not started |
| [T8](tickets/TICKET-08.md) | `02_sas_txn_analytics.sas` + `03_sas_risk_scoring.sas` → PySpark + MLlib | `synapse/spark/txn_analytics.py`, `synapse/spark/risk_scoring.py` | Not started |
| [T9](tickets/TICKET-09.md) | `04_sas_data_products.sas` golden record + shared macros → `synapse/common/` | `synapse/common/`, `synapse/spark/data_products.py` | Not started |
| [T10](tickets/TICKET-10.md) | Synapse Pipelines orchestration + 02:00 ET trigger | `synapse/pipelines/` | Not started |

Ticket sessions must **not** edit this README, `tickets/`, or contracts they do not own;
status is updated by the integrator when each ticket's PR merges.

## Shared contracts

All cross-ticket interfaces are frozen in [`common/contracts/`](common/contracts/README.md)
before dispatch. **T2 (schema) and T9 (audit, validation, connectivity, write-back) are
the prerequisite contract owners**; every other ticket codes against those contracts.

## Conventions (all tickets)

- Never modify anything outside `synapse/`. Original `bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `data/` and `export_data.py` are read-only references.
- Every staging proc and Spark job accepts an explicit **as-of date** instead of reading `CURRENT_DATE`/`today()` directly (see contract C7). The committed reference CSVs in `data/` were generated as of **2026-04-10**.
- All `LOAD_TS` / audit timestamps are UTC (`SYSUTCDATETIME()` in T-SQL, `current_timestamp()` with session time zone `UTC` in Spark).
- Python: Synapse Spark 3.4 runtime (Python 3.10). Jobs end with `if __name__ == "__main__": sys.exit(main())`.
- Never commit `__pycache__/`, `.venv/`, credentials, or generated data (see `synapse/.gitignore`).
