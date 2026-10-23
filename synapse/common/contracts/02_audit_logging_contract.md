# C2 — Audit logging contract (`ETL_RUN_LOG` + logging helpers)

| | |
|---|---|
| **Owner** | [T9](../../tickets/TICKET-09.md) — implements `synapse/common/audit.py` and `synapse/common/sql/usp_log_step.sql` |
| **Co-implementer** | [T2](../../tickets/TICKET-02.md) creates the `ETL_STAGING_DB.ETL_RUN_LOG` table with exactly the shape in §2 (DDL lives in `synapse/sql/ddl/`) |
| **Consumers** | T3, T4, T5, T6 (SQL helper); T7, T8, T9 (Python helper); T10 (pipeline-level rows + monitoring) |
| **Status** | **FROZEN v1.0** (prerequisite contract) |
| **Replaces** | BTEQ `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG (JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS)` at the end of every `bteq/*.bteq`; SAS `%init_audit` / `%log_step` (`sas/macros/log_step.sas`) writing `WORK.PIPELINE_AUDIT (JOB_NAME, STATUS, MESSAGE, ROW_COUNT, LOG_TS)` |

## 1. Design

One audit table for both engines. SAS used a session-local `WORK.PIPELINE_AUDIT` dataset
(lost at session end) and BTEQ wrote `ETL_RUN_LOG`; the target unifies both into the
persistent `ETL_STAGING_DB.ETL_RUN_LOG`, correlated by `RUN_ID` (= Synapse pipeline run id).

## 2. Table shape — `ETL_STAGING_DB.ETL_RUN_LOG` (DISTRIBUTION = ROUND_ROBIN, HEAP)

The first six business columns keep the BTEQ names and order.

```
LOG_ID      BIGINT IDENTITY(1,1) NOT NULL   -- surrogate; dedicated-pool IDENTITY is unique but not gap-free/ordered
RUN_ID      VARCHAR(64)    NOT NULL         -- Synapse @pipeline().RunId, or a uuid4 for ad-hoc/local runs
JOB_NAME    VARCHAR(100)   NOT NULL         -- see §4
STEP_NAME   VARCHAR(100)   NOT NULL         -- see §4
STATUS      VARCHAR(10)    NOT NULL         -- START | SUCCESS | WARNING | ERROR
ROW_COUNT   BIGINT         NULL
START_TS    DATETIME2(6)   NOT NULL         -- UTC
END_TS      DATETIME2(6)   NULL             -- UTC; NULL for START rows
MESSAGE     VARCHAR(4000)  NULL             -- free text (SAS msg=); error text on ERROR (truncate to 4000)
ENGINE      VARCHAR(10)    NOT NULL         -- SQL | SPARK | PIPELINE
ENV         VARCHAR(10)    NOT NULL         -- DEV | UAT | PROD
AS_OF_DATE  DATE           NULL             -- business date of the run (C7)
LOG_TS      DATETIME2(6)   NOT NULL         -- UTC insert time (supplied by the helper; no function defaults)
```

## 3. Helper interfaces

### 3.1 T-SQL — `ETL_STAGING_DB.usp_log_step` (file `synapse/common/sql/usp_log_step.sql`)

```
ETL_STAGING_DB.usp_log_step
    @RunId      VARCHAR(64),
    @JobName    VARCHAR(100),
    @StepName   VARCHAR(100),
    @Status     VARCHAR(10),            -- START | SUCCESS | WARNING | ERROR
    @RowCount   BIGINT        = NULL,
    @Message    VARCHAR(4000) = NULL,
    @StartTs    DATETIME2(6)  = NULL,   -- NULL -> SYSUTCDATETIME()
    @EndTs      DATETIME2(6)  = NULL,   -- NULL -> SYSUTCDATETIME() unless @Status = 'START'
    @Env        VARCHAR(10)   = 'DEV',
    @AsOfDate   DATE          = NULL,
    @Engine     VARCHAR(10)   = 'SQL'
```
Behaviour: validates `@Status` (invalid → `THROW 50010`), inserts exactly one row, sets
`LOG_TS = SYSUTCDATETIME()`. Insert failures propagate (BTEQ also exited on a failed log insert).

### 3.2 Python — `common.audit` (file `synapse/common/audit.py`, stub [`stubs/audit.pyi`](stubs/audit.pyi))

```python
log_step(cfg, job_name, step_name, status, row_count=None, message=None, start_ts=None, end_ts=None) -> None
audit_step(cfg, job_name, step_name) -> ContextManager[AuditHandle]   # START on enter, SUCCESS/ERROR on exit
init_audit(cfg) -> None                                                # replaces %init_audit: checks ETL_RUN_LOG is reachable
get_run_log(cfg, run_id=None) -> list[dict]                            # replaces the SAS "Pipeline Audit Trail" PROC PRINT
```
- Inserts go through `common.sql_connector.execute_sql` (pyodbc, parameterised) — **not** the Spark connector.
- Every call also prints the SAS-style console line `[PIPELINE] <utc-ts> | <job> | <step> | <status> | rows=<n> | <msg>` so it shows up in Spark driver logs.
- Sink is selected by `cfg.audit_sink`: `"sql"` (default on Synapse), `"stdout"` (print only), `"memory"` (append to `cfg.audit_records`; for unit tests). Set locally with env var `RBAP_AUDIT_SINK`.
- `audit_step` re-raises the original exception after logging `ERROR` with `message = f"{type(e).__name__}: {e}"`.

## 4. Naming rules for `JOB_NAME` / `STEP_NAME`

| Job | `JOB_NAME` | `ENGINE` | Owner |
|---|---|---|---|
| Source load | `00_load_source_tables` | SQL | T3 |
| Customer 360 staging | `01_stg_customer_360` | SQL | T4 |
| Txn summary staging | `02_stg_txn_summary` | SQL | T5 |
| Risk factor staging | `03_stg_risk_factors` | SQL | T6 |
| Customer segments | `01_CUSTOMER_SEG` | SPARK | T7 |
| Txn analytics | `02_TXN_ANALYTICS` | SPARK | T8 |
| Risk scoring | `03_RISK_SCORING` | SPARK | T8 |
| Golden record | `04_MASTER_PROFILE` | SPARK | T9 |
| Master pipeline | `pl_rbap_daily` | PIPELINE | T10 |

(Names are the BTEQ `JOB_NAME` literals and the SAS `step=` values, unchanged.)

Required rows per job run:
1. `STEP_NAME = 'FULL_LOAD'`, `STATUS = 'START'` at the beginning.
2. `STEP_NAME = 'FULL_LOAD'`, `STATUS = 'SUCCESS'`, `ROW_COUNT` = rows written to the job's target table, at the end — **or** `STATUS = 'ERROR'` with the error text before re-throwing.

Optional intermediate steps use UPPER_SNAKE names from this list where applicable:
`EXTRACT`, `FEATURES`, `MODEL`, `SCORE`, `BUILD`, `VALIDATE`, `LOAD`, `UPDATE_STATS`, `CLEANUP`.
`VALIDATE` rows are written by the C3 helpers themselves; `LOAD` rows by the C5 helper.
