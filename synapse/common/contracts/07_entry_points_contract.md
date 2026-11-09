# C7 — Job entry points & runtime parameters

| | |
|---|---|
| **Owner** | [T10](../../tickets/TICKET-10.md) (orchestrator — the consumer of every entry point). Producers implement the signatures below exactly. |
| **Producers** | T3 (`usp_load_source_tables`), T4–T6 (staging procs), T7–T9 (Spark jobs), T9 (`common.config` defaults) |
| **Status** | **FROZEN v1.0** |
| **Replaces** | `orchestration/run_full_pipeline.sh` flags (`--skip-bteq`, `--skip-sas`, `--dry-run`), `envsubst < script.bteq \| bteq`, `sas -sysin ...`, `CURRENT_DATE` / `today()` in every script |

## 1. Common parameters

| Parameter | T-SQL | Spark CLI | Meaning |
|---|---|---|---|
| Run id | `@RunId VARCHAR(64)` | `--run-id` (required) | `@pipeline().RunId`; written to `ETL_RUN_LOG.RUN_ID` |
| Environment | `@Env VARCHAR(10)` | `--env {DEV,UAT,PROD}` (required) | |
| As-of date | `@AsOfDate DATE = NULL` | `--as-of-date YYYY-MM-DD` | **Replaces every `CURRENT_DATE` / `today()`**. T10 always passes it explicitly = trigger date in `America/New_York`. `NULL`/omitted → `CAST(SYSUTCDATETIME() AS DATE)` (T-SQL) / today in `America/New_York` (Spark). Parity tests use `2026-04-10`. |
| Key Vault | — | `--key-vault-name` (optional, default `kv-rbap-<env>`) | C4 §2 |

## 2. Stored procedures (all in schema `ETL_STAGING_DB`, one file each)

```
-- T3: synapse/ingestion/usp_load_source_tables.sql
ETL_STAGING_DB.usp_load_source_tables
    @RunId VARCHAR(64), @Env VARCHAR(10), @LoadDate DATE,
    @StorageAccount VARCHAR(64),          -- resolved by T10 from Key Vault 'adls-account-name'
    @Tables VARCHAR(4000) = NULL          -- optional comma list of SCHEMA.TABLE; NULL = all six

-- T4: synapse/sql/staging/usp_stg_customer_360.sql
ETL_STAGING_DB.usp_stg_customer_360   @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL

-- T5: synapse/sql/staging/usp_stg_txn_summary.sql
ETL_STAGING_DB.usp_stg_txn_summary    @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL,
                                      @LookbackMonths INT = 12

-- T6: synapse/sql/staging/usp_stg_risk_factors.sql
ETL_STAGING_DB.usp_stg_risk_factors   @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL

-- T9 helpers (C2, C3): synapse/common/sql/
ETL_STAGING_DB.usp_log_step, ETL_STAGING_DB.usp_validate_table
```
Success = normal completion. Failure = `THROW` (after logging `ERROR` via `usp_log_step`); the
pipeline activity then fails. No reliance on return codes. Each file is re-runnable
(`IF OBJECT_ID(...) IS NOT NULL DROP PROCEDURE ...; GO; CREATE PROCEDURE ...`).

## 3. Spark jobs (`synapse/spark/`, one file each)

| File | Audit job | Job-specific args (defaults) | Owner |
|---|---|---|---|
| `customer_segments.py` | `01_CUSTOMER_SEG` | `--kmeans-restarts 10`, `--seed 42` | T7 |
| `txn_analytics.py` | `02_TXN_ANALYTICS` | `--reporting-period YYYY-MM` (default = month of as-of date) | T8 |
| `risk_scoring.py` | `03_RISK_SCORING` | `--risk-score-threshold 700` | T8 |
| `data_products.py` | `04_MASTER_PROFILE` | — | T9 |

Every job module exposes (see [`stubs/spark_job.pyi`](stubs/spark_job.pyi)):
```python
def build_arg_parser() -> argparse.ArgumentParser
def run(spark, cfg, **job_params) -> int        # pure-ish core; returns rows written; unit-testable
def main(argv: list[str] | None = None) -> int  # parses args, load_config(), gets SparkSession, calls run(); 0 on success
if __name__ == "__main__":
    sys.exit(main())                            # REQUIRED: Synapse runs the file as __main__
```
Spark session time zone is set to `UTC` by `main`. Jobs import shared code only as `common.*`
(`common.zip` reference file on Synapse; `synapse/` on `PYTHONPATH` locally).

## 4. Runtime parameter values

| Parameter | DEV | UAT | PROD | Source |
|---|---|---|---|---|
| `LookbackMonths` | 12 | 12 | 12 | `LOOKBACK_MONTHS` in `config/pipeline_config.cfg` |
| `RiskScoreThreshold` | 700 | 700 | 700 | `RISK_SCORE_THRESHOLD` |
| `LogLevel` | INFO | INFO | INFO | `LOG_LEVEL` |
| Spark `min_rows` | 1 | 1000 | 1000 | C3 §2 |

## 5. Synapse artifact names (T10)

| Artifact | Name |
|---|---|
| Master pipeline | `pl_rbap_daily` |
| Phase pipelines | `pl_rbap_ingest`, `pl_rbap_staging`, `pl_rbap_analytics`, `pl_rbap_post_validation` |
| Schedule trigger | `tr_rbap_daily_0200_et` (daily 02:00, time zone `Eastern Standard Time` = America/New_York incl. DST) |
| Spark job definitions | `sjd_customer_segments`, `sjd_txn_analytics`, `sjd_risk_scoring`, `sjd_data_products` |
| `pl_rbap_daily` parameters | `Env`, `AsOfDate` (blank → trigger date in ET), `LookbackMonths`, `SkipIngest`, `SkipStaging` (≙ `--skip-bteq`), `SkipAnalytics` (≙ `--skip-sas`), `DryRun` (≙ `--dry-run`) |
