# Azure Synapse Migration

## Architecture

```text
LEGACY
cron (0 2 * * *) -> orchestration/run_full_pipeline.sh
                       ├── bteq/run_bteq_pipeline.sh -> BTEQ -> Teradata
                       └── sas/run_sas_pipeline.sh  -> SAS  -> Teradata

SYNAPSE
tr_daily_0200 -> pl_retail_banking_daily
                   ├── SQL pool stored procedures -> dedicated SQL pool
                   └── Spark job definitions       -> Spark pool
                                                       │
                         dedicated pool schemas: core / txn / stg / dp
```

The schedule trigger starts the pipeline at 02:00 UTC. Three staging procedures run in parallel, followed by three parallel Spark analytics jobs, the master-profile job, and a dedicated SQL-pool validation script.

## Pipeline DAG

```text
Load_stg_customer_360 ─┬─> Run_sjd_customer_segments ─┐
Load_stg_txn_summary ──┼─> Run_sjd_txn_analytics ─────┼─> Run_sjd_master_profile
Load_stg_risk_factors ─┴─> Run_sjd_risk_scoring ──────┘            │
                                                                   v
                                                   Validate_post_run_row_counts
                                                     ├─ Failed/Skipped -> Fail_pipeline
                                                     └─ Succeeded      -> pipeline succeeds
```

Each `Run_sjd_*` activity waits for all three staging procedures to succeed. `Run_sjd_master_profile` waits for all three analytics jobs. The validation activity runs after the master profile, and `Fail_pipeline` runs when validation is failed or skipped; the dependency conditions are also reached when an earlier failure causes downstream activities to be skipped.

## Component map

| Unit | Branch | Legacy source | Synapse paths |
|---|---|---|---|
| AZSYN-01 Synapse DDL + source data load | `devin/azsyn-01-ddl-source-load` | `ddl/*.sql`, `data/01_source_tables/*.csv` | `synapse/sql/ddl/00_schemas.sql`, `01_source_tables.sql`, `02_staging_tables.sql`, `03_data_product_tables.sql`, `synapse/sql/load/copy_into_source_tables.sql`, `tests/sql/test_ddl.py` |
| AZSYN-02 | `devin/azsyn-02-stg-customer-360` | `bteq/01_stg_customer_360.bteq` | `synapse/sql/procs/stg.usp_load_stg_customer_360.sql`, `tests/sql/test_stg_customer_360.py` |
| AZSYN-03 | `devin/azsyn-03-stg-txn-summary` | `bteq/02_stg_txn_summary.bteq` | `synapse/sql/procs/stg.usp_load_stg_txn_summary.sql`, `tests/sql/test_stg_txn_summary.py` |
| AZSYN-04 | `devin/azsyn-04-stg-risk-factors` | `bteq/03_stg_risk_factors.bteq` | `synapse/sql/procs/stg.usp_load_stg_risk_factors.sql`, `tests/sql/test_stg_risk_factors.py` |
| AZSYN-05 | `devin/azsyn-05-spark-customer-segments` | `sas/01_sas_customer_segments.sas` | `synapse/spark/jobs/customer_segments.py`, `tests/spark/test_customer_segments.py` |
| AZSYN-06 | `devin/azsyn-06-spark-txn-analytics` | `sas/02_sas_txn_analytics.sas` | `synapse/spark/jobs/txn_analytics.py`, `tests/spark/test_txn_analytics.py` |
| AZSYN-07 | `devin/azsyn-07-spark-risk-scoring` | `sas/03_sas_risk_scoring.sas` | `synapse/spark/jobs/risk_scoring.py`, `tests/spark/test_risk_scoring.py` |
| AZSYN-08 | `devin/azsyn-08-spark-master-profile` | `sas/04_sas_data_products.sas` | `synapse/spark/jobs/master_profile.py`, `tests/spark/test_master_profile.py` |
| AZSYN-09 | `devin/azsyn-09-spark-common-lib` | `sas/macros/*.sas` | `synapse/__init__.py`, `synapse/spark/__init__.py`, `synapse/spark/jobs/__init__.py`, `synapse/spark/common/__init__.py`, `synapse/spark/common/pipeline_utils.py`, `tests/spark/test_pipeline_utils.py`, `requirements-dev.txt` |
| AZSYN-10 (this) | `devin/azsyn-10-orchestration-docs` | `orchestration/*.sh`, `bteq/run_bteq_pipeline.sh`, `sas/run_sas_pipeline.sh`, `config/pipeline_config.cfg`, `docs`, `README` | `synapse/pipelines/pl_retail_banking_daily.json`, `synapse/sparkJobDefinitions/sjd_*.json`, `synapse/triggers/tr_daily_0200.json`, `synapse/config/pipeline_config.*.json`, `synapse/sql/validation/post_run_row_counts.sql`, `docs/synapse_migration.md`, `README.md`, `.gitignore`, `tests/test_synapse_artifacts.py` |

## Construct mapping

| Legacy construct | Synapse replacement |
|---|---|
| `0 2 * * *` cron | `tr_daily_0200` ScheduleTrigger |
| `run_full_pipeline.sh` | `pl_retail_banking_daily` |
| `run_bteq_pipeline.sh` sequential loop | Three parallel `SqlPoolStoredProcedure` activities |
| `run_sas_pipeline.sh` loop | SparkJob activities (three parallel jobs plus master profile) |
| `if rc -ne 0 ... ABORT; exit` | Succeeded dependencies plus `Fail_pipeline` on Failed/Skipped |
| `set -euo pipefail` and grep for `*** Failure` in logs | Synapse activity status |
| SAS `rc >= 2` and `^ERROR` log grep | Spark job non-zero exit and `ValidationError` |
| `pipeline_config.cfg` | `synapse/config/pipeline_config.<env>.json` |
| `TD_SERVER` / `TD_USERNAME` / LDAP | `sqlpool_retail` linked service and Key Vault (managed identity) |
| `RUN_DATE=$(date)` | `AsOfDate` pipeline parameter |
| Phase 3 BTEQ `UNION ALL` | Script activity plus `post_run_row_counts.sql` with `THROW` |
| `envsubst` | Pipeline expressions |
| Master `tee` log and log rotation (`find -mtime +30 gzip`) | Synapse Monitor run history, `stg.ETL_RUN_LOG`, and Log Analytics diagnostic settings |
| `--skip-bteq` / `--skip-sas` | Rerun from a failed activity in Synapse Monitor, or debug-run the pipeline |
| `--dry-run` | Validate All / debug run |
| BTEQ `.IF ACTIVITYCOUNT = 0 .EXIT 99` | `THROW 50099` in the stored procedures |
| SAS `%ABORT CANCEL` | `ValidationError` and a non-zero exit |
| `COLLECT STATISTICS` | `CREATE STATISTICS` |
| Teradata databases | Dedicated pool schemas `core`, `txn`, `stg`, and `dp` |

## As-of-date convention

Scheduled runs set `AsOfDate` from `trigger().scheduledTime` as a UTC `yyyy-MM-dd` date. A manual run with an empty `AsOfDate` uses the pipeline's `TriggerTime` date in UTC; `TriggerTime` is constant within a run, so stored procedures and Spark jobs share the same date even if a run crosses UTC midnight. Stored procedures receive `@AsOfDate DATE`, and Spark jobs receive `--as-of-date`. `REPORTING_PERIOD` is the `yyyy-MM` month of the as-of date and `EFFECTIVE_DATE` is the as-of date. Parity runs use `2026-04-10`, when the committed CSVs were generated.

## Deployment notes

From the repository root, create an archive containing only the Python package files:

```bash
find synapse -type f -name '*.py' -print | zip synapse.zip -@
```

Upload `synapse.zip` to the environment's configured `code_root`. Spark job files must be uploaded under `code_root/synapse/spark/jobs/`; the Spark job definitions currently use the dev storage URL, so override those file paths and the archive URL per environment using the corresponding `pipeline_config.<env>.json`. The `sqlpool_retail` linked service (`AzureSqlDW`, managed identity) must exist before publishing the pipeline. The validation Script activity also requires that linked service. The daily trigger ships in the `Stopped` state; start `tr_daily_0200` after deployment. Pipeline job arguments are configured in the pipeline JSON, and each environment's JSON config is the deployment source for names and storage roots.

## Local testing

Install the local test dependencies with `pip install "pyspark==3.4.*" pytest pandas`, or use `pip install -r requirements-dev.txt` after AZSYN-09 merges. The Spark local tests need those dependencies; the Synapse artifact contract tests use pytest and the Python standard library.

The T-SQL test uses Docker SQL Server 2022 (`mcr.microsoft.com/mssql/server:2022-latest`) and skips cleanly if Docker, its daemon, or the image is unavailable. Its SQL Server shim removes the Synapse `WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)` table option; CTAS-to-`SELECT INTO` conversion is not needed for this validation script.

```bash
pytest tests/test_synapse_artifacts.py
pytest tests/sql
pytest tests/spark
```

## Known deviations and contract notes

- The Script activity requires a linked service named `sqlpool_retail`; no unit owns that linked service.
- Synapse Spark job definitions run their `.py` file as `__main__`, so modules need `if __name__ == "__main__": sys.exit(main())`.
- The legacy cron timezone is assumed to be UTC.
- The three staging procedures now run in parallel; this is safe because the three BTEQ scripts read only from `core` and `txn`.
- Synapse pipelines cannot read the config JSON at runtime, so `LOOKBACK_MONTHS` and `RISK_SCORE_THRESHOLD` are literals in the pipeline and are kept in sync with config by the artifact contract test.
- No unit owns the `synapse.zip` build step.
