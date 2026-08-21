# Orchestration migration: shell scripts to Azure Synapse Pipelines

Ticket: MBA-2211 (TICKET-10)

## Why

`orchestration/run_full_pipeline.sh` runs the pipeline from an edge server: it
sources `config/pipeline_config.cfg`, calls the BTEQ and SAS phase runners in
order, aborts on the first non-zero return code, validates row counts with an
inline BTEQ heredoc and writes a timestamped log file. None of that survives the
move to Snowflake and Synapse Spark, so the control flow moves into a Synapse
pipeline that is version-controlled and deployed by CI.

## Behaviour mapping

| Shell construct | Synapse equivalent |
|-----------------|--------------------|
| `set -euo pipefail` + `exit ${RC}` after each phase | `dependsOn` with `Succeeded` conditions; a failed activity leaves downstream activities unrun |
| `--skip-bteq` | `skipStaging` pipeline parameter, `StagingPhase` IfCondition |
| `--skip-sas` | `skipAnalytics` pipeline parameter, `AnalyticsPhase` IfCondition |
| `--dry-run` (log the steps, `exit 0`) | `dryRun` pipeline parameter, `DryRunGate` IfCondition writing the planned activity list to the `plannedActivities` variable and executing nothing else |
| `bteq/run_bteq_pipeline.sh` (3 BTEQ steps) | `pl_staging_snowflake`: three sequential `nb_run_dbt` notebook activities selecting `stg_customer_360`, `stg_txn_summary`, `stg_risk_factors` |
| `sas/run_sas_pipeline.sh` (4 SAS programs) | `pl_analytics_spark`: `nb_customer_segments` -> `nb_txn_analytics` -> `nb_risk_scoring` -> `nb_data_products` |
| Post-run BTEQ `SELECT COUNT(*)` heredoc | `pl_post_run_validation.CheckRowCounts` Script activity over the three staging tables and four data products |
| `.EXIT 99` / silent zero rows | `FailOnRowCountBreach` Fail activity (error code 99) when any table is below its per-environment minimum |
| `ETL_RUN_LOG` audit inserts | `WriteRunLog` Script activity inserting into `<staging_db>.OPS.PIPELINE_RUN_LOG` |
| `tee` to `${LOG_DIR}/pipeline_master_*.log`, log archival | Synapse monitoring/Log Analytics run history; no edge-server log files to rotate |
| Failure e-mail/notification from the edge server | `GetAlertWebhook` (Key Vault via managed identity) -> `NotifyFailure` Web activity -> `FailPipeline` |
| `cron` invocation | `tr_daily_retail_banking_analytics` schedule trigger, 02:00 UTC |

## Configuration mapping

`config/pipeline_config.cfg` values become pipeline parameters, supplied per
environment from `synapse/config/<env>.parameters.json`:

| cfg variable | Pipeline parameter | Notes |
|--------------|--------------------|-------|
| `RUN_DATE` / `RUN_TIMESTAMP` | `runDate` (+ derived `runId` variable) | Empty `runDate` defaults to `utcnow()`; `runId` also carries the Synapse run id |
| `LOOKBACK_MONTHS` | `lookbackMonths` | Passed to the staging models and transaction analytics |
| `RISK_SCORE_THRESHOLD` | `riskScoreThreshold` | Passed to `nb_risk_scoring` |
| `LOG_LEVEL` | `logLevel` | Passed to every notebook |
| `DB_CORE` / `DB_TXN` / `DB_STG` / `DB_DP` | `snowflakeDatabaseCore` / `snowflakeDatabaseTxn` / `snowflakeDatabaseStaging` / `snowflakeDatabaseProducts` | Teradata databases become Snowflake databases with an environment suffix (TICKET-01) |
| `TD_SERVER` / `TD_USERNAME` / `TD_LOGMECH` | `snowflakeAccountIdentifier` / `snowflakeUser` + key-pair auth | LDAP is replaced by key-pair auth; the private key lives in Key Vault (TICKET-02) |
| `SAS_HOME` / `SAS_CONFIG` / `SAS_BATCH` / `SAS_AUTOEXEC` | dropped | Replaced by the Synapse Spark pool |
| `PIPELINE_HOME` / `BTEQ_DIR` / `SAS_DIR` / `LOG_DIR` / `ARCHIVE_DIR` | dropped | No edge-server filesystem in Synapse |
| (new) | `minRowsStaging` / `minRowsProducts` | Row-count thresholds, tighter in UAT/PROD than DEV |
| (new) | `keyVaultName`, `snowflakePrivateKeySecretName`, `alertWebhookSecretName` | Secret *names* only; values stay in Key Vault |

## Failure semantics

A failure anywhere inside `pl_staging_snowflake`, `pl_analytics_spark` or
`pl_post_run_validation` fails its `ExecutePipeline` activity, which fails
`pl_retail_banking_analytics_run`, which trips the `Failed` dependency on
`RunPipeline` in the entry pipeline: the webhook notification fires and
`FailPipeline` marks the run failed. Downstream activities never start, matching
the `exit ${RC}` behaviour of the shell orchestrator.

## Validation performed

There is no Snowflake or Azure environment available for this ticket, so the
artifacts were validated statically:

- `python scripts/validate_synapse_artifacts.py` - JSON well-formedness, artifact
  name/file agreement, resolvable pipeline/linked-service/notebook references,
  complete linked-service parameter sets, `dependsOn` present on every non-first
  activity in every scope (the fail-fast ordering guarantee), consistent
  parameter keys across DEV/UAT/PROD, every required entry-pipeline parameter
  supplied by each environment file, and no credential literals.
- `python scripts/deploy_synapse.py --environment {dev,uat,prod} --dry-run` -
  renders each environment and prints the `az synapse` commands in
  callee-before-caller order.
