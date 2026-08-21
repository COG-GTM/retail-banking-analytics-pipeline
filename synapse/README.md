# Azure Synapse orchestration (MBA-2211 / TICKET-10)

Pipeline-as-code replacement for the bash orchestrators. The legacy scripts
(`orchestration/run_full_pipeline.sh`, `config/pipeline_config.cfg`) stay in the
repo until the Teradata/SAS estate is decommissioned.

## Artifacts

| File | Purpose |
|------|---------|
| `pipeline/pl_retail_banking_analytics.json` | Entry point: dry-run gate, delegation to the run pipeline, failure notification |
| `pipeline/pl_retail_banking_analytics_run.json` | Phase ordering with fail-fast dependencies and the skip-phase conditions |
| `pipeline/pl_staging_snowflake.json` | Phase 1 - the three Snowflake staging models (TICKET-03..05) |
| `pipeline/pl_analytics_spark.json` | Phase 2 - the four Synapse Spark analytics jobs (TICKET-06..09) |
| `pipeline/pl_post_run_validation.json` | Phase 3 - row-count validation and run-log write |
| `linkedService/ls_snowflake.json` | Snowflake key-pair connection, private key from Key Vault |
| `linkedService/ls_keyvault.json` | Key Vault linked service, vault name supplied per environment |
| `trigger/tr_daily_retail_banking_analytics.json` | Daily 02:00 UTC schedule |
| `config/{dev,uat,prod}.parameters.json` | Everything that used to live in `config/pipeline_config.cfg` |

## Shell flag equivalents

| Shell | Synapse |
|-------|---------|
| `./run_full_pipeline.sh` | Run `pl_retail_banking_analytics` with the environment defaults |
| `--skip-bteq` | `skipStaging = true` |
| `--skip-sas` | `skipAnalytics = true` |
| `--dry-run` | `dryRun = true` (writes the planned activity list to the `plannedActivities` variable, touches no data) |
| exit code 99 on zero rows | `FailOnRowCountBreach` Fail activity, error code 99 |

## Deploying

```bash
python scripts/validate_synapse_artifacts.py          # static checks, no cloud access
python scripts/deploy_synapse.py --environment dev --dry-run
python scripts/deploy_synapse.py --environment dev    # requires an authenticated az CLI
```

`deploy_synapse.py` renders the environment-neutral artifacts by substituting the
Spark pool name and the default pipeline parameters from the environment file, so
DEV/UAT/PROD deploy from identical definitions with no manual edits. CI runs the
validation on every pull request and the deployment on `main`
(`.github/workflows/synapse-ci.yml`).

## Secrets

No credential is stored in the repo. The Snowflake private key and the alert
webhook URL are read from Azure Key Vault at runtime - the linked service
resolves the private key, and the entry pipeline fetches the webhook with the
workspace managed identity. Environment files carry only vault and secret
*names*.

## Assumptions on predecessor tickets

These artifacts reference, but do not define, the outputs of TICKET-01..09:

- Snowflake databases/schemas `<staging_db>.STAGING`, `<products_db>.DATA_PRODUCTS`
  and the run-log table `<staging_db>.OPS.PIPELINE_RUN_LOG` (TICKET-01/02).
- A dbt runner notebook `nb_run_dbt` accepting `dbt_select` plus the run context,
  used for `stg_customer_360`, `stg_txn_summary` and `stg_risk_factors`
  (TICKET-03..05).
- Spark notebooks `nb_customer_segments`, `nb_txn_analytics`, `nb_risk_scoring`
  and `nb_data_products` (TICKET-06..09).

If a predecessor lands different names, only the `referenceName` values and the
`EXPECTED_NOTEBOOKS` set in `scripts/validate_synapse_artifacts.py` change.
