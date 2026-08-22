# Azure Synapse Spark jobs

PySpark replacements for the SAS analytics programs, reading from and writing to
Snowflake (`RETAIL_BANKING_<ENV>`, per TICKET-01) instead of Teradata.

```
synapse/
├── pipeline_utils/                  # shared module, replaces sas/macros
│   ├── config.py                    # environment + Snowflake coordinates
│   ├── secrets.py                   # Azure Key Vault secret resolution
│   ├── snowflake_io.py              # %connect_teradata replacement
│   ├── run_log.py                   # %log_step / %init_audit replacement
│   └── validation.py                # %validate_table replacement
├── jobs/
│   └── data_products_master_profile.py   # 04_sas_data_products.sas
└── tests/                           # unit + reconciliation tests (CI)
```

## Macro mapping

| SAS macro | Replacement | Notes |
|-----------|-------------|-------|
| `%connect_teradata` | `SnowflakeIO` + `KeyVaultSecretResolver` | Four LIBNAMEs become schema arguments; the key-pair credential is read from Azure Key Vault at runtime, never from source |
| `%log_step` / `%init_audit` | `RunLogger` | Buffers audit entries and appends them to `ETL_STAGING.PIPELINE_RUN_LOG` |
| `%validate_table` | `validate_table` | Row-count, key uniqueness, null-rate and threshold checks; `ValidationError` is the `%ABORT CANCEL` equivalent |

## Running the master profile job

```bash
export PIPELINE_ENV=DEV
export SNOWFLAKE_ACCOUNT=<account_locator>
export SNOWFLAKE_USER=SVC_SYNAPSE
export AZURE_KEY_VAULT_URL=https://<vault>.vault.azure.net/

spark-submit --py-files synapse.zip \
    synapse/jobs/data_products_master_profile.py \
    --effective-date 2026-04-10 --min-rows 1000
```

Optional overrides: `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`,
`SNOWFLAKE_PRIVATE_KEY_SECRET`.

## Tests

```bash
cd synapse && pip install -r requirements-dev.txt && pytest -q
```

`tests/test_reconciliation.py` rebuilds the golden record from the sample
extracts under `data/` and compares it column by column with
`data/03_sas_data_products/customer_master_profile.csv`, the SAS output.
