# Azure Synapse Spark jobs

PySpark replacement for the SAS phase of the pipeline, reading from and writing
to Snowflake through the Snowflake Spark connector.

```
synapse/
├── jobs/
│   └── data_products_master_profile.py   # port of sas/04_sas_data_products.sas
├── pipeline_utils/                       # shared module, replaces sas/macros/
│   ├── config.py                         # pipeline + Snowflake parameters
│   ├── secrets.py                        # Azure Key Vault secret resolution
│   ├── snowflake_io.py                   # replaces %connect_teradata
│   ├── run_log.py                        # replaces %log_step / %init_audit
│   └── validation.py                     # replaces %validate_table / %ABORT CANCEL
└── tests/                                # pytest suite run in CI
```

## Macro replacements

| SAS macro | Python replacement | Notes |
|-----------|--------------------|-------|
| `%connect_teradata` | `pipeline_utils.snowflake_io.SnowflakeIO` | Teradata LIBNAMEs (`COREDB`/`TXNDB`/`STGDB`/`DPDB`) become fully qualified `database.schema.table` names; LDAP password auth becomes key-pair (JWT) auth with the private key read from Azure Key Vault (TICKET-02). |
| `%log_step` / `%init_audit` | `pipeline_utils.run_log.RunLogger` + `RunLogSink` | Rows go to the shared `PIPELINE_RUN_LOG` table instead of `WORK.PIPELINE_AUDIT`. |
| `%validate_table` + `%ABORT CANCEL` | `pipeline_utils.validation.validate_dataframe` + `ValidationError` | Row-count, key-uniqueness, null-rate and generic threshold assertions; `raise_for_status()` aborts the job and the failure is written to the run-log. |

## Golden record job

`jobs/data_products_master_profile.py` ports the 4-way data step `MERGE`
(`IN=` semantics) as left joins from the active-customer base. Each merge member
is tagged with a marker column so the SAS `if not _seg then do; ... end;`
default blocks apply only when the customer is absent from that member; a
customer present with NULL measures keeps its NULLs.

## Configuration

All values come from Synapse pipeline parameters exposed as environment
variables; no secrets live in the repo.

| Variable | Purpose | Default |
|----------|---------|---------|
| `PIPELINE_ENV` | DEV / UAT / PROD | `DEV` |
| `PIPELINE_RUN_ID` | Synapse run id recorded in the run-log | empty |
| `RUN_DATE` | Effective date of the run (ISO date) | today |
| `LOG_LEVEL` | Python log level | `INFO` |
| `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` / `SNOWFLAKE_ROLE` / `SNOWFLAKE_WAREHOUSE` | Snowflake connection | see `config.py` |
| `SNOWFLAKE_STAGING_DB` / `SNOWFLAKE_DATA_PRODUCTS_DB` | Database layout from TICKET-01 | `ETL_STAGING_<ENV>` / `DATA_PRODUCTS_<ENV>` |
| `AZURE_KEY_VAULT_URL` / `SNOWFLAKE_PRIVATE_KEY_SECRET` | Key Vault lookup for the key-pair private key | empty / `snowflake-synapse-private-key` |

## Running the tests

```bash
cd synapse
pip install -r requirements-dev.txt
pytest
```

Tests run in CI via `.github/workflows/synapse-python-ci.yml`.

`tests/test_reconciliation_against_sas_output.py` reconciles the job output row for
row against the SAS golden record extract in
`data/03_sas_data_products/customer_master_profile.csv`, and re-runs the merge with
slices of each upstream product removed to exercise the `IN=` default paths.
