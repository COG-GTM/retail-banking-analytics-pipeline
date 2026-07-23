# PySpark / Databricks pipeline

Databricks (PySpark + Delta Lake + Unity Catalog) port of the legacy Teradata
BTEQ + SAS retail-banking analytics pipeline. Code lives under this `pyspark/`
directory; the legacy `bteq/`, `sas/`, `ddl/` sources remain in the repo root as
reference.

## Layout

```
pyspark/
  common/          # config, spark, audit, validation helpers
  ddl/             # Delta CREATE TABLE .sql + bootstrap
  jobs/            # one module per pipeline step (run(spark, cfg))
  orchestration/   # Databricks Workflow / DLT + local_runner.py
  tests/           # pytest unit tests (seeded from ../data CSVs)
  databricks.yml   # Databricks Asset Bundle
  requirements.txt
  conftest.py      # shared spark fixture
```

## Running tests

Requires JDK 17 and the repo-root `.venv`.

```bash
# from repo root
cd pyspark && . ../.venv/bin/activate && pytest
```

## Configuration & secrets

All configuration flows through `pyspark/common/config.py` — there are **no
hardcoded catalog/schema/table names, paths, or credentials** anywhere else.
This replaces the legacy `config/pipeline_config.cfg` bash exports and the
hardcoded `{SAS004}` encrypted Teradata passwords in
`sas/macros/connect_teradata.sas`.

### `Config`

`get_config()` returns a frozen `Config` dataclass. Values resolve in this order
(first hit wins):

1. **Databricks job parameters / notebook widgets** — `dbutils.widgets.get(<field>)`.
2. **Environment variables** — `PIPELINE_<FIELD>` (e.g. `PIPELINE_CATALOG`,
   `PIPELINE_LOOKBACK_MONTHS`), used for local runs and CI.
3. **Frozen defaults** on `Config`.

| Field                  | Default            | Legacy source (`pipeline_config.cfg`) |
| ---------------------- | ------------------ | ------------------------------------- |
| `catalog`              | `retail_banking`   | —                                     |
| `schema_core`          | `core_banking`     | `DB_CORE=CORE_BANKING_DB`             |
| `schema_txn`           | `txn_processing`   | `DB_TXN=TXN_PROCESSING_DB`            |
| `schema_stg`           | `etl_staging`      | `DB_STG=ETL_STAGING_DB`               |
| `schema_dp`            | `data_products`    | `DB_DP=DATA_PRODUCTS_DB`              |
| `lookback_months`      | `12`               | `LOOKBACK_MONTHS`                     |
| `risk_score_threshold` | `700`              | `RISK_SCORE_THRESHOLD`                |
| `run_date`             | today (ISO)        | `RUN_DATE`                            |
| `log_level`            | `INFO`             | `LOG_LEVEL`                           |
| `secret_scope`         | `retail_banking`   | —                                     |

Build fully-qualified table names with the helper (never hardcode
`catalog.schema.table`):

```python
from common.config import get_config

cfg = get_config()
cfg.table(cfg.schema_stg, "stg_customer_360")
# -> "retail_banking.etl_staging.stg_customer_360"
```

### Secrets

`get_secret(cfg, key)` resolves secrets in order:

1. `dbutils.secrets.get(cfg.secret_scope, key)` on Databricks.
2. Environment variable named `key` (local/CI).
3. An optional `default`; otherwise raises `MissingSecretError`.

```python
from common.config import get_config, get_secret

cfg = get_config()
td_password = get_secret(cfg, "td_password")  # scope: retail_banking
```

**Required secret keys** (populate in the `retail_banking` secret scope, or as
env vars locally):

| Key           | Purpose                                                                 |
| ------------- | ----------------------------------------------------------------------- |
| `td_password` | Legacy Teradata service-account password — only needed while migrating data off Teradata. Replaces the hardcoded `{SAS004}` values. |

Create the scope + keys once per workspace:

```bash
databricks secrets create-scope retail_banking
databricks secrets put-secret  retail_banking td_password
```

### Databricks Workflow wiring

`databricks.yml` (Databricks Asset Bundle) defines a job whose **parameters map
1:1 onto `Config` fields**, so `get_config()` reads them via `dbutils.widgets`.
The `secret_scope` variable selects the scope used by `get_secret`.

```bash
databricks bundle deploy -t dev
databricks bundle run retail_banking_pipeline -t dev
```
