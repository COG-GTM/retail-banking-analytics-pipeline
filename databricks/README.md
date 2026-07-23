# Databricks pipeline (modernized)

Databricks-native reimplementation of the retail-banking analytics pipeline,
migrated off Teradata BTEQ + SAS 9.4. See [`../MIGRATION.md`](../MIGRATION.md) for
the per-ticket mapping and design notes.

## Layout

```
databricks/
├── common/                     # shared utilities (ticket 2 & 3)
│   ├── config.py               #   widgets/job-params + Databricks Secrets
│   ├── audit.py                #   Delta audit log (etl_run_log)
│   ├── validation.py           #   validate_dataframe (raises DataValidationError)
│   ├── ddl.py                  #   render/split/run the Delta DDL files
│   └── spark_utils.py          #   get_spark (local Spark+Delta or active session)
├── ddl/                        # ticket 1 — Unity Catalog + Delta DDL
│   ├── 00_unity_catalog_setup.sql
│   ├── 01_source_tables.sql
│   ├── 02_staging_tables.sql
│   └── 03_data_product_tables.sql
├── jobs/                       # tickets 4-10 — transformation logic (pure functions)
│   ├── stg_customer_360.py     stg_txn_summary.py     stg_risk_factors.py
│   ├── customer_segments.py    transaction_analytics.py  risk_scoring.py
│   └── master_profile.py
├── notebooks/                  # thin Databricks notebook wrappers (04..10 + setup)
├── orchestration/             # local runner + sample-data loader + step wiring
│   ├── pipeline.py             #   run_pipeline: read → transform → write → validate → log
│   ├── load_sample_data.py     #   load repo data/ fixtures into Delta
│   └── run_local.py            #   CLI entrypoint for a full local run
├── resources/
│   └── retail_banking_pipeline.job.yml   # Workflow: tickets 4→10 in dependency order
├── databricks.yml              # Asset Bundle definition (dev / prod targets)
├── requirements.txt
└── tests/                      # pytest: unit + functional + end-to-end (95% cov)
```

## Run locally

Requires JDK 17 and Python 3.12.

```bash
python -m venv .venv
.venv/bin/pip install -r databricks/requirements.txt

# full pipeline on the bundled sample data (data/01_source_tables/*.csv)
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  PYTHONPATH=databricks \
  .venv/bin/python -m orchestration.run_local \
  --catalog spark_catalog --min-rows 1 --run-date 2026-04-10
```

`run_local` creates the schemas/tables (Delta), loads the CSV fixtures, then runs
`stg_customer_360 → stg_txn_summary → stg_risk_factors → customer_segments →
transaction_analytics → customer_risk_scores → customer_master_profile`, validating
and audit-logging each step.

## Tests

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 .venv/bin/python -m pytest databricks/tests
# with coverage
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 .venv/bin/python -m pytest databricks/tests \
  --cov=databricks --cov-report=term-missing --cov-fail-under=90
```

## Deploy on Databricks

```bash
databricks bundle deploy -t dev     # or -t prod
databricks bundle run retail_banking_pipeline -t dev
```

Job parameters: `catalog`, `min_rows`, `run_date`. The bundle default catalog is
`retail_banking_analytics`.

## Configuration & secrets

- Runtime params come from **widgets / job parameters** (`catalog`, `core_schema`,
  `txn_schema`, `staging_schema`, `products_schema`, `lookback_months`,
  `risk_score_threshold`, `secret_scope`, `run_date`); off-cluster they fall back to
  upper-cased env vars, then defaults.
- Secrets resolve from a **Databricks secret scope** (default
  `retail_banking_analytics`) via `PipelineConfig.secret(key)`; local dev can supply
  `RBA_SECRET_<KEY>` env vars. No credentials are stored in source or logged.

Set up the secret scope once:

```bash
databricks secrets create-scope retail_banking_analytics
databricks secrets put-secret retail_banking_analytics <key>
```

## Not migrated

`export_data.py` is a DuckDB/scikit-learn **local data generator** and is out of
scope for the Databricks migration.
