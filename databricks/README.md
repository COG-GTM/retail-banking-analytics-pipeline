# Databricks Port of the Retail Banking Analytics Pipeline

A one-to-one port of the three-phase **Teradata BTEQ + SAS 9.4** pipeline onto
**Unity Catalog + Delta + Spark**, orchestrated by **Databricks Workflows**. The
four certified data products keep the schemas and semantics defined in
`ddl/02_data_product_tables.sql`.

The legacy `bteq/`, `sas/`, `orchestration/`, `ddl/` and `config/` directories are
untouched — they remain the reference for comparison.

## Architecture

```
 BRONZE (Unity Catalog)          SILVER (was BTEQ)              GOLD (was SAS)
 ======================         ==================            ==================

 <catalog>.bronze               <catalog>.silver              <catalog>.gold
 ├─ CUSTOMERS  ─────────┐
 ├─ ACCOUNTS   ─────────┼──▶ STG_CUSTOMER_360 ──────┬──▶ CUSTOMER_SEGMENTS ────┐
 ├─ ADDRESSES  ─────────┘                           │    (StandardScaler+KMeans)│
 │                                                  │                           │
 ├─ TRANSACTIONS ───────┐                           ├──▶ TRANSACTION_ANALYTICS ─┤
 ├─ TRANSACTION_TYPES ──┼──▶ STG_TXN_SUMMARY ───────┘    (cume_dist + IQR)      ├──▶ CUSTOMER_MASTER_PROFILE
 │                      │                                                       │    (4-way golden record)
 └─ CUSTOMER_BUREAU_SCORES ─▶ STG_RISK_FACTORS ─────────▶ CUSTOMER_RISK_SCORES ─┘
                                                          (LogisticRegression)

 <catalog>.ops.ETL_RUN_LOG  ← every step, from shared/audit.py (was %log_step)
```

## Layout

```
databricks/
├── databricks.yml                        # Asset Bundle (dev / prod targets)
├── ruff.toml, pytest.ini                 # lint + test config for this subtree
├── notebooks/
│   ├── bronze/00_ingest_source_tables.py # 6 source tables -> Delta
│   ├── silver/01_stg_customer_360.py     # was bteq/01_stg_customer_360.bteq
│   ├── silver/02_stg_txn_summary.py      # was bteq/02_stg_txn_summary.bteq
│   ├── silver/03_stg_risk_factors.py     # was bteq/03_stg_risk_factors.bteq
│   ├── gold/01_customer_segments.py      # was sas/01_sas_customer_segments.sas
│   ├── gold/02_txn_analytics.py          # was sas/02_sas_txn_analytics.sas
│   ├── gold/03_risk_scoring.py           # was sas/03_sas_risk_scoring.sas
│   ├── gold/04_data_products.py          # was sas/04_sas_data_products.sas
│   └── validation/99_validate_data_products.py
├── shared/                               # was sas/macros/
│   ├── config.py       # job parameters (was config/pipeline_config.cfg)
│   ├── secrets.py      # Databricks secret scopes (was {SAS004} passwords)
│   ├── schemas.py      # table contracts transcribed from ddl/*.sql
│   ├── io.py           # Delta write / MERGE / OPTIMIZE (was PRIMARY INDEX + PROC APPEND)
│   ├── audit.py        # ETL_RUN_LOG (was %log_step)
│   ├── validation.py   # data quality (was %validate_table)
│   ├── modeling.py     # StandardScaler / KMeans / LogisticRegression helpers
│   └── logging_utils.py# structured JSON logging
├── tests/                                # pytest, runs the whole pipeline locally
└── workflows/pipeline_job.json           # Jobs API definition
```

## Artifact mapping

### Orchestration and shared services

| Legacy artifact | Databricks replacement |
|---|---|
| `orchestration/run_full_pipeline.sh` | Workflow `retail_banking_analytics_pipeline` (`workflows/pipeline_job.json` / `databricks.yml`) |
| `bteq/run_bteq_pipeline.sh`, `sas/run_sas_pipeline.sh` | the `silver_*` and `gold_*` task groups of that Workflow |
| `--skip-bteq` / `--skip-sas` flags | job parameters `skip_bteq`/`skip_silver` and `skip_sas`/`skip_gold` |
| `config/pipeline_config.cfg` | job parameters resolved by `shared/config.py` (`PipelineConfig.from_widgets`) |
| `envsubst` row-count validation at the end of the shell script | `notebooks/validation/99_validate_data_products.py` task |
| `sas/macros/connect_teradata.sas` (LDAP LIBNAME, `{SAS004}` passwords) | Unity Catalog grants; optional JDBC credentials from a secret scope (`shared/secrets.py`) |
| `sas/macros/log_step.sas` + BTEQ `INSERT INTO ETL_RUN_LOG` | `shared/audit.py` -> `<catalog>.<ops>.ETL_RUN_LOG` + JSON driver logs |
| `sas/macros/validate_table.sas` | `shared/validation.py` (`validate_table`, `validate_and_log`, `expectations` for DLT) |
| `ddl/00,01,02*.sql` | `shared/schemas.py` (`SOURCE_SCHEMAS`, `SILVER_SCHEMAS`, `GOLD_SCHEMAS`) |

### Transformations

| Legacy artifact | Databricks replacement | Key rewrites |
|---|---|---|
| `bteq/01_stg_customer_360.bteq` | `notebooks/silver/01_stg_customer_360.py` | `QUALIFY ROW_NUMBER` kept as-is; `MONTHS_BETWEEN`/date arithmetic -> Spark equivalents |
| `bteq/02_stg_txn_summary.bteq` | `notebooks/silver/02_stg_txn_summary.py` | volatile table + `ADD_MONTHS(CURRENT_DATE, -12)` -> CTE + `add_months(run_date, -lookback_months)` |
| `bteq/03_stg_risk_factors.bteq` | `notebooks/silver/03_stg_risk_factors.py` | `WRK_*` work tables -> CTEs; correlated `NOT IN` merchant subquery -> anti-join; `STDDEV_POP` unchanged |
| `sas/01_sas_customer_segments.sas` | `notebooks/gold/01_customer_segments.py` | `PROC STDIZE` -> `StandardScaler`; `PROC FASTCLUS` -> `KMeans(k=5, maxIter=50, tol=0.001)`; same order-by-avg-balance labelling |
| `sas/02_sas_txn_analytics.sas` | `notebooks/gold/02_txn_analytics.py` | `PROC RANK groups=100` -> `cume_dist()`; `PROC MEANS` median/IQR -> `percentile()` |
| `sas/03_sas_risk_scoring.sas` | `notebooks/gold/03_risk_scoring.py` | `PROC LOGISTIC` -> `pyspark.ml.LogisticRegression`; composite formula, tier cutoffs and driver ranking preserved verbatim |
| `sas/04_sas_data_products.sas` | `notebooks/gold/04_data_products.py` | data-step `MERGE ... IN=` -> left joins with the same defaults |
| `CREATE MULTISET TABLE ... PRIMARY INDEX (CUSTOMER_ID)` | Delta `saveAsTable` + liquid clustering on `CUSTOMER_ID` (`shared/io.py`) |
| `COLLECT STATISTICS` | `OPTIMIZE` (falls back to `OPTIMIZE ... ZORDER BY`) |
| `PROC SQL DELETE` + `PROC APPEND FORCE` | Delta `overwrite` (default) or keyed `MERGE` via `write_mode` |

## Running it

Asset Bundle (recommended):

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run retail_banking_analytics_pipeline -t dev
```

Jobs API:

```bash
databricks jobs create --json @databricks/workflows/pipeline_job.json
databricks jobs run-now --job-id <id> --python-params ...
```

Common variations (all as job parameters, no code change):

```bash
# staging refresh only  (was ./run_full_pipeline.sh --skip-sas)
--notebook-params '{"skip_sas": "true"}'
# analytics rerun only  (was ./run_full_pipeline.sh --skip-bteq)
--notebook-params '{"skip_bteq": "true"}'
# backfill an as-of date
--notebook-params '{"run_date": "2026-04-10"}'
```

## Job parameters

| Parameter | Default | Purpose |
|---|---|---|
| `catalog` | `retail_banking` | Unity Catalog catalog for every layer |
| `bronze_schema` / `silver_schema` / `gold_schema` / `ops_schema` | `bronze` / `silver` / `gold` / `ops` | replace `CORE_BANKING_DB`, `ETL_STAGING_DB`, `DATA_PRODUCTS_DB` |
| `run_date` | `""` (today) | replaces `CURRENT_DATE`, making runs reproducible and backfillable |
| `lookback_months` | `12` | transaction summary window |
| `risk_score_threshold` | `700` | carried over from `pipeline_config.cfg`; like `%let RISK_THRESHOLD` in the SAS program it is recorded in the run log but no scoring rule consumes it |
| `source_format` | `csv` | `csv` (landing volume) or `jdbc` (legacy Teradata) |
| `source_data_path` | `""` | defaults to `/Volumes/<catalog>/<bronze_schema>/landing/01_source_tables` |
| `reference_data_path` | `""` | optional legacy CSVs for the parity checks in the validation task |
| `secret_scope` | `retail-banking-analytics` | scope holding the optional JDBC credentials |
| `skip_bronze` / `skip_silver` / `skip_gold` | `false` | layer switches (`skip_bteq`, `skip_sas` are accepted aliases) |
| `table_format` | `delta` | `parquet` is only for local test runs |
| `write_mode` | `overwrite` | `overwrite` (truncate-and-load parity) or `merge` |
| `optimize_tables` | `true` | run `OPTIMIZE` after each write |
| `min_rows` | `1` | `%validate_table` minimum row count |

## Governance and secrets

* Table access is granted in Unity Catalog; no LIBNAME, no LDAP bind, no
  `{SAS004}` encoded password is carried over. Nothing in this directory
  contains a credential.
* Ingesting straight from the legacy Teradata system (`source_format=jdbc`) is
  the only path that needs a secret: `shared/secrets.py` reads
  `jdbc_url` / `jdbc_user` / `jdbc_password` from the configured scope at run
  time. Values are never logged.
* Audit rows (`ETL_RUN_LOG`) and the JSON driver logs carry run ids, step names,
  statuses and row counts only — never customer attributes.

## Validation

`notebooks/validation/99_validate_data_products.py` is the final Workflow task. It
checks, per gold product:

1. the schema against `shared/schemas.GOLD_SCHEMAS` (i.e. against the Teradata DDL),
2. key uniqueness and required non-null columns,
3. `min_rows`,
4. optionally, row counts and key aggregate sums against the legacy CSVs under
   `reference_data_path`.

`databricks/tests/` runs the same pipeline on a local Spark session against
`data/01_source_tables` and diffs it against the committed legacy outputs:

```bash
cd databricks && python -m pytest tests -q     # ~2 min, needs Java 17+ and pyspark
ruff check .
```

## Parity with the legacy pipeline

Verified equal on the 500-customer generated dataset (`run_date = 2026-04-10`):
row counts at every layer (478 / 1,251 / 478 silver, 407 / 500 / 407 / 407 gold),
all four gold schemas, and the certified numeric columns — balances, credit
limits and utilization, transaction counts and amounts, fee income, spend
percentile, anomaly flags, every risk component, the composite risk score,
probability of default and risk tier.

Four differences against `data/02_bteq_staging` and `data/03_sas_data_products`
are deliberate. Those CSVs come from the DuckDB/pandas reference runner, which
simplifies the legacy logic in the places listed below; this port follows the
BTEQ/SAS source, which is the contract:

| Column | This port | Reference CSVs | Why |
|---|---|---|---|
| `AGE`, `TENURE_MONTHS` | `CAST(datediff(...)/365.25 AS SMALLINT)`, `CAST(months_between(...) AS INT)` — the BTEQ expressions, and Teradata truncates a decimal-to-integer cast exactly like Spark | `datediff('year', ...)`, `datediff('month', ...)` — calendar-boundary counts | reference deviates from `bteq/01_stg_customer_360.bteq`; ±1 on ~28% of rows |
| `NEW_MERCHANT_CNT_30D` | merchants with no earlier transaction on the same account (the BTEQ correlated `NOT IN`, rewritten as an anti-join) | every distinct merchant in the last 30 days | reference drops the correlated subquery |
| `TOP_SPEND_CATEGORY` | `MAX(TOP_MERCHANT_CATEGORY)` per customer, as in `sas/02_sas_txn_analytics.sas` | first account's category | reference substitutes `first()` for SAS `max()` |
| `PRIMARY_RISK_DRIVER`, `SECONDARY_RISK_DRIVER` | SAS `do i = 1 to 4` loop with strict `>`, so ties keep the earlier driver | `np.argsort(...)[::-1]`, so ties keep the later driver | `CREDIT_RISK_COMPONENT` is always `100 - BUREAU_SCORE_COMPONENT`, so those two always tie and the labels come out swapped |

One difference is irreducible: **cluster membership** in `CUSTOMER_SEGMENTS`.
`PROC FASTCLUS`, scikit-learn `KMeans` (k-means++) and `pyspark.ml` `KMeans`
(k-means||) use different seeding, so they converge on different partitions of
the same feature space. The port keeps the requested Spark ML implementation and
everything around it is deterministic and matches: the feature engineering,
`ENGAGEMENT_SCORE`, `PRODUCT_BREADTH_INDEX`, `BALANCE_TIER`, the action flags,
the five certified labels, and the rule that assigns them by descending average
balance. `SEGMENT_ID` is the model-assigned cluster index and is not stable
across engines or runs — consumers should key on `SEGMENT_NAME`.

Not exercisable outside a workspace, and therefore untested here: Delta `MERGE`
(`write_mode=merge`), `OPTIMIZE`/`ZORDER`, liquid clustering, Unity Catalog
grants, and JDBC ingestion from Teradata. The local test run uses Parquet.
