# Retail Banking Analytics — Databricks implementation

PySpark + Delta + Unity Catalog port of the three-phase Teradata BTEQ / SAS 9.4
pipeline. The legacy artefacts (`bteq/`, `sas/`, `orchestration/`, `ddl/`) are
kept unchanged for reference; everything under `databricks/` is the replacement.

The four certified data products — `CUSTOMER_SEGMENTS`, `TRANSACTION_ANALYTICS`,
`CUSTOMER_RISK_SCORES`, `CUSTOMER_MASTER_PROFILE` — keep the exact column names,
types and ordering of `ddl/02_data_product_tables.sql`, so downstream consumers
are unaffected.

## Unity Catalog layout

| Layer  | Schema                        | Contents |
|--------|-------------------------------|----------|
| bronze | `retail_banking.core_banking` | `CUSTOMERS`, `ACCOUNTS`, `ADDRESSES`, `TRANSACTIONS`, `TRANSACTION_TYPES`, `CUSTOMER_BUREAU_SCORES` |
| silver | `retail_banking.etl_staging`  | `STG_CUSTOMER_360`, `STG_TXN_SUMMARY`, `STG_RISK_FACTORS` |
| gold   | `retail_banking.data_products`| `CUSTOMER_SEGMENTS`, `TRANSACTION_ANALYTICS`, `CUSTOMER_RISK_SCORES`, `CUSTOMER_MASTER_PROFILE` |
| ops    | `retail_banking._ops`         | `etl_run_log` |

Source extracts are read from the managed volume
`retail_banking.core_banking.landing` (override with `source_data_path`).
Catalog and schema names are job parameters, so `dev` deploys into
`retail_banking_dev` without touching the notebooks.

## Old artefact → new artefact

| Legacy | Databricks |
|---|---|
| `ddl/00_source_tables.sql` | `shared/schemas.py` (`SOURCE_SCHEMAS`) + `notebooks/bronze/01_load_source_tables.py` |
| `bteq/01_stg_customer_360.bteq` | `notebooks/silver/01_stg_customer_360.py` |
| `bteq/02_stg_txn_summary.bteq` | `notebooks/silver/02_stg_txn_summary.py` |
| `bteq/03_stg_risk_factors.bteq` | `notebooks/silver/03_stg_risk_factors.py` |
| `sas/01_sas_customer_segments.sas` | `notebooks/gold/01_customer_segments.py` |
| `sas/02_sas_txn_analytics.sas` | `notebooks/gold/02_txn_analytics.py` |
| `sas/03_sas_risk_scoring.sas` | `notebooks/gold/03_risk_scoring.py` |
| `sas/04_sas_data_products.sas` | `notebooks/gold/04_data_products.py` |
| `sas/macros/log_step.sas`, `%init_audit` | `shared/audit.py` → `_ops.etl_run_log` |
| `sas/macros/validate_table.sas` | `shared/dq.py` |
| `sas/macros/connect_teradata.sas` (LDAP `LIBNAME`, `{SAS004}` password) | deleted — Unity Catalog grants; secret scopes for any external system |
| `config/pipeline_config.cfg` | job parameters in `resources/pipeline_job.yml` |
| `orchestration/run_full_pipeline.sh` | the Workflow in `resources/pipeline_job.yml` |
| final `envsubst` BTEQ row-count block | `notebooks/validation/99_validate_data_products.py` |
| `--skip-bteq` / `--skip-sas` | job parameters `skip_silver` / `skip_gold` |
| Teradata `CREATE ... AS ... WITH DATA` | `write.format("delta").mode("overwrite")` |
| `COLLECT STATISTICS`, `PRIMARY INDEX` | dropped; `OPTIMIZE ... ZORDER BY (CUSTOMER_ID)` |
| `VT_RUN_PARAMS` volatile table | `lookback_months` job parameter (default 12) |
| `QUALIFY ROW_NUMBER() OVER (...)` | `Window` + `row_number()` filter |
| `NULLIFZERO(x)` | `nullif(x, 0)` |
| `STDDEV_POP`, `ADD_MONTHS`, `MONTHS_BETWEEN` | `stddev_pop`, `add_months`, `months_between` |
| `PROC STDIZE method=std` | `VectorAssembler` + `StandardScaler(withMean, withStd)` |
| `PROC FASTCLUS maxclusters=5` | `pyspark.ml.clustering.KMeans(k=5, maxIter=50, tol=0.001)` |
| `PROC LOGISTIC` | `pyspark.ml.classification.LogisticRegression` |
| `PROC RANK groups=100` | `ntile(100) - 1` |
| `PROC MEANS median= qrange=` | `approxQuantile` |

## Workflow DAG

```
uc_setup
   └── bronze_ingest
         └── silver_customer_360
               ├── silver_txn_summary ──┐
               └── silver_risk_factors ─┤
                                        ├── gold_customer_segments ─┐
                                        ├── gold_txn_analytics ─────┤
                                        └── gold_risk_scoring ──────┤
                                                                    └── gold_data_products
                                                                          └── validate_data_products
```

`silver_txn_summary` and `silver_risk_factors` run in parallel after
`silver_customer_360`; the three gold analytics notebooks run in parallel and
the golden record waits for all three, exactly like the sequencing enforced by
`run_full_pipeline.sh`.

## Deploy and run

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run retail_banking_analytics_pipeline -t dev

# override any job parameter for a single run
databricks bundle run retail_banking_analytics_pipeline -t dev \
  --params lookback_months=6,skip_gold=true
```

Upload the source extracts to the landing volume before the first run:

```bash
databricks fs cp -r data/01_source_tables \
  dbfs:/Volumes/retail_banking/core_banking/landing
```

### Parameters

| Parameter | Default | Purpose |
|---|---|---|
| `catalog` | `retail_banking` | Unity Catalog catalog |
| `bronze_schema` / `silver_schema` / `gold_schema` / `ops_schema` | `core_banking` / `etl_staging` / `data_products` / `_ops` | schema names |
| `source_data_path` | `/Volumes/retail_banking/core_banking/landing` | source CSV location |
| `lookback_months` | `12` | transaction summary window (former `VT_RUN_PARAMS`) |
| `risk_score_threshold` | `700` | from `pipeline_config.cfg` |
| `min_gold_rows` | `1000` | `%validate_table(min_rows=)` for the gold products |
| `model_seed` | `42` | k-means seed, for reproducible segments |
| `skip_silver` | `false` | equivalent of `--skip-bteq` |
| `skip_gold` | `false` | equivalent of `--skip-sas` |

`min_gold_rows` is a parameter because the SAS programs hard-coded
`min_rows=1000`, which no longer holds for a smaller extract; the `dev` target
lowers it to 100 rather than weakening the check itself.

## Audit and data quality

Every task wraps its work in `AuditLogger.step(...)`, which appends `START` and
then `SUCCESS` (with the produced row count) or `ERROR` (with the exception) to
`retail_banking._ops.etl_run_log` — the same trail `%log_step` wrote to
`WORK.PIPELINE_AUDIT`, plus a run id and step duration.

`shared/dq.validate_table` reproduces `%validate_table` check for check:
minimum row count and key uniqueness raise `DataQualityError` (aborting the
task, as `%abort cancel` did), while NULL columns are reported as warnings.

## Security

No credential material exists in this implementation: no `{SAS004}` password, no
`LOGMECH=LDAP`, no `LIBNAME`. Data access is authorised by Unity Catalog grants
on the catalog and schemas; if an external system is ever needed, read its
credentials from a Databricks secret scope (`dbutils.secrets.get`) rather than
from a config file.

## Behaviour that intentionally differs from the legacy code

1. **K-means initialisation.** `PROC FASTCLUS` seeds with `replace=full`, Spark
   uses `k-means||`. Same k, iterations and tolerance, and the clusters are
   still labelled by descending average `LOG_BALANCE`, but individual borderline
   customers can land in a neighbouring segment. Segment *sizes* match the
   reference implementation to within a few percent.
2. **Stepwise selection.** `PROC LOGISTIC ... selection=stepwise (slentry=0.10,
   slstay=0.05)` has no Spark equivalent, so `LogisticRegression` is fitted on
   all nine predictors with `regParam=0` (unpenalised MLE, as in SAS). Only
   `PROBABILITY_OF_DEFAULT` and the `WATCH_LIST_FLAG` that depends on it are
   affected; every other risk field is derived deterministically from the
   component scores. If exact parity is ever required, collect the (customer
   scale) feature frame and fit `statsmodels` stepwise on the driver.
3. **Segment scores use raw features.** The SAS program reads
   `LIFETIME_VALUE_SCORE`, `ENGAGEMENT_SCORE` and the cross-sell / retention
   flags off `WORK.CUST_CLUSTERED`, which holds the *standardised* copies of
   `LOG_BALANCE`, `TENURE_MONTHS` and `ACCT_RATIO` produced by `PROC STDIZE`.
   Z-scores make `LIFETIME_VALUE_SCORE` negative for roughly half the book, so
   this is treated as a defect: the raw features are used here, matching the
   documented business definition and the DuckDB reference implementation.
4. **`SPEND_PERCENTILE` is 0-99**, matching `PROC RANK groups=100`
   (`ntile(100) - 1`), not the 1-100 that bare `ntile` would produce.
5. **Period-scoped load.** `TRANSACTION_ANALYTICS` is written with Delta
   `replaceWhere` on `REPORTING_PERIOD`, reproducing the SAS
   `DELETE ... WHERE REPORTING_PERIOD = '<period>'` + `PROC APPEND` without
   rewriting history. The other three products are full overwrites.

## Local validation

The transformations live in plain functions guarded by `if in_databricks()`, so
the same code can be exercised on local Spark with no cluster:

```bash
python databricks/tests/test_gold_schema_contract.py   # gold schemas vs ddl/02
uv run export_data.py --customers 500                  # regenerate the reference
python databricks/tests/run_local_pipeline.py          # run + diff vs reference
```

`run_local_pipeline.py` compares row counts for all seven tables, checks the
gold schemas against the DDL contract, and prints the segment distribution, risk
tier distribution and composite score statistics next to the DuckDB reference.

Regenerate the reference before comparing: several transforms are relative to
`current_date()` (12-month lookback, 30/90-day balance windows, tenure, age), so
the committed point-in-time extracts drift as they age.

Latest run against a freshly generated 500-customer extract:

| Table | PySpark | DuckDB reference |
|---|---|---|
| `STG_CUSTOMER_360` | 478 | 478 |
| `STG_TXN_SUMMARY` | 1,251 | 1,251 |
| `STG_RISK_FACTORS` | 478 | 478 |
| `CUSTOMER_SEGMENTS` | 407 | 407 |
| `TRANSACTION_ANALYTICS` | 500 | 500 |
| `CUSTOMER_RISK_SCORES` | 407 | 407 |
| `CUSTOMER_MASTER_PROFILE` | 407 | 407 |

Risk tier distribution and composite score statistics are identical
(`LOW` 290 / `MODERATE` 113 / `ELEVATED` 4; mean 15.894226, max 53.36), as are
every deterministic staging aggregate. The remaining differences are places
where the reference implementation simplified the legacy SQL and this port did
not:

| Column | Difference |
|---|---|
| `NEW_MERCHANT_CNT_30D` | The reference counts every distinct merchant in the last 30 days; the BTEQ `NOT IN` correlated subquery — implemented here as an anti-join against the account's prior merchants — counts only merchants never seen before. |
| `PRIMARY_RISK_DRIVER` / `SECONDARY_RISK_DRIVER` | `CREDIT_RISK_COMPONENT` and `100 - BUREAU_SCORE_COMPONENT` are equal by construction; the SAS `do i = 1 to 4` loop uses strict `>`, so the tie resolves to `CREDIT_UTILIZATION` primary / `BUREAU_SCORE` secondary. The reference picks the other order. |
| `TOP_SPEND_CATEGORY` | SAS aggregates the account-level categories with `max()`; the reference takes `first()`. |
| `AGE`, `TENURE_MONTHS`, `MONTHS_SINCE_LAST_LATE` | Teradata `(CURRENT_DATE - DOB)/365.25` and `MONTHS_BETWEEN(...)` truncated to an integer, versus the reference's calendar-boundary `datediff('year'/'month', ...)`. Differences are at most one unit. |
| `SPEND_PERCENTILE` | 0-99 (`PROC RANK`) versus the reference's percent rank. |

In each case the Databricks implementation follows the BTEQ/SAS source, which is
the contract being migrated.
