# Databricks Port — Retail Banking Analytics Pipeline

Delta Lake / Unity Catalog port of the Teradata BTEQ + SAS pipeline.
Everything lives under `databricks/`; the legacy tree is untouched.

## Layout

```
databricks/
├── databricks.yml                  # Asset bundle (targets: dev, prod)
├── pyproject.toml                  # retail_banking wheel (deployed to tasks)
├── requirements-dev.txt            # local test deps
├── ddl/                            # UC catalog/schemas, Delta DDL, ANALYZE
├── resources/retail_banking_pipeline.job.yml
├── notebooks/                      # Databricks source-format notebooks
│   ├── 00_resolve_run_mode.py      # skip/dry-run flags -> taskValues
│   ├── 00_bronze_ingest.py
│   ├── silver/01..03               # BTEQ equivalents
│   ├── gold/04..07                 # SAS equivalents
│   ├── 90_row_count_validation.py  # UNION ALL row counts (final BTEQ step)
│   └── 91_parity_check.py          # compares gold vs baseline CSVs
├── src/retail_banking/             # pure DataFrame builders (no I/O)
│   ├── config.py  logging_utils.py  validation.py  parity.py  date_utils.py
│   ├── bronze/ingest.py  silver/*.py  gold/*.py
└── tests/                          # pytest + local PySpark parity tests
```

## Migration mapping

| Legacy artifact | New artifact |
|---|---|
| `bteq/01_stg_customer_360.bteq` | `src/retail_banking/silver/stg_customer_360.py` + `notebooks/silver/01_stg_customer_360.py` |
| `bteq/02_stg_txn_summary.bteq` | `silver/stg_txn_summary.py` + notebook `02` |
| `bteq/03_stg_risk_factors.bteq` | `silver/stg_risk_factors.py` + notebook `03` |
| `sas/01_sas_customer_segments.sas` | `gold/customer_segments.py` + notebook `04` |
| `sas/02_sas_txn_analytics.sas` | `gold/transaction_analytics.py` + notebook `05` |
| `sas/03_sas_risk_scoring.sas` | `gold/customer_risk_scores.py` + notebook `06` |
| `sas/04_sas_data_products.sas` | `gold/customer_master_profile.py` + notebook `07` |
| `sas/macros/validate_table.sas` | `src/retail_banking/validation.py` |
| `sas/macros/log_step.sas` + `WORK.PIPELINE_AUDIT` | `src/retail_banking/logging_utils.py` + `etl_staging.pipeline_audit` |
| `orchestration/run_full_pipeline.sh` | Asset bundle job + `00_resolve_run_mode.py` + `silver_gate`/`gold_gate` condition tasks (`--skip-bteq`→`skip_silver`, `--skip-sas`→`skip_gold`, `--dry-run`→`dry_run`) |
| Final BTEQ row-count `UNION ALL` | `notebooks/90_row_count_validation.py` |
| `ddl/01_staging_tables.sql` | `ddl/01_silver_tables.sql` |
| `ddl/02_data_product_tables.sql` | `ddl/02_gold_tables.sql` |
| Teradata logon (`{SAS004}`/LDAP) | Databricks secret scope `retail-banking-teradata` or UC Lakehouse Federation `CONNECTION` |

## Teradata → Spark idioms

| Teradata / BTEQ | Spark / Delta |
|---|---|
| `QUALIFY ROW_NUMBER() = 1` | `row_number()` window + filter |
| `ADD_MONTHS(d, n)` / `d - INTERVAL 'n' MONTH` | `F.add_months` |
| `MONTHS_BETWEEN` / `datediff('month')` | boundary-crossing diff in `date_utils.py` |
| `current_date - n` days | `F.date_sub` |
| `MULTISET`, `PRIMARY INDEX`, `NO FALLBACK`, `FORMAT`, `CHARACTER SET` | dropped; `CLUSTER BY` on `stg_txn_summary` |
| `PARTITION BY COLUMN(REPORTING_PERIOD)` | `PARTITIONED BY (reporting_period)` |
| `COLLECT STATISTICS` | `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` (`ddl/03_analyze.sql`) |
| Volatile/work tables (`wrk_*`) | intermediate DataFrames |
| Primary index uniqueness | informational `PRIMARY KEY ... NOT ENFORCED` |
| SAS `PROC STDIZE` + `PROC FASTCLUS` | `pyspark.ml` `StandardScaler` + `KMeans` |
| SAS `PROC LOGISTIC` | `pyspark.ml` `LogisticRegression` |
| SAS `PROC RANK` (pct) | average-rank window (`avg(row_number)` per tie group / N) |
| `pd.cut` bins | `when/otherwise` with right-inclusive edges |
| SAS `DELETE WHERE REPORTING_PERIOD` reload | Delta `replaceWhere` |

## Dates

All transforms take `run_date: datetime.date`. Notebooks read the `run_date`
widget/job parameter; empty string = today. `current_date` in the reference
SQL is `F.lit(run_date)`; `current_timestamp` stays `F.current_timestamp()`.
This lets tests replay run_date=2026-04-10 against the sample CSVs.

Date-difference semantics deliberately mirror DuckDB `datediff`, not Spark
`months_between`: `age = year(run) - year(dob)`, `tenure_months =
12*(dy) + (dm)`, and the on-time payment window uses the same
boundary-crossing month diff.

## Deploy & run

```bash
# one-time: secret scope for Teradata credentials
databricks secrets create-scope retail-banking-teradata
databricks secrets put-secret retail-banking-teradata td-host --string-value tdprod.corp.bankdemo.com
databricks secrets put-secret retail-banking-teradata td-user --string-value svc_etl_pipeline
databricks secrets put-secret retail-banking-teradata td-password --string-value '***'

cd databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev
# run the DDL (00_catalog_and_schemas.sql, 01, 02) once via SQL warehouse
databricks bundle run retail_banking_pipeline -t dev \
  --params run_date=2026-04-10
# skip flags / dry run (mirrors run_full_pipeline.sh)
databricks bundle run retail_banking_pipeline -t dev \
  --params skip_silver=true,dry_run=true
```

### Workflow / run-mode gating

`resolve_run_mode` sets `run_silver` / `run_gold` task values from the
`skip_silver`, `skip_gold`, and `dry_run` job parameters (`dry_run=true`
prints the planned step list and forces both to `false`). Two
`condition_task` gates implement the skip semantics:

- `silver_gate` (after `resolve_run_mode`) passes only when
  `run_silver == "true"`; `bronze_ingest` and the three silver tasks run
  on `outcome: "true"`.
- `gold_gate` depends on all three silver tasks plus `resolve_run_mode`
  with `run_if: ALL_DONE`, so it still evaluates (and correctly fails the
  gate) when silver was skipped. The three leaf gold tasks run on
  `outcome: "true"`; `customer_master_profile` → `row_count_validation` →
  `parity_check` chain unchanged.

`bronze_ingest` accepts `source_mode=jdbc` (Teradata via JDBC + secrets) or
`source_mode=csv` (CSV files in `/Volumes/<catalog>/core_banking/landing/`).
With Lakehouse Federation (`CREATE CONNECTION ... TYPE TERADATA` +
`CREATE FOREIGN CATALOG`, commented example in
`ddl/00_catalog_and_schemas.sql`) the foreign catalog *is* bronze and the
ingest task is unnecessary.

## Local tests

```bash
python3 -m venv ~/venvs/dbx
~/venvs/dbx/bin/pip install -r databricks/requirements-dev.txt setuptools
cd <repo root>
SETUPTOOLS_USE_DISTUTILS=local ~/venvs/dbx/bin/pytest databricks/tests -q
~/venvs/dbx/bin/ruff check databricks/
```

(`SETUPTOOLS_USE_DISTUTILS=local` works around pyspark 3.5.3 importing
`distutils` on Python 3.12.)

Tests replay the sample CSVs in `data/` at run_date=2026-04-10:
`test_silver` compares the three staging outputs column-by-column
(numerics within 0.01, keys and string flags exact), `test_gold` runs
`parity.compare_gold_outputs` against `data/03_sas_data_products/`.

## Known differences / caveats

- **Naming:** all identifiers are `lower_snake_case` (Databricks/UC
  convention; UC is case-insensitive anyway).
- **Primary keys** are `NOT ENFORCED` (informational) — Spark does not
  enforce PKs like Teradata UPI.
- **`min_rows`:** SAS hard-codes `min_rows=1000` for gold validation; the
  notebooks read `cfg.min_gold_rows` (default 1000). Sample data has ~500
  customers, so local tests use `min_rows=1`.
- **`RISK_SCORE_THRESHOLD` (700):** read from config and logged (as the SAS
  program does via `%sysget`) but never applied — same as legacy.
- **K-means labels are unstable across implementations** (sklearn vs MLlib
  use different init); `segment_name` parity therefore compares the sorted
  share vector within ±10pp, while all deterministic columns
  (`tenure_group`, `age_group`, `balance_tier`, flags) are compared exactly.
  `segment_id` is the raw cluster id and may differ per run.
- **`probability_of_default`** is compared by mean (±0.05), not row-exact.
- **Risk-driver tie-breaks** use the fixed order CREDIT_UTILIZATION,
  PAYMENT_BEHAVIOUR, TRANSACTION_VELOCITY, BUREAU_SCORE.
- `transaction_analytics` outputs the DDL column list only; the reference
  CSV's extra `total_fees` column is an intermediate, not exported.
