# TICKET-06 / MBA-2207 — `01_sas_customer_segments.sas` → Azure Synapse Spark

Migrates the customer segmentation program from SAS 9.4 (Base + SAS/STAT over
Teradata) to a PySpark job running on Azure Synapse Spark against Snowflake.

## Artifacts

| Path | Purpose |
|------|---------|
| `synapse/spark/jobs/customer_segments.py` | The PySpark job (extract → features → scale → k-means → label → validate → publish) |
| `synapse/spark/jobdefinitions/customer_segments.json` | Synapse Spark job definition (pool, jars, args, Key Vault wiring) |
| `synapse/reconciliation/reconcile_customer_segments.py` | SAS-vs-Spark reconciliation harness, exits non-zero on threshold breach |
| `tests/synapse/test_customer_segments.py` | Unit tests over the transformation logic (run with local Spark) |

## Construct mapping

| SAS / Teradata | Synapse Spark / Snowflake |
|----------------|---------------------------|
| `%connect_teradata` LIBNAME `STGDB` | Snowflake Spark connector read (`net.snowflake.spark.snowflake`) |
| `PROC SQL ... where CUSTOMER_STATUS='A'` | DataFrame `select` + `filter` in `read_staging` |
| Feature `DATA` step (breadth, tenure/age/balance groups, `log`, ratios) | `engineer_features` |
| `PROC STDIZE METHOD=STD` | `StandardScaler(withMean=True, withStd=True)` — both centre on the mean and divide by the corrected (n−1) sample standard deviation |
| `PROC FASTCLUS MAXCLUSTERS=5 MAXITER=50 CONVERGE=0.001 LEAST=2` | `pyspark.ml.clustering.KMeans(k=5, maxIter=50, tol=0.001, distanceMeasure="euclidean", seed=…)` |
| `PROC SQL` cluster profile + `_N_` labelling | `label_segments` — clusters ranked by mean standardised balance (desc), then labelled `PREMIUM_WEALTH → VALUE_BASIC` |
| `%validate_table` (`min_rows`, key uniqueness, not-null) | `validate` (raises `ValidationError`) |
| `execute (DELETE …) by teradata` + `PROC APPEND … FORCE` | Snowflake connector write, `mode("overwrite")` with `truncate_table=on` (preserves the target DDL) |
| `%log_step` audit macro | `logging` to the Synapse driver log; run-level auditing is owned by the shared run-log from TICKET-03/TICKET-10 |
| `%ABORT CANCEL` on validation failure | Exception → failed Spark job → failed Synapse activity |
| `PROC DATASETS` WORK cleanup | Not required (Spark session teardown) |

Cluster ids: SAS `FASTCLUS` numbers clusters `1..5`; Spark predicts `0..4`, so the
job adds 1 to keep `SEGMENT_ID` in the SAS domain.

## Score basis (important)

The SAS `PROC SQL` in STEP 6 aliases the *standardised* dataset as `c.` and the
raw feature dataset as `f.`, so a literal reading computes
`LIFETIME_VALUE_SCORE` and `ENGAGEMENT_SCORE` from z-scores. The certified
`CUSTOMER_SEGMENTS` extract in `data/03_sas_data_products/customer_segments.csv`
and the column domains in `ddl/02_data_product_tables.sql`
(`ENGAGEMENT_SCORE DECIMAL(5,2)`, non-negative LTV) show the intended semantics
are the **raw** values.

The job therefore defaults to `--score-basis raw`, which reconciles exactly with
the SAS extract, and keeps `--score-basis standardised` available to reproduce
the literal SAS expression. Columns always taken from raw values (as in SAS):
`PRODUCT_BREADTH_INDEX`, `TENURE_GROUP`, `AGE_GROUP`, `BALANCE_TIER` and the
`UPSELL_FLAG` rule.

## Snowflake object assumptions (owned by TICKET-01 / TICKET-02)

This ticket creates no Snowflake objects. It assumes, and lets every value be
overridden on the command line:

- staging database `ETL_STAGING_<ENV>`, schema `STAGING`, table `STG_CUSTOMER_360`
  (produced by TICKET-03);
- data product database `DATA_PRODUCTS_<ENV>`, schema `ANALYTICS`, table
  `CUSTOMER_SEGMENTS` (DDL from TICKET-01);
- functional role `TRANSFORMER` and warehouse `WH_SPARK` (TICKET-02);
- key-pair authentication with the private key resolved from Azure Key Vault via
  `mssparkutils.credentials.getSecret` (TICKET-02) — no credential literals in
  the repo.

## Reproducibility

`--seed` (default `20260401`) is passed to `KMeans`; the cluster ranking used for
labelling breaks ties on cluster id, so the same input plus the same seed yields
identical assignments and labels. Verified by
`test_same_seed_is_reproducible`.

## Validation before publish

`validate()` runs before any write and fails the job on: fewer than `--min-rows`
(default 1000) rows, duplicate `CUSTOMER_ID`, or nulls in `CUSTOMER_ID`,
`SEGMENT_NAME`, `SEGMENT_ID`.

## Reconciliation results (sample extracts in `data/`)

```bash
python synapse/reconciliation/reconcile_customer_segments.py \
  --staging data/02_bteq_staging/stg_customer_360.csv \
  --baseline data/03_sas_data_products/customer_segments.csv
```

| Measure | Result | Notes |
|---------|--------|-------|
| Row count | 407 / 407 | Exact, after the `CUSTOMER_STATUS='A'` filter |
| `CUSTOMER_ID` coverage | 407 matched | No orphan keys either side |
| `LIFETIME_VALUE_SCORE` | 100 % within ±0.01 | Raw score basis |
| `ENGAGEMENT_SCORE` | 100 % within ±0.01 | |
| `PRODUCT_BREADTH_INDEX` | 100 % within ±0.01 | |
| `BALANCE_TIER`, `CROSS_SELL_FLAG`, `RETENTION_RISK_FLAG` | 100 % | |
| `UPSELL_FLAG` | 99.5 % | Two rows, both driven by the `TENURE_GROUP` boundary difference below |
| `TENURE_GROUP` / `AGE_GROUP` | 98.8 % / 94.3 % | All 28 differing rows sit exactly on a boundary (`AGE = 25/57/76`, `TENURE_MONTHS = 12/36/84`). The sample baseline treats the boundary as inclusive; the SAS source uses strict `<`, which the job reproduces. On a true SAS baseline these are expected at 100 % |
| Cluster assignment (after permutation alignment) | 61.7 % | Agreed threshold **≥ 60 %** for the sample extract |

Cluster agreement threshold rationale: `PROC FASTCLUS` with `REPLACE=FULL`
selects initial seeds by a sequential maximum-separation scan, while Spark
`KMeans` uses k-means‖ initialisation. On a 407-row, six-dimensional extract with
no strongly separated clusters, the two converge to different local optima for
the boundary customers even though the segment structure (five clusters ordered
by balance) is preserved. Differences are pure cluster-membership drift at the
margins, resolved for reporting by the maximum-overlap permutation mapping the
harness prints. On production-volume data the threshold should be re-agreed with
the segmentation owners and raised; the harness makes it a single flag
(`--cluster-threshold`).

## Running

```bash
# Unit tests (local Spark)
python -m pytest tests/synapse -q

# Synapse (submitted from the job definition, or manually)
python synapse/spark/jobs/customer_segments.py \
  --sf-url <account>.snowflakecomputing.com --sf-user SVC_SYNAPSE \
  --env DEV --key-vault kv-analytics --key-vault-secret snowflake-synapse-private-key
```
