# TICKET-07 (MBA-2208) — `02_sas_txn_analytics.sas` → Azure Synapse Spark

The TRANSACTION_ANALYTICS data product is now produced by a PySpark job on
Azure Synapse Spark reading from and writing to Snowflake, replacing the SAS
program that ran against Teradata.

| Artifact | Path |
|---|---|
| Spark job | `synapse/spark/jobs/txn_analytics_job.py` |
| Parity tests | `synapse/spark/tests/test_txn_analytics_job.py` |
| Synapse pipeline | `synapse/pipelines/02_txn_analytics_pipeline.json` |
| Legacy program (retained for reference) | `sas/02_sas_txn_analytics.sas` |

## Data flow

```
RETAIL_BANKING_<ENV>.ETL_STAGING.STG_TXN_SUMMARY   (TICKET-04, dbt)
        │  Snowflake Spark connector (read)
        ▼
  Synapse Spark: txn_analytics_job.py
        │  aggregate → trend → PROC RANK buckets → IQR anomalies → validate
        ▼
RETAIL_BANKING_<ENV>.DATA_PRODUCTS.TRANSACTION_ANALYTICS   (delete-then-append per REPORTING_PERIOD)
```

## SAS → PySpark construct mapping

| SAS construct | PySpark equivalent |
|---|---|
| `LIBNAME` via SAS/ACCESS to Teradata | Snowflake Spark connector (`net.snowflake.spark.snowflake`) with Key Vault–backed credentials |
| `PROC SQL ... GROUP BY CUSTOMER_ID` (STEP 2) | `aggregate_customer_txn`: `groupBy("CUSTOMER_ID").agg(...)`, `countDistinct`, guarded divisions |
| `max(TOP_MERCHANT_CATEGORY)` | `F.max` — lexical maximum, quirk preserved |
| DATA step trend classification (STEP 3) | `add_spend_trend`: chained `when/otherwise` with the same `AVG_TRANSACTION_SIZE * 5` threshold |
| `PROC RANK GROUPS=100` (STEP 4) | `add_proc_rank_groups`: `FLOOR(mean_rank * 100 / (n + 1))` over window functions |
| `PROC MEANS ... median= qrange=` (STEP 5) | `compute_iqr_stats` / `sas_quantiles`: exact order statistics using QNTLDEF=5 |
| `%validate_table` + `%ABORT CANCEL` (STEP 6) | `validate` raising `ValidationError` → non-zero Spark exit → pipeline `Fail` activity |
| `PROC SQL EXECUTE (DELETE ...) BY TERADATA` + `PROC APPEND` (STEP 7) | Snowflake connector `preactions` DELETE + `mode("append")` in one transaction-scoped write |
| `%log_step` audit macro | `logging` to the Spark driver log (surfaced in Synapse monitoring) |
| `PROC DATASETS` WORK cleanup | not required — Spark session teardown releases cached DataFrames |

## Percentile ranking: matching SAS tie handling

`PROC RANK GROUPS=k` does **not** compute `ntile` or `percent_rank`. SAS ranks
the values (ties averaged, the `TIES=MEAN` default) and then buckets them:

```
group = FLOOR(rank * k / (n + 1))          # 0-based, so 0..k-1 for GROUPS=100
```

`add_proc_rank_groups` reproduces this exactly:

* `rank()` over the ascending value order gives the minimum rank of a tie group;
* `count(*)` over a window partitioned by the value gives the tie size, so
  `mean_rank = min_rank + (tie_size - 1) / 2` is the SAS averaged rank;
* `n` counts only non-missing values, and missing values stay missing — Spark
  sorts nulls first by default, so the ordering uses `asc_nulls_last()`.

`ntile(100)` would have produced different buckets: it splits rows into equal
sized groups and can therefore separate tied values into different buckets,
and it is 1-based. `percent_rank` normalises by `n - 1` instead of `n + 1`.

## Quantile definition and anomaly bounds

`PROC MEANS` uses SAS's default quantile definition **QNTLDEF=5** (empirical
distribution with averaging). For the `n` non-missing values sorted ascending
and `np = n * p`:

* `np` integral → `(x[np] + x[np+1]) / 2` (1-based order statistics; `x[n]` when `np = n`);
* otherwise → `x[ceil(np)]`.

`sas_quantiles` implements this with `row_number()` and collects only the two
order statistics it needs, so Q1/median/Q3 — and therefore `QRANGE = Q3 - Q1` —
match SAS exactly rather than within a tolerance. Spark's `percentile_approx`
(approximate) and `percentile` (linear interpolation, i.e. QNTLDEF=1-like) both
give different values on small or tied samples and are deliberately not used.

Two anomaly rules are derived from the same statistics:

| `--anomaly-rule` | Condition | Purpose |
|---|---|---|
| `sas_median_3iqr` (default) | `TOTAL_DEBIT_AMT > median + 3 * IQR` | Bit-for-bit parity with the legacy SAS STEP 5 rule |
| `tukey_fences` | `TOTAL_DEBIT_AMT < Q1 - 1.5*IQR` or `> Q3 + 1.5*IQR` | The standard IQR fences requested for the modernised product |

The default keeps `ANOMALY_FLAG` identical to SAS during parallel-run
reconciliation; `tukey_fences` can be enabled per environment from the pipeline
parameter once the wider fences are signed off. When `IQR <= 0` (degenerate
distribution) no rows are flagged, matching the SAS `_IQR > 0` guard.

## Validation before publish

`validate()` runs on the final DataFrame **before** any write and raises
`ValidationError` (job exits non-zero, pipeline `Fail` activity fires) when:

* the row count is below `--min-rows` (default 1000, the SAS `min_rows=1000`);
* `CUSTOMER_ID` is not unique;
* the null rate of `CUSTOMER_ID`, `REPORTING_PERIOD` or `TOTAL_TRANSACTIONS`
  exceeds `--max-null-rate` (0 by default, the SAS `not_null=` list).

## Output schema

`conform_to_target` projects and casts to the exact published column list and
order of `DATA_PRODUCTS.TRANSACTION_ANALYTICS`, so the contract with downstream
consumers is unchanged from the Teradata data product.

## Running

```bash
# Synapse (via pl_02_txn_analytics)
spark-submit txn_analytics_job.py --env PROD --reporting-period 2026-08

# Local reconciliation against the CSV extracts in data/
spark-submit synapse/spark/jobs/txn_analytics_job.py --io local \
    --input data/02_bteq_staging/stg_txn_summary.csv \
    --output /tmp/transaction_analytics --min-rows 1

pytest synapse/spark/tests
```

## Assumptions

* Snowflake object naming follows TICKET-01: `RETAIL_BANKING_<ENV>` with
  schemas `ETL_STAGING` and `DATA_PRODUCTS`. Those objects are created by
  TICKET-01/TICKET-02 and are not created here.
* `ETL_STAGING.STG_TXN_SUMMARY` is populated by the dbt model from TICKET-04
  with the same column names as the Teradata staging table.
* Snowflake credentials (`snowflake-url`, `snowflake-user`,
  `snowflake-private-key`) live in the Key Vault linked to the Synapse
  workspace; the job reads them through `mssparkutils` and never logs them.
* Orchestration ownership (TICKET-10) stays with the master pipeline; the
  pipeline JSON here is the single-job unit it invokes.
