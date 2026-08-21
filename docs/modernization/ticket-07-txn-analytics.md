# TICKET-07 (MBA-2208): `02_sas_txn_analytics.sas` -> Synapse Spark

Migrates the transaction analytics data product from SAS on Teradata to a
PySpark job running on Azure Synapse Spark, reading and writing Snowflake via
the Snowflake Spark connector.

| Legacy asset | Migrated asset |
|---|---|
| `sas/02_sas_txn_analytics.sas` | `synapse/spark/jobs/txn_analytics_job.py` |
| `ddl/02_data_product_tables.sql` (TRANSACTION_ANALYTICS) | `ddl/snowflake/02_data_product_tables_transaction_analytics.sql` |
| `sas/run_sas_pipeline.sh` step 2 | `synapse/pipelines/02_txn_analytics_pipeline.json` |

The Teradata/SAS assets are intentionally left in place for parallel-run
reconciliation.

## SAS -> PySpark mapping

| SAS construct | PySpark equivalent |
|---|---|
| `proc sql ... group by CUSTOMER_ID` (STEP 2) | `aggregate_to_customer` — `groupBy("CUSTOMER_ID").agg(...)`, `count(distinct)` -> `countDistinct` |
| Data step trend / revenue derivations (STEP 3) | `add_spend_trend` — chained `when/otherwise` |
| `proc rank groups=100` (STEP 4) | `add_spend_percentile` — `rank()` + tie count window |
| `proc means median= qrange=` (STEP 5) | `sas_quantiles` — exact order-statistic lookup |
| `median + 3 * IQR` anomaly rule | `add_anomaly_flag(method="sas_parity")` |
| `%validate_table` | `validate` (raises `ValidationError`) |
| `%log_step` | `log_step` (stdlib logging, captured in Synapse driver logs) |
| `proc sql ... execute (DELETE ...) by teradata` + `proc append` | `delete_reporting_period` + append-mode connector write |
| `%abort cancel` | uncaught `ValidationError` -> non-zero exit, Synapse activity fails |

## Percentile ranking parity

`PROC RANK ... GROUPS=k` assigns

```
group = floor(rank * k / (n + 1))
```

where `n` is the number of non-missing values and `rank` is the ordinary rank
under the default `TIES=MEAN`, i.e. tied values all receive the average of the
ranks they span. The job reproduces this with

```python
mean_rank = rank() over (order by TOTAL_DEBIT_AMT) + (count(*) over (partition by TOTAL_DEBIT_AMT) - 1) / 2
SPEND_PERCENTILE = floor(mean_rank * 100 / (n + 1))
```

so tied spend values always share a bucket, matching SAS. `NTILE` was rejected:
it distributes rows into equal-sized buckets and splits ties across bucket
boundaries. Rows with a missing value keep a NULL percentile, as SAS leaves
them missing.

## Quantile definition and IQR bounds

`PROC MEANS` uses `QNTLDEF=5` by default (empirical distribution function with
averaging). With `n` non-missing values sorted ascending and `j = n * p`:

* `j` integer -> quantile = `(x[j] + x[j+1]) / 2`
* otherwise   -> quantile = `x[ceil(j)]`

Spark's `percentile` / `approx_percentile` use linear interpolation
(equivalent to `QNTLDEF=4`), which drifts from SAS on small or discrete
datasets, so `sas_quantiles` computes the order statistics directly and applies
the QNTLDEF=5 rule. `QRANGE` is `Q3 - Q1` under the same definition.

Two anomaly rules are available:

* `sas_parity` (default) — the legacy rule `TOTAL_DEBIT_AMT > median + 3 * IQR`,
  used so the migrated output reconciles 1:1 with the SAS data product.
* `tukey` — `TOTAL_DEBIT_AMT < Q1 - 1.5 * IQR` or `> Q3 + 1.5 * IQR`, the
  standard fences requested for the target state; enable with
  `--anomaly-method tukey` once the parallel run is signed off.

Both are suppressed when `IQR <= 0`, matching the `_IQR > 0` guard in SAS.

## Validation before publish

`validate()` runs before anything is written and raises `ValidationError` on:

* row count below `--min-rows` (default 1000, same as `%validate_table`);
* duplicate `CUSTOMER_ID`;
* NULLs in `CUSTOMER_ID`, `REPORTING_PERIOD`, `TOTAL_TRANSACTIONS`.

Publishing is idempotent: the target `REPORTING_PERIOD` is deleted through the
Snowflake connector's `Utils.runQuery` before the append, mirroring the
`DELETE ... WHERE REPORTING_PERIOD = ...` pass-through in SAS.

## Configuration

Connection settings come from the environment (Synapse linked service backed by
Key Vault): `SF_URL`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`, plus
`--sf-database` / `--sf-stg-schema` / `--sf-dp-schema` arguments. No credential
is stored in the repository.

## Testing

`tests/test_txn_analytics_job.py` exercises the transformation functions on a
local Spark session, including the tie-handling and QNTLDEF=5 cases:

```bash
pip install pyspark pytest
pytest tests/test_txn_analytics_job.py
```
