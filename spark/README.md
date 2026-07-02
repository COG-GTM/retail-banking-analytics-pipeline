# PySpark Analytics Layer

PySpark + Spark ML port of the SAS analytics layer (Ticket 2). This package
replaces the four SAS programs in `sas/` and the `connect_teradata` SAS/ACCESS
macro. It **does not** touch the BTEQ staging scripts, the Teradata DDL, or the
top-level orchestration — it consumes the same staging inputs the BTEQ layer
produces and writes the same certified data-product outputs (identical columns
and semantics), so downstream BI/ML consumers are unaffected.

## Layout

```
spark/
  config.py              # env-driven, run-scoped configuration (no hardcoded paths/secrets)
  session.py             # get_spark() + DataLayer (replaces the connect_teradata macro)
  logging_utils.py       # structured, run-scoped logging + audit trail (replaces %log_step)
  validation.py          # data-quality gate (replaces %validate_table)
  jobs/
    customer_segments.py # 01  STDIZE + FASTCLUS  -> StandardScaler + KMeans
    txn_analytics.py     # 02  RANK + MEANS       -> window functions + aggregations
    risk_scoring.py      # 03  LOGISTIC           -> LogisticRegression + composite score
    data_products.py     # 04  4-way MERGE        -> left joins with defaults
    run_pipeline.py      # runs 01 -> 04 in order (replaces run_sas_pipeline.sh)
  tests/                 # PySpark unit tests (pytest)
  spark_pipeline.cfg     # env config, sourced by run_spark_pipeline.sh
  run_spark_pipeline.sh  # shell entrypoint
  requirements.txt
```

## SAS -> PySpark mapping

| SAS artifact | PROC / feature | PySpark replacement |
|---|---|---|
| `macros/connect_teradata.sas` | `LIBNAME` + `bulkload`/`fastload` | `session.DataLayer` reads/writes datasets; bulk-load options dropped (no Spark equivalent) |
| `01_sas_customer_segments.sas` | `PROC STDIZE method=std` | `pyspark.ml.feature.StandardScaler(withMean, withStd)` |
| | `PROC FASTCLUS maxclusters=5` | `pyspark.ml.clustering.KMeans(k=5, maxIter=50, tol=0.001)` |
| `02_sas_txn_analytics.sas` | `PROC SQL` customer aggregation | `groupBy().agg(...)` |
| | `PROC RANK groups=100` | window `rank`/`count` with the PROC RANK GROUPS formula |
| | `PROC MEANS median=/qrange=` | exact `percentile()` aggregation (IQR anomaly rule) |
| `03_sas_risk_scoring.sas` | `PROC LOGISTIC` (PD model) | `pyspark.ml.classification.LogisticRegression` |
| | composite scoring data step | `withColumn` + UDF for the top-2 driver loop |
| `04_sas_data_products.sas` | data step 4-way `MERGE` | left joins on `customer_id` with default handling |
| | `COLLECT STATISTICS` | dropped (no Spark equivalent) |

## Statistical differences / approximations

- **K-means cluster ids** are non-deterministic across engines; segment *names*
  are assigned by ordering clusters on average balance (same rule as SAS), but the
  raw `segment_id` numbering will not match SAS run-for-run.
- **PROC RANK groups=100** yields integer percentile buckets `0..99`
  (`spend_percentile`), reproducing SAS TIES=MEAN + GROUPS behaviour, rather than
  a continuous percentile.
- **Logistic regression**: SAS `selection=stepwise` has no direct Spark
  equivalent; the port fits `LogisticRegression` on the full predictor set.
  `probability_of_default` will differ numerically from the SAS model. If the
  target has a single class the job falls back to the base rate.
- **PROC MEANS quantiles**: Spark `percentile()` uses a different interpolation
  than SAS `PCTLDEF=5`, so median/IQR (and thus the anomaly flag) may differ at
  the margins.
- **`top_spend_category`** follows SAS `MAX()` semantics (alphabetically last
  category across a customer's accounts).
- Monetary fields are not additionally rounded (the SAS code does not round
  `interest_income` / `revenue_contribution`); downstream storage may apply
  `DECIMAL(_,2)` scale.

## Running

```bash
pip install -r spark/requirements.txt

# Full pipeline against the committed sample data (lower the row-count gate):
PIPELINE_MIN_ROWS=1 ./spark/run_spark_pipeline.sh

# Or a single job:
PIPELINE_MIN_ROWS=1 python -m spark.jobs.customer_segments
```

All paths, the Spark master, model hyper-parameters, and the data-quality gate
are configured via environment variables (see `spark_pipeline.cfg` and
`config.py`). SAS used `min_rows=1000`; override `PIPELINE_MIN_ROWS` to run
against the smaller committed sample.

## Tests

```bash
python -m pytest spark/tests -q
```
