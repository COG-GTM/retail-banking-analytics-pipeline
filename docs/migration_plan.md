# BTEQ + SAS → PySpark Migration Plan

Step-by-step plan for migrating the retail banking analytics pipeline off
Teradata BTEQ and SAS 9.4 onto PySpark, with data-parity validation at every
stage. The PySpark implementation in `pyspark/` and the validation harness in
`validation/` accompany this plan and have been executed locally against the
checked-in reference data (**143/143 parity checks pass** — see
[Validation results](#7-validation-results)).

See [`data_lineage.md`](data_lineage.md) (and `data_lineage.png` /
`data_lineage.svg`) for the full table-level lineage and dependency graph.

---

## 1. Current-state inventory

| Artifact | Type | Purpose | Reads | Writes |
|---|---|---|---|---|
| `bteq/01_stg_customer_360.bteq` | BTEQ | Denormalized customer master | customers, addresses, accounts | `STG_CUSTOMER_360` |
| `bteq/02_stg_txn_summary.bteq` | BTEQ | 12-month account txn rollup | transactions, accounts, transaction_types | `STG_TXN_SUMMARY` |
| `bteq/03_stg_risk_factors.bteq` | BTEQ | Behavioral risk indicators | transactions, accounts, transaction_types, customer_bureau_scores | `WRK_DAILY_BALANCE`, `WRK_PAYMENT_HISTORY`, `STG_RISK_FACTORS` |
| `sas/01_sas_customer_segments.sas` | SAS | K-means segmentation (FASTCLUS) | `STG_CUSTOMER_360` | `CUSTOMER_SEGMENTS` |
| `sas/02_sas_txn_analytics.sas` | SAS | Customer txn analytics (RANK/MEANS) | `STG_TXN_SUMMARY` | `TRANSACTION_ANALYTICS` |
| `sas/03_sas_risk_scoring.sas` | SAS | Composite risk score (LOGISTIC) | `STG_RISK_FACTORS`, `STG_CUSTOMER_360` | `CUSTOMER_RISK_SCORES` |
| `sas/04_sas_data_products.sas` | SAS | Golden-record assembly | `STG_CUSTOMER_360` + 3 products | `CUSTOMER_MASTER_PROFILE` |
| `orchestration/run_full_pipeline.sh` | Shell | Sequential orchestration | — | `ETL_RUN_LOG` |
| `config/pipeline_config.cfg` | Config | Run date, lookback, thresholds, DB names | — | — |

## 2. Target architecture

```
Bronze (source CSVs / Delta)        Silver (staging)              Gold (data products)
customers, accounts, addresses  →   stg_customer_360          →   customer_segments
transactions, transaction_types →   stg_txn_summary           →   transaction_analytics
customer_bureau_scores          →   stg_risk_factors          →   customer_risk_scores
                                                              →   customer_master_profile
```

- One PySpark module per legacy script (`pyspark/*.py`), pure functions taking
  a `SparkSession` + input DataFrames and returning a DataFrame.
- Parameterized `run_date` / `lookback_months` (replaces `CURRENT_DATE` and
  the volatile `RUN_PARAMS` table) so any historical run is reproducible.
- `pyspark/run_pipeline.py` replaces `orchestration/run_full_pipeline.sh`.
- Paths built with `pathlib.Path` relative to the repo root — no hardcoding.

## 3. Step-by-step migration procedure

### Phase 0 — Foundations
1. Stand up the PySpark environment (local: `python -m venv venv && pip install pyspark pandas numpy pyarrow python-dateutil`).
2. Pin the reference `RUN_DATE` (2026-04-10 for the checked-in data) so every
   translation can be validated against the golden CSVs.
3. Land the source extracts (`data/01_source_tables/`) as the Bronze layer.

### Phase 1 — BTEQ staging layer
4. Translate `01_stg_customer_360.bteq` → `pyspark/stg_customer_360.py`
   (Spark SQL; `QUALIFY ROW_NUMBER` → window function + filter).
5. Translate `02_stg_txn_summary.bteq` → `pyspark/stg_txn_summary.py`
   (volatile `RUN_PARAMS` table → Python parameters; `TOP_CAT` derived table →
   CTE with `ROW_NUMBER`).
6. Translate `03_stg_risk_factors.bteq` → `pyspark/stg_risk_factors.py`
   (the `WRK_DAILY_BALANCE` / `WRK_PAYMENT_HISTORY` work tables become CTEs).
7. Validate each staging output column-by-column against
   `data/02_bteq_staging/*.csv` before moving on (Layer 1 of
   `validation/validate_parity.py`).

### Phase 2 — SAS analytics layer
8. Translate `01_sas_customer_segments.sas` → `pyspark/customer_segments.py`
   (PROC STDIZE → `StandardScaler`, PROC FASTCLUS → `pyspark.ml` `KMeans`).
9. Translate `02_sas_txn_analytics.sas` → `pyspark/txn_analytics.py`
   (PROC RANK → `cume_dist` window; PROC MEANS median/IQR → `percentile_approx`).
10. Translate `03_sas_risk_scoring.sas` → `pyspark/risk_scoring.py`
    (PROC LOGISTIC → `pyspark.ml` `LogisticRegression` with a degenerate-target
    fallback to the PD floor).
11. Translate `04_sas_data_products.sas` → `pyspark/master_profile.py`
    (SAS `MERGE ... IF _base` → left joins + `coalesce` defaults).
12. Validate deterministic columns exactly; validate model-assigned columns
    (cluster IDs) at distribution level (Layers 2–3 of the harness).

### Phase 3 — Orchestration & productionization
13. Replace `run_full_pipeline.sh` with `pyspark/run_pipeline.py` (done for
    local runs); in production wire the same functions into Airflow/Databricks
    Workflows, one task per module, with the dependency edges from
    `docs/data_lineage.md`.
14. Re-point Bronze ingestion from CSV to the real Teradata extracts (JDBC or
    nightly unloads) and write Silver/Gold as Delta/Parquet instead of CSV.
15. Recreate `ETL_RUN_LOG` auditing as a lightweight run-metadata table
    written by the orchestrator.
16. Add CI: run `validation/validate_parity.py` on every change while the
    legacy pipeline is still producing reference outputs (parallel-run
    period), then cut over and retire BTEQ/SAS.

## 4. Construct-mapping reference

| Legacy construct | PySpark equivalent |
|---|---|
| BTEQ `QUALIFY ROW_NUMBER() OVER (...)` | `ROW_NUMBER` window + `WHERE rn = 1` |
| Volatile tables (`RUN_PARAMS`) | Function parameters / CTEs |
| Teradata work tables (`WRK_*`) | CTEs (or cached DataFrames) |
| `CURRENT_DATE`, `ADD_MONTHS`, `EXTRACT` | Pinned `run_date` param, `ADD_MONTHS`, `YEAR`/`MONTH` |
| Teradata month difference | Calendar-boundary diff: `(Y2−Y1)*12 + (M2−M1)` |
| SAS DATA step | `withColumn` chains / `select` expressions |
| SAS `MERGE ... IF in_base` | Left joins + `coalesce` defaults |
| PROC SQL | Spark SQL |
| PROC MEANS (median, qrange) | `percentile_approx` aggregations |
| PROC RANK (percentiles) | `cume_dist()` window function |
| PROC STDIZE | `pyspark.ml.feature.StandardScaler` (withMean, withStd) |
| PROC FASTCLUS (k=5) | `pyspark.ml.clustering.KMeans(k=5)` |
| PROC LOGISTIC | `pyspark.ml.classification.LogisticRegression` |
| SAS macro variables | Python function parameters / config constants |

## 5. Numeric & semantic gotchas found during validation

These were discovered by running the parity harness — each caused real
mismatches until handled explicitly:

- **Date arithmetic**: age/tenure/months-since-last-late use calendar-boundary
  month/year differences, not `MONTHS_BETWEEN`/365.25-day approximations.
- **Rounding**: monetary outputs use banker's rounding (HALF_EVEN) on the
  scaled double (`bround(x*100)/100`), not Spark's default HALF_UP `round`.
- **Group boundaries**: tenure/age group cutoffs are inclusive (`<=`), which
  the SAS comments (`< 25`) understate.
- **Tie-breaking**: risk-driver ranking ties resolve in a fixed scan order
  (BUREAU_SCORE → TRANSACTION_VELOCITY → CREDIT_UTILIZATION →
  PAYMENT_BEHAVIOUR) after rounding components to 6 dp.
- **Aggregation grouping**: Spark requires `open_date` in the GROUP BY of the
  payment-history rollup (Teradata's optimizer is laxer about functional
  dependence).

## 6. Known, documented deviations

| Column | Deviation | Validation approach |
|---|---|---|
| `stg_risk_factors.new_merchant_cnt_30d` | Reference generator's "new merchant" rule differs from the BTEQ `NOT IN` subquery semantics this migration faithfully implements | Distribution-level check (means within tolerance) |
| `transaction_analytics.top_spend_category` | For multi-account customers the reference picks an arbitrary account's top category; migration keeps SAS `MAX()` semantics | Set-membership check (always within the customer's candidate set; exact for single-candidate customers) |
| `customer_segments.segment_id` / `segment_name` | Cluster assignments are model outputs, not bit-reproducible between SAS FASTCLUS and Spark ML KMeans | Distribution-level check (label sets, cluster count) |

## 7. Validation results

`venv/bin/python validation/validate_parity.py` (local PySpark 4.2.0,
run date 2026-04-10):

```
Layer 1 — BTEQ staging (source CSVs → PySpark vs data/02_bteq_staging):
  stg_customer_360   478 rows,  23/23 columns exact
  stg_txn_summary   1251 rows,  22/22 columns exact
  stg_risk_factors   478 rows,  17/17 deterministic columns exact
                     + new_merchant_cnt_30d distribution check (known deviation)

Layer 2 — SAS analytics (golden staging → PySpark vs data/03_sas_data_products):
  customer_segments        407 rows, all deterministic columns exact
  transaction_analytics    500 rows, all deterministic columns exact
  customer_risk_scores     407 rows, all columns exact (incl. drivers & tiers)

Layer 3 — golden-record assembly:
  customer_master_profile  407 rows, all 28 compared columns exact

Validation summary: 143/143 checks passed, 0 failed
```

End-to-end run: `pyspark/run_pipeline.py` produces all 7 outputs
(478 / 1,251 / 478 staging rows; 407 / 500 / 407 / 407 product rows).
