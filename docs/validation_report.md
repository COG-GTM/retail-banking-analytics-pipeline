# Risk Scoring Migration — Validation Report

Scope: `sas/03_sas_risk_scoring.sas` (STEP 1 → STEP 6) and its upstream feature
builder `bteq/03_stg_risk_factors.bteq`, ported to PySpark under `risk_scoring/`.

Reproduce everything in this report with:

```bash
pip install -r requirements.txt
python -m pytest tests/ -q                          # 161 unit tests
python -m risk_scoring.driver --min-rows 400        # writes output/customer_risk_scores{,_csv}
python validation/compare_to_oracle.py              # writes validation/parity_report.md
```

Both commands exit non-zero on failure — the driver when the STEP 5 gate aborts
or the connection is misconfigured, the comparison when the exact regime fails
(pass `--allow-mismatch` to inspect a failing report without a non-zero exit), so
either can gate CI as written.

The machine-generated numbers live in
[`validation/parity_report.md`](../validation/parity_report.md); this document is
the interpretation and the catalogue of deviations.

## 1. Result summary

| Area | Result |
| --- | --- |
| Row coverage | 407 / 407, no key present on only one side |
| Deterministic fields (composite, all five components, tier, `SCORE_DELTA_30D`, `REVIEW_REQUIRED_FLAG`, `WATCH_LIST_FLAG`) | **exact parity, 0 mismatches** |
| Risk tier distribution | identical: LOW 290, MODERATE 113, ELEVATED 4 |
| `PRIMARY_RISK_DRIVER` / `SECONDARY_RISK_DRIVER` | diverge by design — the *oracle* deviates from the SAS source (§3.1) |
| `PROBABILITY_OF_DEFAULT` | 0.0 here vs a constant 0.05 in the oracle; the target is degenerate (§3.2) |

`WATCH_LIST_FLAG` is gated as an exact field even though it derives from
`PROBABILITY_OF_DEFAULT`: the `> 0.5` test is nowhere near being met, so it is
deterministic in practice and a change in it would be a real regression.

The end-to-end run is deterministic and re-runnable: the sink is a full
truncate-load, so a second run reproduces the first byte for byte apart from
`LOAD_TS`.

## 2. Environment discrepancy: the reference generator is missing

The task named `run_demo.phase3_python_analytics` (imported by `export_data.py`
from `local/duckdb/`) as the numeric oracle. **That file does not exist** — the
repository has no `local/` directory, and a filesystem-wide `find / -name
run_demo.py` returns only unrelated copies in other checkouts
(`~/repos/ETL-Pipeline/demo/run_demo.py`, `~/repos/etl-pipeline-demo/demo/run_demo.py`).
`export_data.py` is therefore broken on `main` as committed.

Substitute oracle used throughout: the committed *output* of that generator,
`data/03_sas_data_products/customer_risk_scores.csv` (407 rows), driven from the
committed inputs `data/02_bteq_staging/stg_risk_factors.csv` and
`stg_customer_360.csv` (478 rows each). This is a strictly weaker oracle — it
pins the values but not the code that produced them, which is why §3.1 could
only be diagnosed by inference rather than by reading the reference source.

## 3. Deviations

### 3.1 Risk-driver tie-breaking — the oracle disagrees with the SAS source

Whenever the `0..100` clamps are inactive, `CREDIT_RISK_COMPONENT` and
`(100 - BUREAU_SCORE_COMPONENT)` are the *same number* — both reduce to
`100 - BUREAU_SCORE_NORM`. Array index 0 (`CREDIT_UTILIZATION`) and index 3
(`BUREAU_SCORE`) therefore tie on essentially every row, and the top-2 selection
is decided entirely by the tie rule:

* **SAS** walks the array in order with a strict `>` against the running maximum,
  so the **first** index wins: primary `CREDIT_UTILIZATION`, secondary `BUREAU_SCORE`.
* **The oracle CSV** reports the opposite on most rows.

This port follows the SAS source, which is the migration's source of truth. The
cost is 294/407 `PRIMARY_RISK_DRIVER` and 407/407 `SECONDARY_RISK_DRIVER`
mismatches against the oracle. Chasing the oracle was rejected because no simple
rule reproduces it: flipping to last-index-wins still leaves 9 primary and 16
secondary mismatches, so the oracle's ordering is an artefact of an unstable sort
in code we cannot read (§2), not a specification.

`validation/compare_to_oracle.py` reports these two columns in their own section
and excludes them from the pass/fail verdict.

### 3.2 The logistic model target is degenerate on this dataset

`PAYMENT_LATE_CNT` is `0` for **all 478** staging rows, so
`DEFAULT_FLAG = (PAYMENT_LATE_CNT > 2)` is `0` for all 407 active customers. The
target has a single class, the logistic model is unidentifiable, and stepwise
selection has nothing to select: it reports "no predictors entered" and the
module emits the intercept-only MLE, `PROB_DEFAULT = 0.0` for every row, with a
`WARNING` in the log. No crash, no dropped rows.

The oracle emits a constant `0.05` instead. Nothing in the SAS program computes
`0.05`, so it is not reproduced — hardcoding it would be a magic constant that
silently masks the degeneracy. The divergence does not propagate: the only
consumer of the probability is `WATCH_LIST_FLAG` (`RISK_TIER = 'CRITICAL' AND
PROBABILITY_OF_DEFAULT > 0.5`), no row reaches the CRITICAL tier, and both `0.0`
and `0.05` are below `0.5`, so `WATCH_LIST_FLAG` is `N` on both sides.

**Consequence for reviewers: this dataset does not exercise the model.** The
stepwise implementation was validated separately against the real 407-row feature
distributions with a synthetic non-degenerate target
(`y ~ Bernoulli(sigmoid(-2 + 3 * CREDIT_UTIL_RATIO))`, 79 events): it entered
`CREDIT_UTIL_RATIO` (p = 0.0001) then `HIGH_RISK_MERCHANT_CNT` (p = 0.0633),
terminated cleanly, and recovered intercept -2.281 / slope +2.244 against the
true -2.0 / +3.0.

### 3.3 Model numerics are not reproducible against SAS by construction

`PROC LOGISTIC` uses Fisher scoring (IRLS); `pyspark.ml.classification.LogisticRegression`
uses LBFGS. Coefficients agree only to optimiser tolerance, so
`PROBABILITY_OF_DEFAULT` is comparable at the distribution level only, never row
by row. MLlib also exposes no p-values for `LogisticRegression`
(`coefficientStandardErrors` exists only on the linear/GLM summaries), so the
score-test entry and Wald exit statistics driving `selection=stepwise` are
computed in-module from the observed information matrix.

Open recommendation, deliberately **not** actioned here because the task and the
interface contract both mandate `LogisticRegression`:
`GeneralizedLinearRegression(family="binomial", solver="irls")` *is* Fisher
scoring and *does* expose `coefficientStandardErrors`/`pValues`. Switching would
remove the optimiser, standard-error and chi-square deviations at once.

### 3.4 SAS missing-value semantics

SAS treats missing (`.`) as less than any number, so `X <= 0 or X = .` is one
branch; in Spark a NULL comparison yields NULL. Explicit `isNull() | (col <= 0)`
predicates restore the SAS behaviour, and both division guards are written as
`when(denominator > 0, ...).otherwise(1.0)` so that a NULL denominator falls to
the SAS `else` branch rather than relying on Spark's null-on-divide.

Conversely `where COL is missing` in SAS is also true for *blank* character
values, so the ported null checks count NULL or blank/whitespace for string
columns; a naive `isNull()` would under-report (e.g. `RISK_TIER = ''`).

### 3.5 Validation gate: `min_rows`

The SAS macro call hardcodes `min_rows=1000`, but the committed extract yields
407 scored rows, so a literal port aborts STEP 5 on the only data available.
`min_rows` is therefore sourced from `PipelineConfig.min_rows`, which still
**defaults to 1000** — the production semantics are unchanged, and local runs
pass `--min-rows 400`. The failure asymmetry of `%validate_table` is preserved
exactly: a row-count or duplicate-key failure aborts (and short-circuits the
remaining checks, as the macro's `%return` does), while null checks only warn.

### 3.6 Sink and connectivity

* `DELETE FROM ... ; PROC APPEND ... FORCE` collapses to a single
  `mode("overwrite")` (`truncate=true` on JDBC). STEP 6 is now idempotent; the
  SAS version was not — a failure between the DELETE and the APPEND left the
  target empty.
* `COLLECT STATISTICS` is dropped; optimiser statistics are a warehouse-side concern.
* `PROC APPEND FORCE` silently dropped columns the target lacked. The sink instead
  raises `SinkContractError` naming the missing contract columns; extra input
  columns are still dropped silently, as `FORCE` did.
* The four `LIBNAME` statements of `connect_teradata.sas` — and their
  `{SAS004}XXXXXXXX` hardcoded passwords — become `Connections` over a pluggable
  backend. **No credential is ever read from source**: `JdbcBackend` resolves
  `$TD_PASSWORD` from the environment per operation and redacts it from every log
  line. `CsvBackend` needs no credentials at all, which is what makes the local
  parity run above possible.
* Reading the committed extracts required routing integral CSV casts through
  DOUBLE (`cast_csv_value`): Teradata `INTEGER` columns are serialised as
  `"13.0"`/`"0.0"` and Spark 4's ANSI `STRING -> INT` cast rejects them outright.

### 3.7 Smaller behavioural notes

* Unset risk drivers are emitted as NULL; SAS leaves the `$40` character
  variables as `''`. Affects rows where no component exceeds 0.
* `BALANCE_TREND_RATIO` is computed by SAS and consumed by nothing downstream.
  Ported anyway, and flagged in the code.
* Audit `LOG_TS` uses UTC (`spark.sql.session.timeZone=UTC`) rather than SAS
  server local time.
* A missing `not_null`/`key_cols` column raises; in SAS the `PROC SQL` step
  errored while `VALIDATION_RC` still read 0. That is a bug in the macro, not
  behaviour worth preserving.
* Rows with any NULL predictor are excluded from the model fit but still emitted
  with `PROB_DEFAULT = NULL` — no filter, no join, so the row count is preserved
  by construction, matching SAS `output out=`.

## 4. What is not covered

* No Teradata instance was reachable, so `JdbcBackend` is exercised only by unit
  tests over its generated options (URL, driver, `truncate`, redaction), not
  against a live database.
* `WATCH_LIST_FLAG`'s `> 0.5` branch and the CRITICAL tier are never reached by
  the committed data (§3.2); unit tests are their only coverage.
* `COLLECT STATISTICS` and the SAS-side `PROC PRINT` formatting have no
  equivalent and were dropped.
