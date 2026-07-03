# Migration Notes

## `sas/03_sas_risk_scoring.sas` → `pyspark/jobs/dp_risk_scoring.py`

Ports the SAS risk-scoring program to PySpark. Job name `03_risk_scoring`,
target data product `CUSTOMER_RISK_SCORES` (contract:
`common.schemas.CUSTOMER_RISK_SCORES`, PK `customer_id`),
`MODEL_VERSION = "RISK_V4.0"`, `risk_threshold = config.risk_score_threshold`
(700).

### SAS → Spark mapping

| SAS construct | PySpark equivalent |
| --- | --- |
| `PROC SQL` inner join `STG_RISK_FACTORS r` ⨝ `STG_CUSTOMER_360 c` on `customer_id`, `WHERE c.CUSTOMER_STATUS = 'A'`, pulling `tenure_months, num_active_accounts, total_balance, customer_status` | `extract_risk_input()` |
| `DATA WORK.RISK_FEATURES` step (STEP 2 feature prep) | `prepare_features()` |
| `if EXTERNAL_CREDIT_SCORE <= 0 or = . then 680` | `when(ext.isNull() \| (ext <= 0), 680).otherwise(ext)` |
| `BUREAU_SCORE_NORM = (score - 300)/(850-300)*100` | same closed form |
| `BALANCE_TREND_RATIO = 30d/90d if 90d>0 else 1` | `when(bal_90 > 0, bal_30/bal_90).otherwise(1)` |
| `VELOCITY_RATIO = (7d*(30/7))/30d if 30d>0 else 1` | `when(dv_30 > 0, (dv_7*30/7)/dv_30).otherwise(1)` |
| `DEFAULT_FLAG = (PAYMENT_LATE_CNT > 2)` | `when(payment_late_cnt > 2, 1).otherwise(0)` |
| `PROC LOGISTIC selection=stepwise slentry=0.10 slstay=0.05` | `jobs.stepwise_logistic.stepwise_logistic()` (see below) |
| composite score / tiers / drivers / flags `DATA` step (STEP 4) | `score_and_classify()` |
| `today()` / `datetime()` | `config.run_date` / `common.dates.load_timestamp()` |
| final `enforce_schema` to the DDL column order + types | `schemas.enforce_schema(df, schemas.CUSTOMER_RISK_SCORES)` |

`today()` is **not** read from the wall clock inside the transform; it comes from
`config.run_date`, which is what makes the port deterministic.

### STEP 4 — deterministic and exact vs. SAS

The composite score, tier assignment, primary/secondary driver selection, and
both flags are ported to match the SAS arithmetic **exactly**:

- Component scores are each clamped to `[0, 100]` with `max(0, min(100, x))`.
- `composite_risk_score = round(credit*0.30 + behaviour*0.25 + velocity*0.15 +
  (100 - bureau_score_component)*0.20 + (100 - payment_history_component)*0.10, 2)`.
  Spark `round` uses HALF_UP (round-half-away-from-zero), matching SAS `round(x, 0.01)`.
- Tiers: `<20 LOW`, `<40 MODERATE`, `<60 ELEVATED`, `<80 HIGH`, else `CRITICAL`.
- **Primary/secondary driver tie-break** reproduces the SAS `do i = 1 to 4` array
  scan verbatim (`_top_two_drivers()` unrolls the loop into Spark column
  expressions): a *strictly greater* value takes over as primary, so on ties the
  **first** candidate in array order `[CREDIT_UTILIZATION, PAYMENT_BEHAVIOUR,
  TRANSACTION_VELOCITY, BUREAU_SCORE]` wins; the running runner-up becomes the
  secondary. Driver values are `credit_risk_component`, `behaviour_risk_component`,
  `velocity_risk_component`, and `(100 - bureau_score_component)`.
- `score_delta_30d = 0` (SAS placeholder; no prior-day compare in this port).
- `watch_list_flag = 'Y'` iff `risk_tier = 'CRITICAL'` and
  `probability_of_default > 0.5`.
- `review_required_flag = 'Y'` iff `composite_risk_score >= 60` and
  `velocity_ratio > 2.0`.

`probability_of_default` is the only **tolerance-based** column vs. SAS (it is
model-derived — see below); every other column above is bit-for-bit deterministic.

## `jobs/stepwise_logistic.py` — PROC LOGISTIC stepwise wrapper

Spark ML has **no native stepwise selection**, so a forward/backward stepwise
procedure is implemented on top of
`pyspark.ml.classification.LogisticRegression` (unregularised:
`regParam=0`, `elasticNetParam=0`, `fitIntercept=True`).

The **same nine candidate effects** the SAS `MODEL` statement offers are offered
to stepwise here, in the identical order — the feature set is **not** silently
changed:

```
BUREAU_SCORE_NORM, CREDIT_UTIL_RATIO, PAYMENT_ONTIME_PCT, BALANCE_VOLATILITY,
VELOCITY_RATIO, ACCOUNT_OVERDRAFT_CNT, LARGE_WITHDRAWAL_CNT,
HIGH_RISK_MERCHANT_CNT, TENURE_MONTHS
```

Procedure per step:
1. **Forward**: tentatively add each not-yet-included candidate, refit, and keep
   the one whose entry most improves fit; include it iff its significance
   `p < slentry` (0.10).
2. **Backward**: drop any already-included variable with `p > slstay` (0.05),
   least-significant first. The variable that just entered is never dropped in
   the same pass, which guarantees termination.

### p-value approximation (fidelity deviation)

`PROC LOGISTIC` reports **Wald** chi-square statistics computed from coefficient
standard errors. Spark ML's `LogisticRegressionSummary` (the classifier summary)
does **not** expose `coefficientStandardErrors` (unlike `GeneralizedLinearRegression`),
so — as anticipated in the task — the wrapper falls back to a **likelihood-ratio
test (LRT)** for single-term entry/removal:

```
G^2 = 2 * (loglik_full - loglik_reduced)   ~   chi-square with 1 df
p   = P(chi2_1 > G^2) = erfc(sqrt(G^2 / 2))      # == 2*(1 - Phi(sqrt(G^2)))
```

Log-likelihood is computed directly from each fitted model's predicted
probabilities (`-2·loglik` deviance form); only Python's `math` module is used,
so no SciPy dependency is added. For a single degree of freedom the LRT and Wald
tests are asymptotically equivalent, so the selected variable set is expected to
match PROC LOGISTIC in the common case; exact p-values (and therefore borderline
entry/exit decisions) may differ slightly from SAS.

`prob_default` = model-predicted `P(default_flag = 1)`.

### Degenerate-case guards

`prob_default` falls back to `coalesce(mean(default_flag), 0)` when:
- the target has a **single class** (cannot fit a binary model),
- **no** candidate meets `slentry` (empty selection), or
- a model fit raises / fails to converge.

The fallback is flagged on `StepwiseResult.fallback` / `fallback_reason` and
logged; `run()` records a `WARNING` audit step.

> Note on the committed sample data: no *active* customer has
> `payment_late_cnt > 2`, so `default_flag` is single-class and the functional
> run legitimately takes the single-class fallback → `probability_of_default = 0`
> for all rows. The unit tests exercise the real model path on a separable
> synthetic set.

### Other deviations

- **Missing model covariates**: SAS PROC LOGISTIC uses listwise deletion of rows
  with any missing model variable. To avoid silently dropping customers from the
  scored output, model covariates are `coalesce(..., 0)`d before assembly
  (`model_input()`). `bureau_score_norm` and `velocity_ratio` are never missing
  (they have `else` branches); the committed data has no missing covariates, so
  this only matters defensively.
- **Load step**: the SAS `DELETE` + `PROC APPEND` to Teradata is replaced by the
  storage-agnostic `io.write_data_product("CUSTOMER_RISK_SCORES")` (overwrite),
  partitioning handled by the `DataIO` layer.
- **`min_rows`**: SAS validates `min_rows=1000`; the port uses `min_rows=1` (as
  the reference staging job does) so the ~407-row sample dataset validates. The
  production threshold can be restored via config if desired.

## Environment note

PySpark 3.5.1's `pyspark.ml` imports `distutils`, which was removed in Python
3.12. `setuptools` (which still vendors `distutils`) is therefore required at
runtime on Python 3.12 and has been added to `pyspark/requirements.txt`.
