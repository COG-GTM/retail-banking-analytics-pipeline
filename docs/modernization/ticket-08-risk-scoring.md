# TICKET-08 / MBA-2209 — `sas/03_sas_risk_scoring.sas` → Synapse Spark

The migrated job lives in `spark/jobs/03_risk_scoring.py` (package `spark/jobs/risk_scoring/`).
The SAS program is kept in place unchanged as the reference implementation.

## Running

```bash
source config/pipeline_config.cfg

# Synapse Spark / spark-submit against Snowflake
spark-submit spark/jobs/03_risk_scoring.py --source snowflake --run-id "$RUN_TIMESTAMP"

# Local reconciliation run against the CSV fixtures in data/
python spark/jobs/03_risk_scoring.py --source local --input-dir data --output-dir build/risk_scoring
```

Snowflake credentials are resolved at runtime from the environment
(`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`/`SNOWFLAKE_PRIVATE_KEY`,
`SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, staging/data-product schemas),
per TICKET-02; nothing is stored in source control.

## SAS → PySpark mapping

| SAS construct | PySpark equivalent |
| --- | --- |
| `PROC SQL` join of `STG_RISK_FACTORS` + `STG_CUSTOMER_360` | `features.extract_risk_raw` (inner join on active customers) |
| Feature DATA step (bureau imputation/normalisation, ratio guards, `DEFAULT_FLAG`) | `features.build_features` |
| `PROC LOGISTIC ... selection=stepwise slentry=0.10 slstay=0.05` | `model.stepwise_logistic` (statsmodels `Logit` on the driver, forward entry + backward elimination) |
| `output out= predicted=PROB_DEFAULT` | `model.probability_column` applied as a Spark expression (scoring stays distributed) |
| Composite score DATA step, tiers, driver array loop | `scoring.score_customers` |
| `%validate_table` + `PROC FREQ` monitoring | `validation.validate_risk_scores` (row count, key uniqueness, nulls, score/probability ranges, tier set and distribution) |
| `WORK.RISK_MODEL` (discarded at session end) | `RISK_MODEL_RUNS` + `RISK_MODEL_COEFFICIENTS` (`ddl/snowflake/03_risk_model_audit_tables.sql`) |
| `libname ... teradata` output | Snowflake Spark connector (`net.snowflake.spark.snowflake`) |

## Model and selected variables vs SAS

* Candidate variables are unchanged: `BUREAU_SCORE_NORM`, `CREDIT_UTIL_RATIO`,
  `PAYMENT_ONTIME_PCT`, `BALANCE_VOLATILITY`, `VELOCITY_RATIO`, `ACCOUNT_OVERDRAFT_CNT`,
  `LARGE_WITHDRAWAL_CNT`, `HIGH_RISK_MERCHANT_CNT`, `TENURE_MONTHS`.
* **Difference:** PROC LOGISTIC uses the score chi-square test for entry and the Wald test for
  removal. statsmodels exposes Wald p-values only, so entry and removal both use Wald. On
  well-conditioned data the two agree; where they do not, the actual selected set, the selection
  log and every coefficient are written to `RISK_MODEL_RUNS` / `RISK_MODEL_COEFFICIENTS` for the
  run, so any divergence is visible per run rather than silent.
* Constant or all-null candidates are skipped (PROC LOGISTIC would drop them as well) and the
  skip is logged.
* **Degenerate cohorts:** if the target has a single level, or no candidate reaches `slentry`,
  PROC LOGISTIC aborts. The Spark job optionally (`--allow-intercept-only-model`) continues with
  an intercept-only model — predicting the observed event rate — and records the reason in the
  audit tables, because the composite score, which carries the business logic, does not depend on
  the model. Without the flag the run fails, as SAS does.
* **Risk drivers:** the SAS array declares its fourth element as `(100 - BUREAU_SCORE_COMPONENT)`
  and ranks with `>`. Reproducing that literally disagrees with the SAS data product on ties
  (`CREDIT_RISK_COMPONENT` always equals `100 - BUREAU_SCORE_COMPONENT`). The published SAS output
  resolves ties in favour of the later array element, so `scoring._risk_drivers` uses `>=`; this
  reproduces the legacy drivers exactly on the reconciliation cohort.

## Parameters

`RISK_SCORE_THRESHOLD` (700) is read from `config/pipeline_config.cfg`, overridable with
`--risk-score-threshold`, and never hardcoded. As in SAS it does not enter tier assignment (tier
boundaries are on the 0–100 composite scale); it is applied as the published monitoring metric
`subprime_bureau_pct` — the share of the cohort scoring below the bureau threshold. Composite
weights, tier boundaries and the candidate feature list are parameters too
(`RISK_COMPOSITE_WEIGHTS`, `RISK_TIER_BOUNDARIES`, `RISK_CANDIDATE_FEATURES`).

## Validation performed

No Snowflake or Synapse environment is available, so validation is local:

* `python -m pytest spark/tests` — 8 unit tests over features, composite score, tiers, drivers,
  parameterisation, validation failures, stepwise selection and reconciliation.
* Full local job run over `data/02_bteq_staging/` (407 customers).
* `spark/jobs/risk_scoring/reconcile.py` against the SAS data product
  `data/03_sas_data_products/customer_risk_scores.csv`:

  ```json
  {"baseline_rows": 407, "candidate_rows": 407, "matched_rows": 407,
   "max_score_diff": 0.0, "tier_agreement_pct": 100.0,
   "primary_driver_agreement_pct": 100.0, "secondary_driver_agreement_pct": 100.0,
   "score_breaches": 0}
  ```

  Probabilities are excluded from that comparison (`--ignore-probability`): the fixture cohort has
  no default events at all (`PAYMENT_LATE_CNT` is 0 for every row), so no model can be fitted from
  it, while the SAS fixture carries a constant placeholder probability of 0.05.
