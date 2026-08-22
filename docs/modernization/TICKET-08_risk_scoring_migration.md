# TICKET-08 / MBA-2209 - `03_sas_risk_scoring.sas` to Azure Synapse Spark

PySpark replacement for the SAS risk scoring program. Source of truth for the
migrated logic is `synapse/risk_scoring/`; the SAS program is retained unchanged
for reference and reconciliation.

## Construct mapping

| SAS / Teradata construct | Synapse Spark implementation |
| --- | --- |
| `%connect_teradata` LIBNAMEs (`STGDB`, `DPDB`) | Snowflake Spark connector options (`synapse/risk_scoring/config.py`) |
| `PROC SQL` extract from `STG_RISK_FACTORS` + `STG_CUSTOMER_360`, `CUSTOMER_STATUS = 'A'` | `features.join_risk_inputs` |
| DATA step imputation, `BUREAU_SCORE_NORM`, `BALANCE_TREND_RATIO`, `VELOCITY_RATIO`, `DEFAULT_FLAG` | `features.prepare_features` |
| `PROC LOGISTIC ... selection=stepwise slentry=0.10 slstay=0.05` | `model.fit_stepwise` - MLlib `LogisticRegression` (unregularised) plus an explicit forward/backward loop driven by Wald p-values |
| PROC LOGISTIC `Standard Error` / Wald chi-square | `model.wald_p_values` - `(X'WX)^-1` observed information aggregated in Spark, inverted on the driver |
| `output out=... predicted=PROB_DEFAULT` | `model.score_probability` (positive-class probability) |
| DATA step components, weighted composite, tier `if/else` ladder | `scoring.classify_risk`, `scoring.RISK_TIER_BREAKPOINTS` |
| `array _comp[4] / _lbl[4]` top-two driver loop | `scoring._top_two_drivers` (loop replicated verbatim, including its tie and empty-secondary behaviour) |
| `%validate_table` + `PROC FREQ` tier distribution | `validation.validate_risk_scores` |
| `execute (DELETE FROM ...) by teradata` + `PROC APPEND` | `publish.publish_risk_scores` - single overwrite with `truncate_table=on`, `usestagingtable=on` |
| `%sysget(RISK_SCORE_THRESHOLD)` macro variable | `--risk-score-threshold` job argument (env fallback `RISK_SCORE_THRESHOLD`) |
| `MULTISET ... PRIMARY INDEX`, `TIMESTAMP(6)`, `COLLECT STATISTICS` | `ddl/snowflake/customer_risk_scores.sql` (indexes/stats dropped, `TIMESTAMP_NTZ(6)`) |

## Stepwise selection: differences from PROC LOGISTIC

* SAS uses the score chi-square for entry and the Wald chi-square for removal.
  MLlib exposes neither, so both directions use the Wald statistic computed from
  the observed information matrix. On well-conditioned data the two criteria
  agree on the retained variable set; the entry order can differ when candidate
  variables are strongly collinear.
* SAS drops observations with missing model variables (listwise deletion). The
  Spark fit uses `VectorAssembler(handleInvalid="skip")`, which is equivalent.
  Scoring uses `handleInvalid="keep"`, and a non-finite probability collapses to
  0, matching the SAS `coalesce(PROB_DEFAULT, 0)`.
* MLlib fits with L-BFGS and `standardization=true`; coefficients are returned on
  the original scale, so they are directly comparable to the SAS estimates, but
  small differences (typically <1e-6 relative) are expected from the optimiser
  and convergence tolerance.
* If the target has a single class or no candidate meets `slentry`, the model
  degenerates to the intercept-only model and every customer receives the
  observed event rate as `PROBABILITY_OF_DEFAULT` - the composite score, which
  does not depend on the model, is unaffected.

## Model auditability

Every run writes a JSON audit record (selected variables, coefficients,
intercept, Wald p-values, every enter/remove step, base rate, `slentry`,
`slstay`, `RISK_SCORE_THRESHOLD`) to:

* the job log,
* `--model-audit-path` on ADLS (one folder per run timestamp), and
* `DATA_PRODUCTS.RISK_MODEL_RUNS` in Snowflake.

The SAS program only produced `outmodel=WORK.RISK_MODEL`, which was deleted by
`PROC DATASETS` at the end of the run, so this is new capability required by the
acceptance criteria.

## Reconciliation

`COMPOSITE_RISK_SCORE`, the five components and the risk tier are deterministic
functions of the staging inputs, so they reconcile exactly with SAS apart from
floating-point rounding: SAS `round(x, 0.01)` and Spark `round(x, 2)` are both
half-up, so the agreed tolerance is 0.01 on the score and exact equality on the
tier for the reconciliation cohort. `PROBABILITY_OF_DEFAULT` is model dependent
and is reconciled against the SAS output with a tolerance of 1e-3 once the SAS
coefficient table for the cohort is available. Risk drivers are compared exactly;
`scoring._top_two_drivers` reproduces the SAS loop including its edge cases and
is covered by unit tests.

## Publish-gate validation

`validation.validate_risk_scores` fails the run (nothing is published) when:
row count < `--min-rows` (default 1000), `CUSTOMER_ID` is null or duplicated,
`COMPOSITE_RISK_SCORE` or `RISK_TIER` is null, `COMPOSITE_RISK_SCORE` falls
outside 0-100, `PROBABILITY_OF_DEFAULT` falls outside 0-1, or an unexpected tier
value appears. The tier distribution is logged for monitoring, replacing the
`PROC FREQ` / `PROC PRINT` block.

## Running on Synapse

```bash
spark-submit \
  --py-files synapse.zip \
  --packages net.snowflake:snowflake-jdbc:3.16.1,net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.4 \
  -m synapse.risk_scoring.job \
  --risk-score-threshold 700 \
  --model-audit-path abfss://models@<storage>.dfs.core.windows.net/risk_scoring
```

Snowflake credentials are read from the environment (`SNOWFLAKE_URL`,
`SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`,
`SNOWFLAKE_DATABASE`, `SNOWFLAKE_STAGING_SCHEMA`, `SNOWFLAKE_DATA_PRODUCT_SCHEMA`)
and should be sourced from Azure Key Vault by the Synapse pipeline.

## Assumptions

* The Snowflake database, warehouse and roles are created by TICKET-01/TICKET-02;
  this job only reads/writes tables. Schema names default to `ETL_STAGING` and
  `DATA_PRODUCTS` and are overridable per environment.
* `STG_RISK_FACTORS` (TICKET-05) and `STG_CUSTOMER_360` (TICKET-03) keep the
  column names and semantics of the Teradata staging tables.
* Synapse pipeline orchestration (triggers, dependency on the staging jobs) is
  owned by the orchestration ticket; this ticket delivers the job itself.
