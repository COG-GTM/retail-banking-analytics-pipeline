# TICKET-08 — Migrate sas/03_sas_risk_scoring.sas to Synapse Spark in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `azure-synapse`, `pyspark`, `sas`, `ml`
- **Depends on (blocked by):** TICKET-05, TICKET-03
- **Blocks:** TICKET-09, TICKET-10

## Context

03_sas_risk_scoring.sas fits a stepwise PROC LOGISTIC model over STG_RISK_FACTORS and STG_CUSTOMER_360, blends the predicted probability into a weighted composite risk score, assigns LOW/MODERATE/ELEVATED tiers using RISK_SCORE_THRESHOLD and derives the top risk drivers per customer.

## Scope

- Reimplement the logistic regression in PySpark (MLlib LogisticRegression or statsmodels/scikit-learn on the driver) including an explicit replacement for SAS stepwise variable selection.
- Port the weighted composite risk score formula and the risk-tier classification, preserving RISK_SCORE_THRESHOLD (currently 700) as an injected parameter.
- Reimplement the risk-driver logic that reports the top contributing factors per customer.
- Persist model coefficients and selected variables for auditability alongside each scoring run.
- Write CUSTOMER_RISK_SCORES to Snowflake and run the job on Synapse Spark.

## Acceptance criteria

- [ ] Model coefficients and the selected variable set are documented and compared against the SAS model, with differences explained.
- [ ] Composite scores and tier assignments reconcile with SAS within an agreed tolerance for the reconciliation cohort.
- [ ] RISK_SCORE_THRESHOLD is a pipeline parameter, not a hardcoded value.
- [ ] Risk drivers match the SAS driver logic for a sampled set of customers.
- [ ] CUSTOMER_RISK_SCORES is validated for row counts, score ranges and tier distribution before publish.

## Affected files

- `sas/03_sas_risk_scoring.sas`
- `config/pipeline_config.cfg`
- `ddl/02_data_product_tables.sql`

## Dependencies

- Blocked by TICKET-05
- Blocked by TICKET-03
- Blocks TICKET-09
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
