# TICKET-06 — Migrate sas/01_sas_customer_segments.sas to Synapse Spark in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `azure-synapse`, `pyspark`, `sas`, `ml`
- **Depends on (blocked by):** TICKET-03
- **Blocks:** TICKET-09, TICKET-10

## Context

01_sas_customer_segments.sas standardizes engineered features with PROC STDIZE and clusters customers into five behavioural segments with PROC FASTCLUS, then loads CUSTOMER_SEGMENTS with LTV, engagement and action flags.

## Scope

- Reimplement the feature engineering step (tenure, balance, product-holding and engagement features) in PySpark reading STG_CUSTOMER_360 from Snowflake.
- Replace PROC STDIZE with a Spark ML StandardScaler (or equivalent) using the same standardization method.
- Replace PROC FASTCLUS k-means (k=5) with Spark MLlib KMeans or scikit-learn, with a fixed seed for reproducibility.
- Reproduce the segment labelling rules, LTV score, digital engagement metric and upsell/cross-sell/retention action flags.
- Write CUSTOMER_SEGMENTS back to Snowflake using the Snowflake Spark connector, and run the job as a Synapse Spark notebook/job definition.

## Acceptance criteria

- [ ] The PySpark job runs on Synapse Spark and writes CUSTOMER_SEGMENTS to Snowflake.
- [ ] Cluster assignments align with the SAS baseline for an agreed majority of customers (documented threshold), with differences explained by cluster-label permutation only.
- [ ] Segment labels, LTV scores and action flags reconcile with the SAS output for the reconciliation cohort.
- [ ] The run is reproducible: same input plus same seed yields identical assignments.
- [ ] Row counts and null rates on CUSTOMER_SEGMENTS are validated before publish.

## Affected files

- `sas/01_sas_customer_segments.sas`
- `ddl/02_data_product_tables.sql`

## Dependencies

- Blocked by TICKET-03
- Blocks TICKET-09
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
