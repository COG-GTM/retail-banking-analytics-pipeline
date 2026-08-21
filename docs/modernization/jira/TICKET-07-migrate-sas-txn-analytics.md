# TICKET-07 — Migrate sas/02_sas_txn_analytics.sas to Synapse Spark in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `azure-synapse`, `pyspark`, `sas`
- **Depends on (blocked by):** TICKET-04
- **Blocks:** TICKET-09, TICKET-10

## Context

02_sas_txn_analytics.sas ranks customer spend into percentiles with PROC RANK and flags anomalies using IQR bounds derived from PROC MEANS, publishing TRANSACTION_ANALYTICS.

## Scope

- Reimplement percentile ranking (PROC RANK GROUPS) in PySpark using window functions (ntile/percent_rank), matching SAS tie-handling.
- Reimplement the PROC MEANS quartile computation and IQR-based anomaly bounds (Q1 - 1.5*IQR, Q3 + 1.5*IQR) in PySpark, matching the SAS quantile definition.
- Port spend-trend metrics and anomaly flags to the same output columns.
- Read STG_TXN_SUMMARY from and write TRANSACTION_ANALYTICS to Snowflake via the Spark connector.
- Package as a Synapse Spark job invoked by the orchestration pipeline.

## Acceptance criteria

- [ ] Percentile buckets match the SAS output for the reconciliation dataset, including tied values.
- [ ] IQR bounds and resulting anomaly flags match SAS within an agreed tolerance, with the quantile definition documented.
- [ ] TRANSACTION_ANALYTICS is written to Snowflake with the same schema as the existing data product.
- [ ] Row-count and null-rate validation runs before publish.
- [ ] The job runs unattended on Synapse Spark and fails loudly on validation errors.

## Affected files

- `sas/02_sas_txn_analytics.sas`
- `ddl/02_data_product_tables.sql`

## Dependencies

- Blocked by TICKET-04
- Blocks TICKET-09
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
