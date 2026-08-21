# TICKET-05 — Migrate bteq/03_stg_risk_factors.bteq to Snowflake SQL/dbt in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `snowflake`, `dbt`, `bteq`
- **Depends on (blocked by):** TICKET-01, TICKET-02
- **Blocks:** TICKET-08, TICKET-10

## Context

03_stg_risk_factors.bteq engineers risk features through several intermediate work tables, computing STDDEV_POP-based volatility, 7-day and 30-day debit velocity and multi-pass joins before assembling STG_RISK_FACTORS, then drops its work tables.

## Scope

- Rewrite the risk feature engineering as Snowflake SQL / dbt models producing STG_RISK_FACTORS.
- Express the intermediate work tables as ephemeral/CTE dbt models or transient Snowflake tables instead of Teradata work tables plus explicit DROP cleanup.
- Port STDDEV_POP volatility measures, 7-day and 30-day debit velocity calculations and the multi-pass joins across TRANSACTIONS, ACCOUNTS and CUSTOMERS.
- Replace BTEQ error handling, ACTIVITYCOUNT validation and ETL_RUN_LOG inserts as in TICKET-03.
- Verify Snowflake window-function semantics match Teradata for the velocity windows, especially at partition boundaries and for customers with sparse activity.

## Acceptance criteria

- [ ] STG_RISK_FACTORS reconciles with the Teradata output row for row, including customers with no transactions in the window.
- [ ] Volatility and velocity features match Teradata within an agreed floating-point tolerance.
- [ ] No orphaned work tables remain after a run.
- [ ] Zero-row output fails the run and is logged.
- [ ] dbt tests cover grain uniqueness and feature range/not-null expectations.

## Affected files

- `bteq/03_stg_risk_factors.bteq`
- `ddl/01_staging_tables.sql`

## Dependencies

- Blocked by TICKET-01
- Blocked by TICKET-02
- Blocks TICKET-08
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
