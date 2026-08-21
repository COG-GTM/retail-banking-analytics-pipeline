# TICKET-04 — Migrate bteq/02_stg_txn_summary.bteq to Snowflake SQL/dbt in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `snowflake`, `dbt`, `bteq`
- **Depends on (blocked by):** TICKET-01, TICKET-02
- **Blocks:** TICKET-07, TICKET-10

## Context

02_stg_txn_summary.bteq aggregates transactions per customer over a configurable lookback window, using Teradata volatile tables to hold run parameters and computing channel-mix percentages alongside SUM/AVG/COUNT metrics.

## Scope

- Rewrite the transaction aggregation as a Snowflake SQL / dbt model producing STG_TXN_SUMMARY.
- Port SUM/AVG/COUNT metrics, debit/credit/fee splits, revenue-flag aggregation and channel-mix percentage calculations.
- Replace Teradata volatile parameter tables with dbt vars / Snowflake session variables or Synapse pipeline parameters.
- Honor LOOKBACK_MONTHS (currently 12 in config/pipeline_config.cfg) as an injected parameter rather than a shell environment variable.
- Replace BTEQ error handling, ACTIVITYCOUNT validation and ETL_RUN_LOG inserts as in TICKET-03.

## Acceptance criteria

- [ ] STG_TXN_SUMMARY reconciles with the Teradata output for the same lookback window (row counts and per-column sums).
- [ ] Channel-mix percentages sum to 100 (within rounding tolerance) for every customer with transactions.
- [ ] Changing the lookback parameter changes the window with no code edit.
- [ ] Zero-row output fails the run and is logged.
- [ ] dbt tests cover grain uniqueness and non-negative amount metrics.

## Affected files

- `bteq/02_stg_txn_summary.bteq`
- `config/pipeline_config.cfg`
- `ddl/01_staging_tables.sql`

## Dependencies

- Blocked by TICKET-01
- Blocked by TICKET-02
- Blocks TICKET-07
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
