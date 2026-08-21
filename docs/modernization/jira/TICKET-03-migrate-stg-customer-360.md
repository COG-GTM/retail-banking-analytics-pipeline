# TICKET-03 — Migrate bteq/01_stg_customer_360.bteq to Snowflake SQL/dbt in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `snowflake`, `dbt`, `bteq`
- **Depends on (blocked by):** TICKET-01, TICKET-02
- **Blocks:** TICKET-06, TICKET-08, TICKET-09, TICKET-10

## Context

01_stg_customer_360.bteq builds the denormalized STG_CUSTOMER_360 view of customers, accounts and addresses using LEFT JOINs, QUALIFY ROW_NUMBER deduplication and CASE-based product flags. Its control flow depends on BTEQ dot commands that have no Snowflake equivalent.

## Scope

- Rewrite the customer-360 denormalization as a Snowflake SQL / dbt model producing STG_CUSTOMER_360.
- Port the LEFT JOINs across CUSTOMERS, ACCOUNTS and ADDRESSES, the QUALIFY ROW_NUMBER primary-address selection, CASE-derived product flags (HAS_CHECKING/HAS_SAVINGS/HAS_CREDIT/HAS_LOAN), tenure/age derivations and the credit utilization percentage.
- Replace .SET ERRORLEVEL / .IF ERRORCODE <> 0 THEN .EXIT fail-fast handling with dbt run failure semantics or Synapse activity failure propagation.
- Replace the .IF ACTIVITYCOUNT = 0 THEN .EXIT 99 zero-row check with a dbt test or explicit row-count assertion.
- Replace the ETL_RUN_LOG audit insert with the shared Snowflake run-log mechanism.
- Drop COLLECT STATISTICS calls.

## Acceptance criteria

- [ ] STG_CUSTOMER_360 in Snowflake matches the Teradata output row for row on a reconciliation run (row counts and column-level checksums).
- [ ] Credit utilization, tenure months, age and all product flags reconcile exactly, including divide-by-zero and NULL handling.
- [ ] A zero-row result fails the run rather than publishing an empty table.
- [ ] Each run writes a row to the replacement run-log with step name, row counts and duration.
- [ ] Model is covered by dbt tests for uniqueness of CUSTOMER_ID and not-null of key columns.

## Affected files

- `bteq/01_stg_customer_360.bteq`
- `ddl/01_staging_tables.sql`

## Dependencies

- Blocked by TICKET-01
- Blocked by TICKET-02
- Blocks TICKET-06
- Blocks TICKET-08
- Blocks TICKET-09
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
