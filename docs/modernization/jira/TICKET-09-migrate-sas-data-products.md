# TICKET-09 — Migrate sas/04_sas_data_products.sas and shared SAS macros to Synapse Spark in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `azure-synapse`, `pyspark`, `sas`
- **Depends on (blocked by):** TICKET-06, TICKET-07, TICKET-08, TICKET-03
- **Blocks:** TICKET-10

## Context

04_sas_data_products.sas assembles the golden record CUSTOMER_MASTER_PROFILE with a 4-way data step MERGE (IN= outer-join semantics) over the three data products plus STG_CUSTOMER_360, and the pipeline relies on three shared macros for connectivity, logging and validation.

## Scope

- Reimplement the 4-way MERGE as PySpark outer joins over CUSTOMER_SEGMENTS, TRANSACTION_ANALYTICS, CUSTOMER_RISK_SCORES and STG_CUSTOMER_360, preserving IN= based default handling for missing members.
- Port the default-value handling and the completeness reporting output.
- Replace %connect_teradata with a Python Snowflake connection utility that pulls credentials from Azure Key Vault (per TICKET-02).
- Replace %log_step with a Python logging utility writing to the shared run-log table.
- Replace %validate_table with a Python validation utility supporting row-count, null-rate and threshold assertions that abort the run (the %ABORT CANCEL equivalent).
- Package the utilities as a shared, unit-tested Python module used by all Synapse Spark jobs.

## Acceptance criteria

- [ ] CUSTOMER_MASTER_PROFILE reconciles with the SAS golden record row for row, including customers missing from one or more upstream products.
- [ ] Completeness reporting reproduces the SAS metrics.
- [ ] The three macros are fully replaced by the shared Python module and no job references SAS macros.
- [ ] Validation failures abort the run and are recorded in the run-log.
- [ ] The shared module has unit tests running in CI.

## Affected files

- `sas/04_sas_data_products.sas`
- `sas/macros/connect_teradata.sas`
- `sas/macros/log_step.sas`
- `sas/macros/validate_table.sas`
- `ddl/02_data_product_tables.sql`

## Dependencies

- Blocked by TICKET-06
- Blocked by TICKET-07
- Blocked by TICKET-08
- Blocked by TICKET-03
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
