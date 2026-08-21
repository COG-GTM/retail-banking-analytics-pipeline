# TICKET-01 — Snowflake DDL migration for source, staging and data product tables in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `snowflake`, `ddl`, `teradata`
- **Depends on (blocked by):** None
- **Blocks:** TICKET-03, TICKET-04, TICKET-05, TICKET-10

## Context

All table definitions are written in Teradata DDL and rely on Teradata-only physical storage constructs. Snowflake has no concept of primary indexes, fallback protection or manual statistics collection, so the DDL must be rewritten rather than lifted and shifted.

## Scope

- Translate every CREATE TABLE statement in ddl/00_source_tables.sql, ddl/01_staging_tables.sql and ddl/02_data_product_tables.sql to Snowflake syntax.
- Remove Teradata-only clauses: MULTISET, NO FALLBACK, PRIMARY INDEX, COLLECT STATISTICS, CHARACTER SET LATIN NOT CASESPECIFIC, RANGE_N partitioning and FORMAT column attributes.
- Map data types: TIMESTAMP(6) -> TIMESTAMP_NTZ, DECIMAL(p,s) -> NUMBER(p,s), CHAR(n)/VARCHAR(n) -> VARCHAR(n), BYTEINT/SMALLINT -> NUMBER, DATE -> DATE.
- Replace RANGE_N partitioning on transaction tables with Snowflake clustering keys where the access pattern justifies it.
- Define the Snowflake database and schema layout replacing CORE_BANKING_DB, TXN_PROCESSING_DB, ETL_STAGING_DB and DATA_PRODUCTS_DB, with environment suffixes for DEV/UAT/PROD.
- Keep the migrated DDL under source control as idempotent, re-runnable scripts (CREATE OR REPLACE / CREATE IF NOT EXISTS).

## Acceptance criteria

- [ ] Snowflake DDL scripts execute end to end against a clean Snowflake account with no syntax errors.
- [ ] Every table present in the Teradata DDL exists in Snowflake with equivalent column names, order, nullability and precision.
- [ ] No Teradata-only keyword (MULTISET, NO FALLBACK, PRIMARY INDEX, COLLECT STATISTICS, CASESPECIFIC, RANGE_N) remains in the migrated scripts.
- [ ] A documented type-mapping table records every Teradata type and its Snowflake equivalent.
- [ ] Scripts are idempotent and safe to re-run in DEV/UAT/PROD.

## Affected files

- `ddl/00_source_tables.sql`
- `ddl/01_staging_tables.sql`
- `ddl/02_data_product_tables.sql`

## Dependencies

- None
- Blocks TICKET-03
- Blocks TICKET-04
- Blocks TICKET-05
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
