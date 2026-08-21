# Teradata -> Snowflake DDL Migration (MBA-2202 / TICKET-01)

Migrated scripts live in `ddl/snowflake/`. The original Teradata DDL under `ddl/`
is retained unchanged as the system of record for the legacy platform.

| Teradata script | Snowflake script |
|---|---|
| (database refs in `config/pipeline_config.cfg`) | `ddl/snowflake/00_databases_schemas.sql` |
| `ddl/00_source_tables.sql` | `ddl/snowflake/01_source_tables.sql` |
| `ddl/01_staging_tables.sql` | `ddl/snowflake/02_staging_tables.sql` |
| `ddl/02_data_product_tables.sql` | `ddl/snowflake/03_data_product_tables.sql` |

## Data type mapping

| Teradata type | Snowflake type | Notes |
|---|---|---|
| `BIGINT` | `NUMBER(19,0)` | Snowflake's `BIGINT` alias resolves to `NUMBER(38,0)`; the explicit precision preserves the Teradata range and keeps column metadata comparable. |
| `INTEGER` | `NUMBER(10,0)` | Same rationale; matches the 32-bit signed range. |
| `SMALLINT` | `NUMBER(5,0)` | Matches the 16-bit signed range. |
| `BYTEINT` | `NUMBER(3,0)` | Not present in the current DDL; recorded for completeness. |
| `DECIMAL(p,s)` | `NUMBER(p,s)` | Identical semantics; precision and scale preserved 1:1. |
| `CHAR(n)` | `VARCHAR(n)` | Snowflake `CHAR(n)` does not blank-pad, so `VARCHAR(n)` is the honest equivalent and avoids trailing-space comparison surprises. |
| `VARCHAR(n)` | `VARCHAR(n)` | Length preserved. |
| `DATE` | `DATE` | `FORMAT 'YYYY-MM-DD'` is a Teradata display attribute only; formatting moves to the consuming query/session (`DATE_OUTPUT_FORMAT`). |
| `TIMESTAMP(6)` | `TIMESTAMP_NTZ(6)` | Legacy timestamps carry no timezone; `TIMESTAMP_NTZ` keeps that semantic explicit rather than depending on `TIMESTAMP_TYPE_MAPPING`. |

## Teradata-only constructs removed

| Construct | Snowflake handling |
|---|---|
| `MULTISET` | Dropped. Snowflake tables are always multiset; uniqueness is a modelling concern. |
| `NO FALLBACK` | Dropped. Replication/durability is managed by the service. |
| `PRIMARY INDEX (...)` | Dropped. There is no data distribution to control; Snowflake micro-partitions automatically. |
| `UNIQUE PRIMARY INDEX (...)` | Modelled as a non-enforced `UNIQUE` constraint on `TRANSACTION_TYPES` for documentation/optimizer purposes. |
| `COLLECT STATISTICS ...` | Dropped. Statistics are collected automatically per micro-partition. |
| `CHARACTER SET LATIN NOT CASESPECIFIC` | Dropped. Snowflake stores UTF-8; case-insensitive comparison is done per query (`COLLATE`/`ILIKE`/`UPPER`) rather than per column, so string comparisons that relied on Teradata's case-insensitive default must be made explicit in downstream SQL. |
| `FORMAT 'YYYY-MM-DD'` | Dropped (display attribute only). |
| `PARTITION BY RANGE_N(TRANSACTION_DATE ... EACH INTERVAL '1' MONTH)` | `CLUSTER BY (TRANSACTION_DATE)` on `TXN_PROCESSING.TRANSACTIONS` - every downstream read filters on a date window. |
| `PARTITION BY COLUMN(REPORTING_PERIOD VARCHAR(7))` | `CLUSTER BY (REPORTING_PERIOD)` on `DATA_PRODUCTS.TRANSACTION_ANALYTICS` - consumers filter by reporting month. |

Clustering keys are declared only on those two tables; the remaining tables are
small enough that automatic micro-partitioning is sufficient and a clustering
key would only add maintenance cost.

## Database and schema layout

Teradata's four databases become four schemas inside one environment-suffixed
database, so DEV/UAT/PROD are isolated and cross-schema joins stay within a
single database:

| Teradata database | Snowflake object |
|---|---|
| `CORE_BANKING_DB` | `RETAIL_BANKING_<ENV>.CORE_BANKING` |
| `TXN_PROCESSING_DB` | `RETAIL_BANKING_<ENV>.TXN_PROCESSING` |
| `ETL_STAGING_DB` | `RETAIL_BANKING_<ENV>.ETL_STAGING` |
| `DATA_PRODUCTS_DB` | `RETAIL_BANKING_<ENV>.DATA_PRODUCTS` |

`<ENV>` is one of `DEV`, `UAT`, `PROD` and is set once via the `ENV` session
variable in `00_databases_schemas.sql`; the table scripts resolve the database
through `IDENTIFIER($DB_RETAIL)`.

## Idempotency

| Layer | Statement form | Why |
|---|---|---|
| Databases / schemas | `CREATE ... IF NOT EXISTS` | Never disturbs existing environments. |
| Source tables | `CREATE TABLE IF NOT EXISTS` | Owned upstream; must never drop landed data. |
| Staging tables | `CREATE OR REPLACE TABLE` | Rebuilt every run, mirroring the Teradata DROP/CREATE pattern. |
| Data product tables | `CREATE TABLE IF NOT EXISTS` | Published contract tables; history is preserved and schema changes go through versioned `ALTER` scripts. |

## Running the scripts

```bash
snowsql -f ddl/snowflake/00_databases_schemas.sql   # set ENV first (DEV|UAT|PROD)
snowsql -f ddl/snowflake/01_source_tables.sql
snowsql -f ddl/snowflake/02_staging_tables.sql
snowsql -f ddl/snowflake/03_data_product_tables.sql
```

Run them in the same session so the `ENV` / `DB_RETAIL` session variables set by
the first script remain in scope.
