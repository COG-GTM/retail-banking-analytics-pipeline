# Snowflake DDL (migrated from Teradata)

Snowflake translation of the Teradata DDL in `ddl/00_source_tables.sql`,
`ddl/01_staging_tables.sql` and `ddl/02_data_product_tables.sql`. The Teradata
scripts are kept unchanged as the migration reference.

## Scripts

| Script | Contents |
|--------|----------|
| `00_databases_and_schemas.sql` | Per-environment database and the four schemas |
| `01_source_tables.sql` | `CUSTOMERS`, `ACCOUNTS`, `ADDRESSES`, `TRANSACTIONS`, `TRANSACTION_TYPES` |
| `02_staging_tables.sql` | `STG_CUSTOMER_360`, `STG_TXN_SUMMARY`, `STG_RISK_FACTORS` |
| `03_data_product_tables.sql` | `CUSTOMER_SEGMENTS`, `TRANSACTION_ANALYTICS`, `CUSTOMER_RISK_SCORES`, `CUSTOMER_MASTER_PROFILE` |

Run them in order, passing the environment as a SnowSQL variable:

```bash
for f in ddl/snowflake/0*.sql; do
  snowsql -f "$f" -D env=DEV     # or UAT / PROD
done
```

Every statement is `CREATE ... IF NOT EXISTS`, so the scripts are idempotent and
safe to re-run in DEV, UAT and PROD without dropping existing data.

## Database and schema layout

Teradata databases are flat table containers; they map to Snowflake schemas
inside one database per environment.

| Teradata database | Snowflake object |
|-------------------|------------------|
| `CORE_BANKING_DB` | `RETAIL_BANKING_<ENV>.CORE_BANKING` |
| `TXN_PROCESSING_DB` | `RETAIL_BANKING_<ENV>.TXN_PROCESSING` |
| `ETL_STAGING_DB` | `RETAIL_BANKING_<ENV>.ETL_STAGING` |
| `DATA_PRODUCTS_DB` | `RETAIL_BANKING_<ENV>.DATA_PRODUCTS` |

`<ENV>` is `DEV`, `UAT` or `PROD`, supplied through the `env` SnowSQL variable.
Warehouses, roles and grants are provisioned separately and are not created here.

## Data type mapping

| Teradata type | Snowflake type | Notes |
|---------------|----------------|-------|
| `BIGINT` | `NUMBER(19,0)` | Preserves the Teradata 8-byte integer range |
| `INTEGER` | `NUMBER(10,0)` | Preserves the Teradata 4-byte integer range |
| `SMALLINT` | `NUMBER(5,0)` | Preserves the Teradata 2-byte integer range |
| `BYTEINT` | `NUMBER(3,0)` | Not used by these tables; recorded for completeness |
| `DECIMAL(p,s)` | `NUMBER(p,s)` | Same precision and scale |
| `CHAR(n)` | `VARCHAR(n)` | Snowflake does not pad `CHAR`; trailing-blank semantics are dropped |
| `VARCHAR(n)` | `VARCHAR(n)` | Same length |
| `DATE` | `DATE` | `FORMAT 'YYYY-MM-DD'` is a display attribute only and is dropped |
| `TIMESTAMP(6)` | `TIMESTAMP_NTZ(6)` | Teradata `TIMESTAMP` is timezone-naive |

## Teradata-only constructs removed

| Teradata construct | Snowflake treatment |
|--------------------|---------------------|
| `MULTISET` | Dropped — Snowflake tables always allow duplicate rows |
| `NO FALLBACK` | Dropped — durability is handled by Snowflake storage |
| `PRIMARY INDEX (...)` | Dropped — no data distribution primitive in Snowflake |
| `UNIQUE PRIMARY INDEX (...)` | Expressed as an unenforced `PRIMARY KEY` constraint (`TRANSACTION_TYPES`) so the uniqueness contract stays in the catalogue |
| `CHARACTER SET LATIN NOT CASESPECIFIC` | Dropped — Snowflake is UTF-8; case-insensitive comparisons must be explicit (`ILIKE`, `UPPER()`) in migrated queries |
| `FORMAT 'YYYY-MM-DD'` | Dropped — formatting belongs to the consuming query/BI tool |
| `PARTITION BY RANGE_N(TRANSACTION_DATE ...)` | `CLUSTER BY (TRANSACTION_DATE)` on `TRANSACTIONS` — pruning is by micro-partition |
| `PARTITION BY COLUMN(REPORTING_PERIOD)` | `CLUSTER BY (REPORTING_PERIOD)` on `TRANSACTION_ANALYTICS` |
| `COLLECT STATISTICS` | Dropped — Snowflake maintains micro-partition statistics automatically |

Tables without a natural range/period access pattern get no clustering key:
below the multi-terabyte range Snowflake's automatic micro-partitioning is
sufficient, and clustering keys carry ongoing reclustering cost.
