# synapse/sql/ddl — Dedicated SQL pool DDL (Ticket T2)

**Owner:** [T2](../../tickets/TICKET-02.md). No other ticket writes here.

T-SQL ports of `ddl/00_source_tables.sql`, `ddl/01_staging_tables.sql` and
`ddl/02_data_product_tables.sql`, plus the two tables the Teradata repo uses but never
defines (`CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES`, `ETL_STAGING_DB.ETL_RUN_LOG`).
Teradata `MULTISET` / `PRIMARY INDEX` / `COLLECT STATISTICS` become `DISTRIBUTION` +
`CLUSTERED COLUMNSTORE INDEX` + `CREATE STATISTICS`.

The tables, columns, types and distributions **must match**
[`../../common/contracts/01_schema_contract.md`](../../common/contracts/01_schema_contract.md).
