# synapse/ingestion — Source ingestion (Ticket T3)

**Owner:** [T3](../tickets/TICKET-03.md). No other ticket writes here.

Lands the six source extracts in the ADLS Gen2 `landing` container and loads them into the
dedicated SQL pool source schemas with `COPY INTO` (entry point
`ETL_STAGING_DB.usp_load_source_tables`, contract C7). Landing paths follow contract C4.

T3 also records here the decision on whether `local/duckdb/run_demo` (imported by
`export_data.py` but not present in the repo) remains the dev data generator.
