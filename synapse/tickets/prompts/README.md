# Launch prompts — Synapse migration (10 parallel sessions)

One prompt per ticket. Start all 10 sessions at the same time, after the prep PR has merged to `main` (see [`../PARALLELIZATION.md`](../PARALLELIZATION.md) §2 and §5). Each prompt is meant to be pasted verbatim as a session's first message.

| Ticket | Prompt | Branch |
|---|---|---|
| T1 Provision Synapse workspace + infrastructure as code | [`TICKET-01.md`](TICKET-01.md) | `migrate/ticket-01-infra` |
| T2 Port Teradata DDL to dedicated SQL pool T-SQL | [`TICKET-02.md`](TICKET-02.md) | `migrate/ticket-02-ddl` |
| T3 Source ingestion into ADLS Gen2 + COPY INTO | [`TICKET-03.md`](TICKET-03.md) | `migrate/ticket-03-ingestion` |
| T4 01_stg_customer_360.bteq → T-SQL stored procedure | [`TICKET-04.md`](TICKET-04.md) | `migrate/ticket-04-stg-customer-360` |
| T5 02_stg_txn_summary.bteq → T-SQL stored procedure | [`TICKET-05.md`](TICKET-05.md) | `migrate/ticket-05-stg-txn-summary` |
| T6 03_stg_risk_factors.bteq → T-SQL stored procedure | [`TICKET-06.md`](TICKET-06.md) | `migrate/ticket-06-stg-risk-factors` |
| T7 01_sas_customer_segments.sas → PySpark + MLlib KMeans | [`TICKET-07.md`](TICKET-07.md) | `migrate/ticket-07-customer-segments` |
| T8 02_sas_txn_analytics.sas + 03_sas_risk_scoring.sas → PySpark + MLlib | [`TICKET-08.md`](TICKET-08.md) | `migrate/ticket-08-txn-risk-analytics` |
| T9 Shared utilities (synapse/common/) + 04_sas_data_products.sas golden record | [`TICKET-09.md`](TICKET-09.md) | `migrate/ticket-09-common-golden-record` |
| T10 Synapse Pipelines orchestration + 02:00 ET daily trigger | [`TICKET-10.md`](TICKET-10.md) | `migrate/ticket-10-orchestration` |
