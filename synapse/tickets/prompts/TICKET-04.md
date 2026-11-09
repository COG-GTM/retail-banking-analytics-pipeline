Repo: COG-GTM/retail-banking-analytics-pipeline

The user asked to "migrate this to azure synapse analytics". The Teradata BTEQ + SAS 9.4 pipeline is being migrated to Azure Synapse Analytics as 10 tickets, one Devin session each, **all running in parallel right now**. You own **TICKET-04: 01_stg_customer_360.bteq → T-SQL stored procedure**.

Your spec is `synapse/tickets/TICKET-04.md` on `main`. It is self-contained: follow it, including its acceptance criteria and "Session rules". Also follow the frozen contracts in `synapse/common/contracts/` and `synapse/tickets/PARALLELIZATION.md`.

- **Branch** `migrate/ticket-04-stg-customer-360`, cut from the latest `main`. Open one PR to `main` titled `[Synapse T4] 01_stg_customer_360.bteq → T-SQL stored procedure`.
- **Write only** `synapse/sql/staging/usp_stg_customer_360.sql`, `synapse/tests/ticket_04/**`. Everything else in the repo is read-only for you.
- **Don't wait** for the other nine tickets and don't base your work on their branches. If you need another ticket's objects (DDL, helper procs, `synapse/common`), use minimal stand-ins that match the contract, kept only inside `synapse/tests/ticket_04/`.
- **Ignore older work.** The repo has many open PRs and `devin/*` branches from earlier Databricks, Snowflake, dbt and Synapse attempts (e.g. #65–#83). Don't base on them, copy from them or comment on them.
- **Validate locally**, without live Azure or paid resources: SQL Server 2022 container on host port **14334**.
- **Before finishing:** run your ticket's test suite twice from a clean state; make sure no `__pycache__`, virtualenvs, credentials or generated data are committed; and address the Devin Review comments on your PR, which is the only PR check.
- **Done** means the PR is open with the description sections the brief asks for: files, validation commands and results, parity vs. the reference CSVs, contract amendments or change requests, and open questions. Don't merge.

Environment tips:
- Use `mcr.microsoft.com/mssql/server:2022-latest` with `pymssql` for T-SQL tests. Shim dedicated-pool-only syntax in the test harness (see `synapse/tests/README.md`). SQL Server on Linux rejects `BULK INSERT ... CODEPAGE`, so load the CSVs with multi-row INSERT batches instead. Remove only containers you started.
