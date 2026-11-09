Repo: COG-GTM/retail-banking-analytics-pipeline

The user asked to "migrate this to azure synapse analytics". The Teradata BTEQ + SAS 9.4 pipeline is being migrated to Azure Synapse Analytics as 10 tickets, one Devin session each, **all running in parallel right now**. You own **TICKET-09: Shared utilities (synapse/common/) + 04_sas_data_products.sas golden record**.

Your spec is `synapse/tickets/TICKET-09.md` on `main`. It is self-contained: follow it, including its acceptance criteria and "Session rules". Also follow the frozen contracts in `synapse/common/contracts/` and `synapse/tickets/PARALLELIZATION.md`.

- **Branch** `migrate/ticket-09-common-golden-record`, cut from the latest `main`. Open one PR to `main` titled `[Synapse T9] Shared utilities (synapse/common/) + 04_sas_data_products.sas golden record`.
- **Write only** `synapse/common/**` (except `__init__.py` and contract files you don't own), `synapse/spark/data_products.py`, `synapse/tests/ticket_09/**`. Everything else in the repo is read-only for you.
- **Don't wait** for the other nine tickets and don't base your work on their branches. If you need another ticket's objects (DDL, helper procs, `synapse/common`), use minimal stand-ins that match the contract, kept only inside `synapse/tests/ticket_09/`.
- **Ignore older work.** The repo has many open PRs and `devin/*` branches from earlier Databricks, Snowflake, dbt and Synapse attempts (e.g. #65–#104, including the closed `[AZSYN-NN]` PRs #93–#102 and the closed prep PR #103). Don't base on them, copy from them or comment on them.
- **Ticket-specific:** You own contracts **C2–C5**, which most other tickets are coding against right now. Additive, non-breaking amendments only (first commit, flagged at the top of the PR description); anything that changes an existing signature, column or behaviour goes in a "Contract change request" section instead. Build the shared library first so it can merge early; write `data_products.py` against C1, using the reference CSVs as inputs.
- **Validate locally**, without live Azure or paid resources: SQL Server 2022 container on host port **14339** for the helper procs, plus local PySpark 3.4.1 (see Environment tips).
- **Before finishing:** run your ticket's test suite twice from a clean state; make sure no `__pycache__`, virtualenvs, credentials or generated data are committed; and address the Devin Review comments on your PR, which is the only PR check.
- **Done** means the PR is open with the description sections the brief asks for: files, validation commands and results, parity vs. the reference CSVs, contract amendments or change requests, and open questions. Don't merge.

Environment tips:
- Use `mcr.microsoft.com/mssql/server:2022-latest` with `pymssql` for T-SQL tests. Shim dedicated-pool-only syntax in the test harness (see `synapse/tests/README.md`). SQL Server on Linux rejects `BULK INSERT ... CODEPAGE`, so load the CSVs with multi-row INSERT batches instead. Remove only containers you started.
- PySpark 3.4 needs Python 3.10, but system python3 is 3.12. Create a venv outside the repo (`uv venv -p 3.10 ~/venvs/t09`) and point PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON at it. Java 17 needs the `--add-opens` flags. `toPandas()` breaks with modern pandas, so use `collect()`. Staging CSV int columns contain values like "48.0", and `load_ts` values carry +02:00 offsets.
