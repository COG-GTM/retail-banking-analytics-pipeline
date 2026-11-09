Repo: COG-GTM/retail-banking-analytics-pipeline

The user asked to "migrate this to azure synapse analytics". The Teradata BTEQ + SAS 9.4 pipeline is being migrated to Azure Synapse Analytics as 10 tickets, one Devin session each, **all running in parallel right now**. You own **TICKET-08: 02_sas_txn_analytics.sas + 03_sas_risk_scoring.sas → PySpark + MLlib**.

Your spec is `synapse/tickets/TICKET-08.md` on `main`. It is self-contained: follow it, including its acceptance criteria and "Session rules". Also follow the frozen contracts in `synapse/common/contracts/` and `synapse/tickets/PARALLELIZATION.md`.

- **Branch** `migrate/ticket-08-txn-risk-analytics`, cut from the latest `main`. Open one PR to `main` titled `[Synapse T8] 02_sas_txn_analytics.sas + 03_sas_risk_scoring.sas → PySpark + MLlib`.
- **Write only** `synapse/spark/txn_analytics.py`, `synapse/spark/risk_scoring.py`, `synapse/tests/ticket_08/**`. Everything else in the repo is read-only for you.
- **Don't wait** for the other nine tickets and don't base your work on their branches. If you need another ticket's objects (DDL, helper procs, `synapse/common`), use minimal stand-ins that match the contract, kept only inside `synapse/tests/ticket_08/`.
- **Ignore older work.** The repo has many open PRs and `devin/*` branches from earlier Databricks, Snowflake, dbt and Synapse attempts (e.g. #65–#104, including the closed `[AZSYN-NN]` PRs #93–#102 and the closed prep PR #103). Don't base on them, copy from them or comment on them.
- **Validate locally**, without live Azure or paid resources: Local PySpark 3.4.1 (see Environment tips).
- **Before finishing:** run your ticket's test suite twice from a clean state; make sure no `__pycache__`, virtualenvs, credentials or generated data are committed; and address the Devin Review comments on your PR, which is the only PR check.
- **Done** means the PR is open with the description sections the brief asks for: files, validation commands and results, parity vs. the reference CSVs, contract amendments or change requests, and open questions. Don't merge.

Environment tips:
- PySpark 3.4 needs Python 3.10, but system python3 is 3.12. Create a venv outside the repo (`uv venv -p 3.10 ~/venvs/t08`) and point PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON at it. Java 17 needs the `--add-opens` flags. `toPandas()` breaks with modern pandas, so use `collect()`. Staging CSV int columns contain values like "48.0", and `load_ts` values carry +02:00 offsets.
