Repo: COG-GTM/retail-banking-analytics-pipeline

The user asked to "migrate this to azure synapse analytics". The Teradata BTEQ + SAS 9.4 pipeline is being migrated to Azure Synapse Analytics as 10 tickets, one Devin session each, **all running in parallel right now**. You own **TICKET-10: Synapse Pipelines orchestration + 02:00 ET daily trigger**.

Your spec is `synapse/tickets/TICKET-10.md` on `main`. It is self-contained: follow it, including its acceptance criteria and "Session rules". Also follow the frozen contracts in `synapse/common/contracts/` and `synapse/tickets/PARALLELIZATION.md`.

- **Branch** `migrate/ticket-10-orchestration`, cut from the latest `main`. Open one PR to `main` titled `[Synapse T10] Synapse Pipelines orchestration + 02:00 ET daily trigger`.
- **Write only** `synapse/pipelines/**`, `synapse/tests/ticket_10/**`, and (as owner) `synapse/common/contracts/07_entry_points_contract.md`. Everything else in the repo is read-only for you.
- **Don't wait** for the other nine tickets and don't base your work on their branches. If you need another ticket's objects (DDL, helper procs, `synapse/common`), use minimal stand-ins that match the contract, kept only inside `synapse/tests/ticket_10/`.
- **Ignore older work.** The repo has many open PRs and `devin/*` branches from earlier Databricks, Snowflake, dbt and Synapse attempts (e.g. #65–#83). Don't base on them, copy from them or comment on them.
- **Ticket-specific:** You own contract **C7** (entry points every other ticket implements right now): additive, non-breaking amendments only; anything breaking goes in a "Contract change request" section. You also own the linked services and the `common.zip` packaging.
- **Validate locally**, without live Azure or paid resources: validate the workspace JSON artifacts with tests only. No deployment.
- **Before finishing:** run your ticket's test suite twice from a clean state; make sure no `__pycache__`, virtualenvs, credentials or generated data are committed; and address the Devin Review comments on your PR, which is the only PR check.
- **Done** means the PR is open with the description sections the brief asks for: files, validation commands and results, parity vs. the reference CSVs, contract amendments or change requests, and open questions. Don't merge.
