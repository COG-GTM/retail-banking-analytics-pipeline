Repo: COG-GTM/retail-banking-analytics-pipeline

The user asked to "migrate this to azure synapse analytics". The Teradata BTEQ + SAS 9.4 pipeline is being migrated to Azure Synapse Analytics as 10 tickets, one Devin session each, **all running in parallel right now**. You own **TICKET-01: Provision Synapse workspace + infrastructure as code**.

Your spec is `synapse/tickets/TICKET-01.md` on `main`. It is self-contained: follow it, including its acceptance criteria and "Session rules". Also follow the frozen contracts in `synapse/common/contracts/` and `synapse/tickets/PARALLELIZATION.md`.

- **Branch** `migrate/ticket-01-infra`, cut from the latest `main`. Open one PR to `main` titled `[Synapse T1] Provision Synapse workspace + infrastructure as code`.
- **Write only** `synapse/infra/**`, `synapse/tests/ticket_01/**`, and (as owner) `synapse/common/contracts/06_environment_and_naming_contract.md`. Everything else in the repo is read-only for you.
- **Don't wait** for the other nine tickets and don't base your work on their branches. If you need another ticket's objects (DDL, helper procs, `synapse/common`), use minimal stand-ins that match the contract, kept only inside `synapse/tests/ticket_01/`.
- **Ignore older work.** The repo has many open PRs and `devin/*` branches from earlier Databricks, Snowflake, dbt and Synapse attempts (e.g. #65–#104, including the closed `[AZSYN-NN]` PRs #93–#102 and the closed prep PR #103). Don't base on them, copy from them or comment on them.
- **Ticket-specific:** You own contract **C6**. Others are coding against C6 v1.0 right now: additive, non-breaking amendments only; anything breaking goes in a "Contract change request" section of your PR description.
- **Validate locally**, without live Azure or paid resources: compile and lint the Bicep locally (install the standalone Bicep CLI if needed). Do not run deployments or what-if against any real subscription.
- **Before finishing:** run your ticket's test suite twice from a clean state; make sure no `__pycache__`, virtualenvs, credentials or generated data are committed; and address the Devin Review comments on your PR, which is the only PR check.
- **Done** means the PR is open with the description sections the brief asks for: files, validation commands and results, parity vs. the reference CSVs, contract amendments or change requests, and open questions. Don't merge.
