# TICKET-10 — Orchestration migration from shell scripts to Azure Synapse Pipelines in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `azure-synapse`, `orchestration`
- **Depends on (blocked by):** TICKET-01, TICKET-02, TICKET-03, TICKET-04, TICKET-05, TICKET-06, TICKET-07, TICKET-08, TICKET-09
- **Blocks:** None

## Context

The pipeline is driven by bash orchestrators that source config/pipeline_config.cfg, run the BTEQ and SAS phases sequentially with fail-fast semantics, support --skip-bteq/--skip-sas/--dry-run, validate row counts after the run and emit logs and notifications.

## Scope

- Build an Azure Synapse Pipeline that replaces run_full_pipeline.sh, run_bteq_pipeline.sh and run_sas_pipeline.sh.
- Preserve sequential, fail-fast execution ordering across the staging and analytics phases.
- Reproduce the skip-phase behaviour (--skip-bteq / --skip-sas) as pipeline parameters with conditional activities.
- Reproduce dry-run behaviour so a run can print the planned activities without touching data.
- Reproduce post-run row-count validation of the staging tables and data products, failing the pipeline on breach.
- Reproduce logging and failure notification, routing to the team's existing alert channel.
- Move config/pipeline_config.cfg values (run date, LOOKBACK_MONTHS, RISK_SCORE_THRESHOLD, database references, log level) to pipeline parameters, with secrets in Azure Key Vault.
- Publish the pipeline definition as code in the repo and wire deployment through CI.

## Acceptance criteria

- [ ] A single Synapse Pipeline run reproduces the full end-to-end pipeline and lands all four data products.
- [ ] A failure in any activity stops downstream activities and surfaces a notification.
- [ ] Skip-phase and dry-run parameters behave equivalently to the shell flags.
- [ ] Post-run row-count validation fails the pipeline when thresholds are breached.
- [ ] No configuration value or secret is hardcoded: all come from pipeline parameters or Key Vault.
- [ ] The pipeline definition is version-controlled and deployable to DEV/UAT/PROD without manual edits.

## Affected files

- `orchestration/run_full_pipeline.sh`
- `bteq/run_bteq_pipeline.sh`
- `sas/run_sas_pipeline.sh`
- `config/pipeline_config.cfg`

## Dependencies

- Blocked by TICKET-01
- Blocked by TICKET-02
- Blocked by TICKET-03
- Blocked by TICKET-04
- Blocked by TICKET-05
- Blocked by TICKET-06
- Blocked by TICKET-07
- Blocked by TICKET-08
- Blocked by TICKET-09

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
