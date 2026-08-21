# TICKET-02 — Snowflake environment, role and secret management setup in COG-GTM/retail-banking-analytics-pipeline

- **Repository:** COG-GTM/retail-banking-analytics-pipeline
- **Issue type:** Story
- **Labels:** `migration`, `snowflake`, `security`, `azure-key-vault`
- **Depends on (blocked by):** None
- **Blocks:** TICKET-03, TICKET-04, TICKET-05, TICKET-10

## Context

The pipeline authenticates to Teradata with an LDAP service account (TD_USERNAME, TD_LOGMECH) and stores {SAS004}-encoded passwords inline in sas/macros/connect_teradata.sas. Snowflake needs a proper warehouse/role/grant model and credentials sourced from Azure Key Vault instead of source-controlled secrets.

## Scope

- Define Snowflake warehouses sized per workload (ELT transformation, Spark read/write, ad-hoc analytics) with auto-suspend and resource monitors.
- Define functional roles (loader, transformer, analytics reader, admin) and grant hierarchies over the staging and data product databases from TICKET-01.
- Create the service principal used by Azure Synapse and configure key-pair or OAuth authentication in place of LDAP username/password.
- Store the private key/OAuth secret in Azure Key Vault and reference it from Synapse linked services; remove the {SAS004} encoded passwords and hardcoded TD_SERVER/TD_USERNAME/TD_LOGMECH values.
- Document the credential rotation procedure and the least-privilege grant matrix.

## Acceptance criteria

- [ ] Warehouses, roles and grants are created by re-runnable scripts held in the repo.
- [ ] The Synapse service principal can connect to Snowflake using key-pair or OAuth auth with no password anywhere in the repo.
- [ ] All secrets resolve from Azure Key Vault at runtime; a repo scan finds no credential literals.
- [ ] Least-privilege is demonstrated: the loader role cannot read data product tables it does not own, and the analytics role has read-only access.
- [ ] Credential rotation is documented and tested once end to end.

## Affected files

- `sas/macros/connect_teradata.sas`
- `config/pipeline_config.cfg`

## Dependencies

- None
- Blocks TICKET-03
- Blocks TICKET-04
- Blocks TICKET-05
- Blocks TICKET-10

---

Source of truth: [`tickets.json`](./tickets.json). Push to Jira with [`push_to_jira.py`](./push_to_jira.py).
