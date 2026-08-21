# Modernization Tickets — Jira Import

Ten modernization stories covering the migration of this pipeline from **Teradata BTEQ + SAS**
to **Snowflake** (staging + data products), **Azure Synapse Pipelines** (orchestration) and
**Synapse Spark / PySpark** (analytics & ML).

## Contents

| File | Purpose |
|------|---------|
| `tickets.json` | Machine-readable source of truth: summary, description content, issue type, labels, affected files, dependencies |
| `TICKET-01..10-*.md` | Human-readable mirror of each ticket (context, scope, acceptance criteria, affected files, dependencies) |
| `push_to_jira.py` | Creates the issues in Jira Cloud and links their dependencies |

Edit `tickets.json` first — the markdown files mirror it.

## Ticket map

| Ticket | Summary | Blocked by |
|--------|---------|-----------|
| [TICKET-01](./TICKET-01-snowflake-ddl-migration.md) | Snowflake DDL migration | — |
| [TICKET-02](./TICKET-02-snowflake-environment-security.md) | Snowflake environment, roles & secrets | — |
| [TICKET-03](./TICKET-03-migrate-stg-customer-360.md) | Migrate `01_stg_customer_360.bteq` | 01, 02 |
| [TICKET-04](./TICKET-04-migrate-stg-txn-summary.md) | Migrate `02_stg_txn_summary.bteq` | 01, 02 |
| [TICKET-05](./TICKET-05-migrate-stg-risk-factors.md) | Migrate `03_stg_risk_factors.bteq` | 01, 02 |
| [TICKET-06](./TICKET-06-migrate-sas-customer-segments.md) | Migrate `01_sas_customer_segments.sas` | 03 |
| [TICKET-07](./TICKET-07-migrate-sas-txn-analytics.md) | Migrate `02_sas_txn_analytics.sas` | 04 |
| [TICKET-08](./TICKET-08-migrate-sas-risk-scoring.md) | Migrate `03_sas_risk_scoring.sas` | 03, 05 |
| [TICKET-09](./TICKET-09-migrate-sas-data-products.md) | Migrate `04_sas_data_products.sas` + macros | 03, 06, 07, 08 |
| [TICKET-10](./TICKET-10-orchestration-synapse-pipelines.md) | Synapse Pipelines orchestration | 01–09 |

## Required environment variables

No Jira configuration exists elsewhere in this repository, so the script is fully parameterized.
Never commit real tokens.

| Variable | Description | Example |
|----------|-------------|---------|
| `JIRA_BASE_URL` | Jira Cloud site base URL, no trailing slash | `https://your-site.atlassian.net` |
| `JIRA_EMAIL` | Atlassian account email used for basic auth | `you@example.com` |
| `JIRA_API_TOKEN` | Atlassian API token ([create one](https://id.atlassian.com/manage-profile/security/api-tokens)) | `ATATT3x...` |
| `JIRA_PROJECT_KEY` | Target Jira project key | `MBA` |

The account needs *Create Issues* and *Link Issues* permission on the target project, and the
project must have a **Story** issue type and a **Blocks** issue link type.

## Install

```bash
pip install requests
```

## Dry run

Prints the exact `POST /rest/api/3/issue` payloads and the planned dependency links without
calling Jira. No credentials are needed (`JIRA_PROJECT_KEY` is used if set):

```bash
python docs/modernization/jira/push_to_jira.py --dry-run
```

## Push for real

```bash
export JIRA_BASE_URL="https://your-site.atlassian.net"
export JIRA_EMAIL="you@example.com"
export JIRA_API_TOKEN="***"
export JIRA_PROJECT_KEY="MBA"

python docs/modernization/jira/push_to_jira.py
```

The script creates each issue, prints the returned key and browse URL, then creates a `Blocks`
link for every dependency (`inwardIssue` = blocker, `outwardIssue` = blocked). Errors from the
Jira API are reported per issue with the API's own message; the script continues with the
remaining tickets and exits non-zero if anything failed.

Useful flags:

- `--dry-run` — print payloads only.
- `--no-links` — create issues but skip dependency links.
- `--tickets-file PATH` — use an alternative tickets file.

Re-running the script creates duplicate issues; it does not upsert.
