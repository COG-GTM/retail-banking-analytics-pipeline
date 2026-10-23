# synapse/infra — Infrastructure as Code (Ticket T1)

**Owner:** [T1](../tickets/TICKET-01.md). No other ticket writes here.

Bicep templates that provision, per environment (DEV / UAT / PROD):

- ADLS Gen2 storage account (`landing`, `artifacts`, `synapse` containers)
- Azure Synapse workspace with system-assigned managed identity
- Dedicated SQL pool (`rbapdw`) and Apache Spark 3.4 pool (`sprbap`)
- Azure Key Vault (`kv-rbap-<env>`) and the non-secret configuration secrets listed in contract C4
- RBAC role assignments and diagnostics

Resource names, containers and Key Vault secret names are frozen in
[`../common/contracts/06_environment_and_naming_contract.md`](../common/contracts/06_environment_and_naming_contract.md).
