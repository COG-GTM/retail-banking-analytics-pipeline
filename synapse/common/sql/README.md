# synapse/common/sql — Shared T-SQL helper procedures (Ticket T9)

**Owner:** [T9](../../tickets/TICKET-09.md).

| File | Procedure | Contract |
|---|---|---|
| `usp_log_step.sql` | `ETL_STAGING_DB.usp_log_step` | [C2](../contracts/02_audit_logging_contract.md) |
| `usp_validate_table.sql` | `ETL_STAGING_DB.usp_validate_table` | [C3](../contracts/03_validation_contract.md) |

Called by every staging proc (T4–T6) and by T3's load proc.
