# synapse/sql/staging — Staging stored procedures (Tickets T4, T5, T6)

One file per procedure; each ticket writes **only its own file** (plus its own tests folder):

| File | Procedure | Replaces | Owner |
|---|---|---|---|
| `usp_stg_customer_360.sql` | `ETL_STAGING_DB.usp_stg_customer_360` | `bteq/01_stg_customer_360.bteq` | [T4](../../tickets/TICKET-04.md) |
| `usp_stg_txn_summary.sql` | `ETL_STAGING_DB.usp_stg_txn_summary` | `bteq/02_stg_txn_summary.bteq` | [T5](../../tickets/TICKET-05.md) |
| `usp_stg_risk_factors.sql` | `ETL_STAGING_DB.usp_stg_risk_factors` | `bteq/03_stg_risk_factors.bteq` | [T6](../../tickets/TICKET-06.md) |

Procedure signatures are frozen in
[`../../common/contracts/07_entry_points_contract.md`](../../common/contracts/07_entry_points_contract.md).
Procs log via `ETL_STAGING_DB.usp_log_step` and validate via `ETL_STAGING_DB.usp_validate_table`
(contracts C2/C3, implemented by T9 in `synapse/common/sql/`).
