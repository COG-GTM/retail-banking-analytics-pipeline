# synapse/common — Shared utilities (Ticket T9)

**Owner:** [T9](../tickets/TICKET-09.md). Other tickets **consume** these modules but never edit them.

Replaces the SAS macro library (`sas/macros/connect_teradata.sas`, `log_step.sas`,
`validate_table.sas`) and the BTEQ `ETL_RUN_LOG` insert pattern:

| Module (to be created by T9) | Contract | Replaces |
|---|---|---|
| `config.py` | C4 / C6 / C7 | `config/pipeline_config.cfg` runtime parameters |
| `keyvault.py` | C4 | `{SAS004}` passwords, `TD_*` env vars |
| `sql_connector.py` | C4 | `%connect_teradata` LIBNAMEs + `PROC SQL` pass-through |
| `audit.py` | C2 | `%init_audit`, `%log_step` |
| `validation.py` | C3 | `%validate_table` |
| `writeback.py` | C5 | `DELETE` pass-through + `PROC APPEND ... FORCE` |
| `sql/usp_log_step.sql` | C2 | BTEQ `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG` |
| `sql/usp_validate_table.sql` | C3 | BTEQ zero-row guards (`.IF ACTIVITYCOUNT = 0 THEN .EXIT 99`) |

The exact signatures are in [`contracts/stubs/`](contracts/stubs/). `__init__.py` is created
by the prep PR.
