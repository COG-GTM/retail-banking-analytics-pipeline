# synapse/spark — PySpark / Spark MLlib jobs (Tickets T7, T8, T9)

One entry-point file per job; each ticket writes **only its own files**:

| File | Job name (audit) | Replaces | Writes | Owner |
|---|---|---|---|---|
| `customer_segments.py` | `01_CUSTOMER_SEG` | `sas/01_sas_customer_segments.sas` | `DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS` | [T7](../tickets/TICKET-07.md) |
| `txn_analytics.py` | `02_TXN_ANALYTICS` | `sas/02_sas_txn_analytics.sas` | `DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS` | [T8](../tickets/TICKET-08.md) |
| `risk_scoring.py` | `03_RISK_SCORING` | `sas/03_sas_risk_scoring.sas` | `DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES` | [T8](../tickets/TICKET-08.md) |
| `data_products.py` | `04_MASTER_PROFILE` | `sas/04_sas_data_products.sas` | `DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE` | [T9](../tickets/TICKET-09.md) |

`__init__.py` is created by the prep PR; do not modify it. Jobs import shared helpers from
`common` (i.e. `synapse/common/`) only through the interfaces frozen in
[`../common/contracts/`](../common/contracts/README.md). CLI arguments are frozen in contract C7.
