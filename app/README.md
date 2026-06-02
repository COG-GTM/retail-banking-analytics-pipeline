# BI API — Retail Banking Analytics

A FastAPI CRUD application that gives BI users actionable access to the
pipeline's SAS data products. The primary entity is `CUSTOMER_MASTER_PROFILE`
(the Golden Record), with drill-down into the component tables.

The four CSVs in `data/03_sas_data_products/` are loaded into an **in-memory
DuckDB** database at startup. DuckDB is the analytical query engine, consistent
with the rest of the pipeline (see `export_data.py`).

## Usage

Run from the **project root** (the package uses relative imports, so it is
launched as `app.main:app`):

```
pip install -r app/requirements.txt
uvicorn app.main:app --reload
# API docs at http://localhost:8000/docs
```

> Run `uv run export_data.py` from the project root first if the data product
> CSVs have not been generated yet.

## Endpoints

### Customers (CUSTOMER_MASTER_PROFILE)

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/customers` | List/search with filters, pagination, sorting |
| `GET` | `/customers/{customer_id}` | Full detail, joined across all 4 tables |
| `PATCH` | `/customers/{customer_id}/flags` | Update actionable flags (Y/N) — write-back |
| `GET` | `/customers/{customer_id}/segments` | Drill-down to `customer_segments` |
| `GET` | `/customers/{customer_id}/transactions` | Drill-down to `transaction_analytics` |
| `GET` | `/customers/{customer_id}/risk` | Drill-down to `customer_risk_scores` |

**`GET /customers` query params:** `segment_name`, `risk_tier`, `state_code`,
`min_lifetime_value`, `max_lifetime_value`, `cross_sell_flag`, `upsell_flag`,
`retention_risk_flag`, `watch_list_flag`, `limit` (default 50, max 500),
`offset`, `sort_by`, `order` (`asc`/`desc`).

**`PATCH /customers/{customer_id}/flags`** accepts any of `cross_sell_flag`,
`upsell_flag`, `retention_risk_flag`, `watch_list_flag` constrained to `'Y'`/`'N'`.
These are the fields BI users override for campaign targeting, retention actions,
or compliance review. The update writes back to the in-memory DuckDB table,
simulating a write-back to the Teradata `DATA_PRODUCTS_DB` in production.

### Analytics (BI dashboards)

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/analytics/segments/distribution` | Customer count per `segment_name` |
| `GET` | `/analytics/risk/distribution` | Customer count per `risk_tier` |
| `GET` | `/analytics/spend/trends` | Count by `monthly_spend_trend` (UP/DOWN/STABLE) |
| `GET` | `/analytics/segments/{segment_name}/summary` | Avg LTV, avg risk, count for a segment |
| `GET` | `/analytics/top-customers?n=20` | Top N customers by `lifetime_value_score` |

## Status codes

- `404` — customer or component record not found
- `422` — invalid flag value (must be `'Y'`/`'N'`) or invalid `sort_by`
