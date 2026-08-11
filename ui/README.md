# RBA Operator Console (`ui/`)

A React + TypeScript (Vite) single-page **operator console** for the retail-banking
data products produced by the BTEQ/SAS pipeline in this repository.

> **This is not a point-of-sale application.** It borrows the *visual metaphor* of a
> dense, keyboard-driven POS "fat client" — left action panel, central data grid,
> right running-total panel, bottom F-key bar — and applies it to customer lookup and
> data-product display. There is no cart, item, tender or checkout flow; the backing
> API is read-only.

## What it shows

| POS element              | Banking equivalent                                                        |
| ------------------------ | ------------------------------------------------------------------------- |
| Register / lane header   | Operator, environment, API connection status, current `CUSTOMER_ID`        |
| Tender buttons (left)    | Lookup Customer, Master Profile, Segments, Risk, Transactions              |
| Scanned-items grid       | Selected data product rendered as a dense column/value (or row) grid       |
| Running total (right)    | Headline KPIs from `CUSTOMER_MASTER_PROFILE` (segment, risk tier, balance) |
| F-key bar (bottom)       | F1–F12 shortcuts for panel switching, refresh, lookup, clear               |

Data products consumed (see `ddl/02_data_product_tables.sql`):
`CUSTOMER_MASTER_PROFILE`, `CUSTOMER_SEGMENTS`, `CUSTOMER_RISK_SCORES`, `TRANSACTION_ANALYTICS`.

## Running

```bash
cd ui
npm install
cp .env.example .env      # optional; defaults are mock-friendly
npm run dev               # http://localhost:5173
```

### Pointing at the Spring Boot API

The console calls the read-only data-products API under `/api/v1`. In dev, Vite proxies
`/api` to the Spring Boot service:

```bash
VITE_USE_MOCKS=false VITE_API_BASE_URL=http://localhost:8080 npm run dev
```

Endpoints used:

```
GET /api/v1/customers/{customerId}/profile
GET /api/v1/customers/{customerId}/segment
GET /api/v1/customers/{customerId}/risk-score
GET /api/v1/customers/{customerId}/transactions?reportingPeriod=YYYY-MM
GET /api/v1/profiles | /segments?segmentName= | /risk-scores?riskTier=   (page, size)
GET /api/v1/health
```

`400` responses from the API's `@RestControllerAdvice` (including `fieldErrors`) and
`404` responses for unknown customers are surfaced verbatim in the panel error box.

### Mock mode (no API, no Teradata)

```bash
VITE_USE_MOCKS=true npm run dev
```

Mock mode serves JSON fixtures in `src/mocks/`, generated from the SAS CSV outputs in
`data/03_sas_data_products/`:

```bash
npm run fixtures              # first 60 customers of each product
npm run fixtures -- --limit 0 # all rows
```

Try `CUSTOMER_ID` `3`, `4`, `5`, `6` — those exist in all four fixture products.

## Keyboard shortcuts

| Key | Action              | Key | Action                    |
| --- | ------------------- | --- | ------------------------- |
| F1  | Master profile      | F7  | Previous panel            |
| F2  | Segment             | F8  | Next panel                |
| F3  | Risk                | F9  | Transactions period entry |
| F4  | Transactions        | F10 | Show data source          |
| F5  | Refresh queries     | F11 | Help line                 |
| F6  | Focus customer input| F12 | Clear customer context    |

## Environment variables

| Variable            | Default                 | Purpose                                    |
| ------------------- | ----------------------- | ------------------------------------------ |
| `VITE_API_BASE_URL` | `http://localhost:8080` | Dev-proxy target for `/api`                |
| `VITE_USE_MOCKS`    | unset (`false`)         | `true` serves CSV-derived JSON fixtures    |
| `VITE_OPERATOR`     | `OP-0001`               | Operator id in the header                  |
| `VITE_ENVIRONMENT`  | `DEV`                   | Environment label in the header            |

## Scripts

```bash
npm run build   # tsc -b && vite build
npm test        # vitest (runs in mock mode; no API or Teradata required)
npm run lint    # eslint
npm run format  # prettier
```
