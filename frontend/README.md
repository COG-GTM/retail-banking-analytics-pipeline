# POS Checkout Frontend

A net-new Vite + React + TypeScript point-of-sale checkout UI for a hardware-store
register (store/register/cashier context, pro job accounts, service lines, split tender,
quote conversion).

> **Separate domain.** This app is unrelated to the retail *banking* analytics pipeline
> that occupies the rest of this repository (`bteq/`, `sas/`, `ddl/`, `data/`). It shares
> no code, no data and no build tooling with it, and lives entirely under `frontend/`.
> It consumes the POS backend schema only.

## Running

```bash
cd frontend
npm install
npm run dev        # http://localhost:5173
```

Other scripts: `npm run build`, `npm run typecheck`, `npm run lint`, `npm test`.

By default the app runs against an in-memory mock of the POS backend
(`src/api/mockData.ts`), so it is fully usable with no backend running. Point it at the
real service by copying `.env.example` to `.env` and setting:

```
VITE_POS_API_BASE_URL=http://localhost:8080/api
```

Every service function in `src/api/posService.ts` then issues the documented HTTP call
instead of returning mock data.

## Mapping to the POS backend schema

TypeScript models in `src/types/pos.ts` mirror the backend entities 1:1:

| Backend entity     | TS type            | Notes |
| ------------------ | ------------------ | ----- |
| `STORE`            | `Store`            | `storeId`, `taxRate` (8.625% in mock data) |
| `REGISTER`         | `Register`         | `registerId`, `registerType` (front end) |
| `EMPLOYEE`         | `Employee`         | cashier shown in the header |
| `PRODUCT` / SKU    | `Product`          | `unitOfMeasure`, `unitPrice`, `aisle`/`bay`, warranty eligibility |
| `CUSTOMER` / `JOB_ACCOUNT` | `JobAccount` | program name, company, `poNumber`, `jobName`, tax-exempt certificate |
| `TRANSACTION`      | `Transaction`      | header: store/register/employee/job account, status, tax rate & exempt flag |
| `TRANSACTION_LINE` | `TransactionLine`  | `lineType` (merchandise, service, warranty, discount), qty, UOM, unit price, extended amount |
| `TENDER`           | `Tender`           | tender type, amount, auth code / last four — multiple rows = split tender |
| `QUOTE`            | `Quote`            | quote header plus quote lines that convert into transaction lines |

### Service endpoints

| Function | Endpoint |
| -------- | -------- |
| `getRegisterContext()` | `GET /stores/{storeId}/registers/{registerId}/context` |
| `lookupProduct(sku)` / `searchProducts(term)` | `GET /products/{sku}`, `GET /products?search=` |
| `getServiceCatalog()` | `GET /service-items` |
| `getJobAccount(id)` | `GET /job-accounts/{id}` |
| `getQuotes(jobAccountId)` / `getQuote(id)` | `GET /quotes`, `GET /quotes/{id}` |
| `postTransaction(txn)` | `POST /transactions` |
| `postTender(txnId, tender)` | `POST /transactions/{id}/tenders` |

## Features

- **Header** — store / register / register type / cashier, transaction id
  (`store/register/sequence`), and a Pro job-account banner with company, PO and job name.
- **Item entry** — SKU or description lookup with type-ahead, aisle/bay display, quantity
  and add-to-cart (repeat SKUs merge into the existing line).
- **Line-item grid** — description, SKU, editable quantity, UOM (EA/GAL/BOX/BAG/LF/CS),
  unit price, extended amount, line removal.
- **Non-scan / service buttons** — lumber cut charge, paint mix, tool rental, special
  order, will call, delivery; each adds a typed service line.
- **Add-ons** — 2-year protection plan on warranty-eligible SKUs (`WARRANTY` line).
- **Discounts** — pro volume pricing as a negative `DISCOUNT` line.
- **Totals** — subtotal, discount total, sales tax (rate + amount) and total, with an
  "Apply tax exempt" toggle that zeroes the taxable base.
- **Tender** — commercial revolving charge, credit/debit and cash, with split tender
  (multiple tender rows summing to the total) and a running balance due.
- **Convert quote** — loads an open quote's lines into the active transaction.

## Structure

```
src/
├── api/         typed client + service layer + mock backend data
├── components/  header, item entry, line grid, service buttons, totals, tender, quote dialog
├── store/       zustand transaction store and pure totals calculation
├── types/pos.ts POS backend schema models
└── utils/       currency/quantity formatting
```

Cart/transaction state (lines, tenders, tax-exempt flag, loaded quote) lives in the
Zustand store `src/store/transactionStore.ts`; totals are derived by the pure
`computeTotals` function, which is unit-tested in `src/store/totals.test.ts`.
