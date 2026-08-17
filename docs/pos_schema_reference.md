# POS Schema Reference — UI ↔ Data Contract

**Separate domain notice.** The point-of-sale (POS) schema in `ddl/pos/00_pos_tables.sql`
is a net-new, standalone model for the retail checkout webapp. It is **not** part of
the retail banking analytics pipeline in this repository and does **not** integrate
with `CORE_BANKING_DB`, `TXN_PROCESSING_DB`, `ETL_STAGING_DB`, or `DATA_PRODUCTS_DB`.
No banking DDL was modified or reused. "Transaction" in this document means a sales
transaction at a register, not a banking financial transaction.

All objects live in `POS_DB`. Dialect: Teradata (matching the rest of `ddl/`).

---

## Entity relationships

```
STORE 1─┬─* REGISTER ──┐
        ├─* EMPLOYEE ──┼─* POS_TRANSACTION 1─* POS_TRANSACTION_LINE *─1 PRODUCT
        └─* CUSTOMER_ACCOUNT 1─* JOB_REFERENCE          │
                             │                          └─* POS_TENDER   (split tender)
                             └─* QUOTE 1─* QUOTE_LINE
                                    │
                                    └── "Convert quote" ──▶ POS_TRANSACTION.SOURCE_QUOTE_ID
PRODUCT 1─* PRODUCT_STORE_PRICE *─1 STORE   (optional per-store price / location override)
```

Keys at a glance:

| Table | Primary key | Business / natural key |
|---|---|---|
| `STORE` | `STORE_ID` | store number (6631) |
| `REGISTER` | `(STORE_ID, REGISTER_ID)` | lane 12 at store 6631 |
| `EMPLOYEE` | `EMPLOYEE_ID` | badge number |
| `PRODUCT` | `SKU` | SKU |
| `PRODUCT_STORE_PRICE` | `(STORE_ID, SKU, EFFECTIVE_DATE)` | — |
| `CUSTOMER_ACCOUNT` | `CUSTOMER_ACCOUNT_ID` | `ACCOUNT_NUMBER` (unique index) |
| `JOB_REFERENCE` | `JOB_REFERENCE_ID` | `PO_NUMBER` within an account |
| `QUOTE` | `QUOTE_ID` | `QUOTE_NUMBER` (unique index) |
| `QUOTE_LINE` | `(QUOTE_ID, LINE_NUMBER)` | — |
| `POS_TRANSACTION` | `TRANSACTION_ID` | `(STORE_ID, REGISTER_ID, BUSINESS_DATE, TRANSACTION_NUMBER)` — the receipt triple `6631/12/84291` |
| `POS_TRANSACTION_LINE` | `(TRANSACTION_ID, LINE_NUMBER)` | — |
| `POS_TENDER` | `(TRANSACTION_ID, TENDER_SEQ)` | — |

---

## UI element → table.column mapping

### Lane header / transaction identity

| UI element | Backed by |
|---|---|
| Store number (`6631`) | `POS_TRANSACTION.STORE_ID` → `STORE.STORE_ID` |
| Store name / city / state | `STORE.STORE_NAME`, `STORE.CITY`, `STORE.STATE_CODE` |
| Register / lane (`12`) | `POS_TRANSACTION.REGISTER_ID` → `REGISTER.REGISTER_ID` |
| Lane area label ("front end") | `REGISTER.REGISTER_AREA` (`REGISTER_TYPE` for self-checkout vs staffed) |
| Transaction number (`84291`) | `POS_TRANSACTION.TRANSACTION_NUMBER` (unique with store + register + business date) |
| Combined receipt id `6631/12/84291` | rendered from the three columns above |
| Cashier name ("M. Reyes") | `POS_TRANSACTION.CASHIER_EMPLOYEE_ID` → `EMPLOYEE.DISPLAY_NAME` |
| Date / time on the lane and receipt | `POS_TRANSACTION.TRANSACTION_TS` (`BUSINESS_DATE` for day-close reporting) |
| Transaction state chip (Open / Suspended / Completed) | `POS_TRANSACTION.TRANSACTION_STATUS` |

### Customer / job panel

| UI element | Backed by |
|---|---|
| Company name ("Bay Area Renovations LLC") | `CUSTOMER_ACCOUNT.COMPANY_NAME` |
| Consumer vs commercial badge | `CUSTOMER_ACCOUNT.ACCOUNT_TYPE` (`CONSUMER` / `COMMERCIAL`) |
| Account number | `CUSTOMER_ACCOUNT.ACCOUNT_NUMBER` |
| Loyalty program ("Pro Xtra") and tier | `CUSTOMER_ACCOUNT.LOYALTY_PROGRAM`, `LOYALTY_TIER`, `LOYALTY_MEMBER_ID` |
| Contact name / phone / email | `CUSTOMER_ACCOUNT.CONTACT_FIRST_NAME`, `CONTACT_LAST_NAME`, `PHONE`, `EMAIL` |
| PO / job reference on the sale | `POS_TRANSACTION.PO_NUMBER`, `POS_TRANSACTION.JOB_REFERENCE_ID` → `JOB_REFERENCE.PO_NUMBER`, `JOB_NAME` |
| Job picker list for a Pro account | `JOB_REFERENCE` rows where `CUSTOMER_ACCOUNT_ID = ?` and `JOB_STATUS = 'O'` |
| "Tax exempt" indicator | `CUSTOMER_ACCOUNT.TAX_EXEMPT_FLAG` (+ `TAX_EXEMPT_CERT_NO`, `TAX_EXEMPT_EXPIRY`); stamped onto the sale as `POS_TRANSACTION.TAX_EXEMPT_FLAG` / `TAX_EXEMPT_CERT_NO` |
| Charge-account availability | `CUSTOMER_ACCOUNT.CHARGE_ACCOUNT_NO`, `CREDIT_LIMIT` |

### Basket / line item grid

Every visible row in the basket is one `POS_TRANSACTION_LINE`, ordered by `LINE_NUMBER`.

| UI element | Backed by |
|---|---|
| Line sequence number | `POS_TRANSACTION_LINE.LINE_NUMBER` |
| SKU column | `POS_TRANSACTION_LINE.SKU` → `PRODUCT.SKU` (nullable for ad-hoc keyed lines) |
| Item description text | `POS_TRANSACTION_LINE.DESCRIPTION_SNAPSHOT` (snapshot of `PRODUCT.PRODUCT_DESC` at ring time) |
| Quantity | `POS_TRANSACTION_LINE.QUANTITY` (`DECIMAL(11,3)` so cut lumber / gallons work) |
| Unit of measure chip (EA, GAL, BOX, BAG) | `POS_TRANSACTION_LINE.UNIT_OF_MEASURE` (catalog default `PRODUCT.UNIT_OF_MEASURE`) |
| Unit price | `POS_TRANSACTION_LINE.UNIT_PRICE` (catalog `PRODUCT.UNIT_PRICE`, or `PRODUCT_STORE_PRICE.UNIT_PRICE`) |
| Extended amount | `POS_TRANSACTION_LINE.EXTENDED_AMT` |
| Location hint ("aisle 12, bay 004") | `POS_TRANSACTION_LINE.AISLE_SNAPSHOT`, `BAY_SNAPSHOT` (catalog `PRODUCT.AISLE`, `PRODUCT.BAY`) |
| Department label ("Paint desk", "Lumber yard", "Saw station") | `POS_TRANSACTION_LINE.DEPARTMENT_SNAPSHOT` (catalog `PRODUCT.DEPARTMENT`) |
| Line kind styling (merchandise / service / warranty / discount) | `POS_TRANSACTION_LINE.LINE_TYPE` ∈ `MERCHANDISE`, `SERVICE`, `WARRANTY`, `DISCOUNT`, `FEE` |
| Voided-line strikethrough | `POS_TRANSACTION_LINE.VOID_FLAG` |
| Line count / unit count in the footer | `POS_TRANSACTION.LINE_COUNT`, `POS_TRANSACTION.UNIT_COUNT` |

Special line kinds visible in the reference UI:

| UI line | `LINE_TYPE` | Additional columns used |
|---|---|---|
| Lumber cut charge | `SERVICE` | `SERVICE_DETAIL` (cut list), `RELATED_LINE_NUMBER` → the lumber line; `PRODUCT.IS_SCANNABLE = 'N'`, `PRODUCT.DEPARTMENT = 'Saw station'` |
| Paint mix / tint | `SERVICE` | `SERVICE_DETAIL` (colorant formula), `RELATED_LINE_NUMBER` → the base paint line |
| Tool rental | `SERVICE` | `SERVICE_DETAIL` (rental window), `UNIT_OF_MEASURE = 'HR'` |
| Special order | `SERVICE` | `FULFILLMENT_TYPE = 'SPECIAL_ORDER'`, `SERVICE_DETAIL` (vendor / ETA) |
| Will call | `SERVICE` | `FULFILLMENT_TYPE = 'WILL_CALL'`, `SERVICE_DETAIL` (pickup window) |
| Delivery | `SERVICE` | `FULFILLMENT_TYPE = 'DELIVERY'`, `SERVICE_DETAIL` (delivery slot / address) |
| 2-year protection plan | `WARRANTY` | `WARRANTY_TERM_MONTHS = 24`, `RELATED_LINE_NUMBER` → covered merchandise line |
| Volume pricing discount | `DISCOUNT` | negative `UNIT_PRICE` / `EXTENDED_AMT`, `DISCOUNT_REASON = 'VOLUME_PRICING'`, optional `RELATED_LINE_NUMBER`, `OVERRIDE_EMPLOYEE_ID` when manager-authorized |

### Totals panel

| UI element | Backed by |
|---|---|
| Subtotal | `POS_TRANSACTION.SUBTOTAL_AMT` = Σ `EXTENDED_AMT` of non-`DISCOUNT`, non-voided lines |
| Discounts | `POS_TRANSACTION.DISCOUNT_TOTAL_AMT` = ABS(Σ `EXTENDED_AMT` of `DISCOUNT` lines), stored positive |
| Taxable amount | `POS_TRANSACTION.TAXABLE_AMT` (lines with `TAXABLE_FLAG = 'Y'`, zero when the sale is tax exempt) |
| Tax rate (e.g. 9.250%) | `POS_TRANSACTION.TAX_RATE` (defaulted from `STORE.DEFAULT_TAX_RATE`) |
| Tax amount | `POS_TRANSACTION.TAX_AMT` (also per line in `POS_TRANSACTION_LINE.TAX_AMT`) |
| Total | `POS_TRANSACTION.TOTAL_AMT` = `SUBTOTAL_AMT` − `DISCOUNT_TOTAL_AMT` + `TAX_AMT` |
| Amount tendered / balance due | `POS_TRANSACTION.TENDERED_AMT`, `BALANCE_DUE_AMT` |
| Change due | `POS_TRANSACTION.CHANGE_DUE_AMT` |

### Tender / payment panel

| UI element | Backed by |
|---|---|
| Each payment row | one `POS_TENDER` row; `TENDER_SEQ` gives display order |
| "Commercial revolving charge" | `POS_TENDER.TENDER_TYPE = 'COMMERCIAL_CHARGE'`, `TENDER_LABEL`, `CHARGE_ACCOUNT_NO` |
| "Credit / debit" | `TENDER_TYPE = 'CREDIT'` or `'DEBIT'`, plus `CARD_BRAND`, `CARD_LAST4`, `ENTRY_METHOD`, `AUTHORIZATION_CODE` |
| "Cash" | `TENDER_TYPE = 'CASH'`, `AMOUNT_AMT`, `CHANGE_AMT` |
| Split tender (multiple payments on one sale) | multiple `POS_TENDER` rows sharing `TRANSACTION_ID`; the UI's remaining-balance figure is `POS_TRANSACTION.BALANCE_DUE_AMT` |
| Declined / voided payment states | `POS_TENDER.TENDER_STATUS` |

### "Convert quote" flow

| UI step | Backed by |
|---|---|
| Quote lookup by number | `QUOTE.QUOTE_NUMBER` (unique index), filtered on `QUOTE_STATUS = 'OPEN'` and `EXPIRATION_DATE` |
| Quote preview (customer, job, totals) | `QUOTE.CUSTOMER_ACCOUNT_ID`, `JOB_REFERENCE_ID`, `SUBTOTAL_AMT`, `DISCOUNT_TOTAL_AMT`, `TAX_RATE`, `TAX_AMT`, `TOTAL_AMT` |
| Quote line preview | `QUOTE_LINE` (column-for-column mirror of `POS_TRANSACTION_LINE`) |
| Convert action | insert `POS_TRANSACTION` with `SOURCE_QUOTE_ID = QUOTE.QUOTE_ID`; `INSERT ... SELECT` `QUOTE_LINE` → `POS_TRANSACTION_LINE` |
| Post-conversion state | `QUOTE.QUOTE_STATUS = 'CONVERTED'`, `CONVERTED_STORE_ID`, `CONVERTED_REGISTER_ID`, `CONVERTED_TXN_NUMBER`, `CONVERTED_TS` |

---

## Conventions the webapp should rely on

1. **Receipt identity** is `(STORE_ID, REGISTER_ID, BUSINESS_DATE, TRANSACTION_NUMBER)`;
   `TRANSACTION_ID` is an internal surrogate and should not be shown to users.
2. **Snapshot columns** (`DESCRIPTION_SNAPSHOT`, `UNIT_PRICE`, `UNIT_OF_MEASURE`,
   `AISLE_SNAPSHOT`, `BAY_SNAPSHOT`, `DEPARTMENT_SNAPSHOT`) are written at ring time,
   so a reprinted receipt is stable even after catalog edits.
3. **Discount sign convention**: `DISCOUNT` lines are negative; the header
   `DISCOUNT_TOTAL_AMT` is positive.
4. **Non-scan services** still resolve to a catalog row (`PRODUCT.IS_SCANNABLE = 'N'`,
   `PRODUCT_TYPE = 'SERVICE'`) so pricing and reporting stay uniform.
5. **Split tender** is the default shape: always read `POS_TENDER` as a collection,
   never as a single payment.
6. `REFERENCES WITH NO CHECK OPTION` (soft RI) is used throughout, per Teradata
   practice — relationships are declared for the optimizer and for documentation, but
   the application is responsible for enforcing them on write.
