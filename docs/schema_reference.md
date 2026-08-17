# Schema Reference

Consolidated reference of every table in the retail banking analytics pipeline,
grouped by layer. Types are the Teradata DDL types (the system of record for the
schema); the DuckDB mirror used by `export_data.py` and the exported CSVs are
cross-checked against them, with all differences listed in
[DDL vs. DuckDB/CSV cross-check](#ddl-vs-duckdbcsv-cross-check).

Database names come from `config/pipeline_config.cfg`:

| Layer | Database | DDL file | Exported CSVs |
|---|---|---|---|
| Source | `CORE_BANKING_DB`, `TXN_PROCESSING_DB` | `ddl/00_source_tables.sql` | `data/01_source_tables/` |
| Staging | `ETL_STAGING_DB` | `ddl/01_staging_tables.sql` | `data/02_bteq_staging/` |
| Data products | `DATA_PRODUCTS_DB` | `ddl/02_data_product_tables.sql` | `data/03_sas_data_products/` |

All Teradata tables are `MULTISET ... NO FALLBACK`; `CHARACTER SET LATIN NOT
CASESPECIFIC` is implied on every character column and omitted below for brevity.
"Key" refers to the Teradata PRIMARY INDEX (distribution key), not an enforced
constraint — only `TRANSACTION_TYPES` uses a UNIQUE PRIMARY INDEX. There are no
declared foreign keys anywhere in the schema; joins rely on convention.

---

## 1. Source layer (read-only, owned by the core banking platform)

### CORE_BANKING_DB.CUSTOMERS
Primary index: `CUSTOMER_ID`

| Column | Type | Notes |
|---|---|---|
| CUSTOMER_ID | BIGINT NOT NULL | PI |
| FIRST_NAME | VARCHAR(60) | |
| LAST_NAME | VARCHAR(60) | |
| DATE_OF_BIRTH | DATE | |
| SSN_HASH | CHAR(64) | SHA-256 hex |
| EMAIL | VARCHAR(120) | |
| PHONE_PRIMARY | VARCHAR(20) | |
| CUSTOMER_SINCE | DATE | |
| CUSTOMER_STATUS | CHAR(1) | A=Active, I=Inactive, C=Closed |
| SEGMENT_CODE | VARCHAR(10) | |
| BRANCH_ID | INTEGER | no BRANCHES table exists |
| CREATED_TS | TIMESTAMP(6) | |
| UPDATED_TS | TIMESTAMP(6) | |

### CORE_BANKING_DB.ACCOUNTS
Primary index: `ACCOUNT_ID`

| Column | Type | Notes |
|---|---|---|
| ACCOUNT_ID | BIGINT NOT NULL | PI |
| CUSTOMER_ID | BIGINT NOT NULL | → CUSTOMERS.CUSTOMER_ID |
| ACCOUNT_TYPE | VARCHAR(20) | CHECKING, SAVINGS, CREDIT, LOAN |
| ACCOUNT_STATUS | CHAR(1) | O=Open, C=Closed, F=Frozen |
| OPEN_DATE | DATE | |
| CLOSE_DATE | DATE | |
| CURRENT_BALANCE | DECIMAL(15,2) | |
| AVAILABLE_BALANCE | DECIMAL(15,2) | |
| CREDIT_LIMIT | DECIMAL(15,2) | |
| INTEREST_RATE | DECIMAL(5,4) | |
| BRANCH_ID | INTEGER | |
| CREATED_TS | TIMESTAMP(6) | |
| UPDATED_TS | TIMESTAMP(6) | |

### CORE_BANKING_DB.ADDRESSES
Primary index: `ADDRESS_ID`. SCD-style validity via `EFFECTIVE_DATE`/`EXPIRATION_DATE`.

| Column | Type | Notes |
|---|---|---|
| ADDRESS_ID | BIGINT NOT NULL | PI |
| CUSTOMER_ID | BIGINT NOT NULL | → CUSTOMERS.CUSTOMER_ID |
| ADDRESS_TYPE | VARCHAR(10) | MAIL, HOME, WORK |
| ADDRESS_LINE_1 | VARCHAR(100) | |
| ADDRESS_LINE_2 | VARCHAR(100) | |
| CITY | VARCHAR(60) | |
| STATE_CODE | CHAR(2) | |
| ZIP_CODE | VARCHAR(10) | |
| COUNTRY_CODE | CHAR(2) DEFAULT 'US' | |
| IS_PRIMARY | CHAR(1) DEFAULT 'N' | Y/N |
| EFFECTIVE_DATE | DATE | |
| EXPIRATION_DATE | DATE | |
| CREATED_TS | TIMESTAMP(6) | |
| UPDATED_TS | TIMESTAMP(6) | |

### TXN_PROCESSING_DB.TRANSACTIONS
Primary index: `TRANSACTION_ID`. Partitioned by `RANGE_N(TRANSACTION_DATE ... EACH INTERVAL '1' MONTH)` over 2020-01-01..2030-12-31.

| Column | Type | Notes |
|---|---|---|
| TRANSACTION_ID | BIGINT NOT NULL | PI |
| ACCOUNT_ID | BIGINT NOT NULL | → ACCOUNTS.ACCOUNT_ID (customer only via ACCOUNTS) |
| TRANSACTION_TYPE_CD | VARCHAR(10) | → TRANSACTION_TYPES |
| TRANSACTION_DATE | DATE | partition column |
| TRANSACTION_TS | TIMESTAMP(6) | |
| AMOUNT | DECIMAL(15,2) | signed; debits negative in generated data |
| RUNNING_BALANCE | DECIMAL(15,2) | |
| MERCHANT_NAME | VARCHAR(100) | nullable |
| MERCHANT_CATEGORY | VARCHAR(60) | free-text category, no lookup table |
| CHANNEL_CODE | VARCHAR(10) | ATM, POS, WEB, MOB, ACH, WIRE (per comment) |
| REFERENCE_NUM | VARCHAR(40) | |
| STATUS_CODE | CHAR(1) | P=Posted, R=Reversed, H=Hold |
| CREATED_TS | TIMESTAMP(6) | |

### TXN_PROCESSING_DB.TRANSACTION_TYPES
Unique primary index: `TRANSACTION_TYPE_CD`

| Column | Type | Notes |
|---|---|---|
| TRANSACTION_TYPE_CD | VARCHAR(10) NOT NULL | UPI |
| DESCRIPTION | VARCHAR(60) | |
| CATEGORY | VARCHAR(30) | DEBIT, CREDIT, FEE, INTEREST |
| IS_REVENUE | CHAR(1) DEFAULT 'N' | |
| EFFECTIVE_DATE | DATE | |
| EXPIRATION_DATE | DATE | |

Seeded codes (`data/01_source_tables/transaction_types.csv`): PUR (Purchase, DEBIT),
WDR (Withdrawal, DEBIT), TRF (Transfer, DEBIT), DEP (Deposit, CREDIT),
PMT (Payment, CREDIT), INT (Interest, CREDIT, revenue), FEE (Account Fee, FEE, revenue),
NSF (NSF Fee, FEE, revenue).

### CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES — undocumented in DDL
Read by `bteq/03_stg_risk_factors.bteq` and created by `export_data.py`, but **absent
from `ddl/00_source_tables.sql`**. Observed shape:

| Column | Type (DuckDB) | Notes |
|---|---|---|
| CUSTOMER_ID | BIGINT | |
| EXTERNAL_CREDIT_SCORE | INTEGER | BTEQ aliases it as `CREDIT_SCORE` |
| REPORT_DATE | DATE | latest row per customer taken via QUALIFY ROW_NUMBER |

---

## 2. Staging layer (`ETL_STAGING_DB`, DROP/CREATE each run)

### STG_CUSTOMER_360
Primary index: `CUSTOMER_ID`. Produced by `bteq/01_stg_customer_360.bteq`, consumed by `sas/01_sas_customer_segments.sas`.

| Column | Type |
|---|---|
| CUSTOMER_ID | BIGINT NOT NULL (PI) |
| FIRST_NAME | VARCHAR(60) |
| LAST_NAME | VARCHAR(60) |
| DATE_OF_BIRTH | DATE |
| AGE | SMALLINT |
| CUSTOMER_SINCE | DATE |
| TENURE_MONTHS | INTEGER |
| CUSTOMER_STATUS | CHAR(1) |
| SEGMENT_CODE | VARCHAR(10) |
| BRANCH_ID | INTEGER |
| PRIMARY_ADDRESS | VARCHAR(200) |
| CITY | VARCHAR(60) |
| STATE_CODE | CHAR(2) |
| ZIP_CODE | VARCHAR(10) |
| NUM_ACCOUNTS | SMALLINT |
| NUM_ACTIVE_ACCOUNTS | SMALLINT |
| HAS_CHECKING / HAS_SAVINGS / HAS_CREDIT / HAS_LOAN | CHAR(1) DEFAULT 'N' |
| TOTAL_BALANCE | DECIMAL(18,2) |
| TOTAL_CREDIT_LIMIT | DECIMAL(18,2) |
| CREDIT_UTILIZATION_PCT | DECIMAL(5,2) |
| LOAD_TS | TIMESTAMP(6) |

### STG_TXN_SUMMARY
Primary index: `(CUSTOMER_ID, ACCOUNT_ID)` — grain is one row per account. Produced by `bteq/02_stg_txn_summary.bteq`, consumed by `sas/02_sas_txn_analytics.sas`.

| Column | Type |
|---|---|
| CUSTOMER_ID | BIGINT NOT NULL (PI) |
| ACCOUNT_ID | BIGINT NOT NULL (PI) |
| ACCOUNT_TYPE | VARCHAR(20) |
| SUMMARY_PERIOD_START / SUMMARY_PERIOD_END | DATE |
| TXN_COUNT_TOTAL / TXN_COUNT_DEBIT / TXN_COUNT_CREDIT / TXN_COUNT_FEE | INTEGER |
| AMT_TOTAL_DEBIT / AMT_TOTAL_CREDIT / AMT_TOTAL_FEES | DECIMAL(18,2) |
| AMT_AVG_DEBIT / AMT_AVG_CREDIT | DECIMAL(15,2) |
| AMT_MAX_SINGLE_DEBIT / AMT_MAX_SINGLE_CREDIT | DECIMAL(15,2) |
| DISTINCT_MERCHANTS | INTEGER |
| TOP_MERCHANT_CATEGORY | VARCHAR(60) |
| PCT_ATM / PCT_POS / PCT_WEB / PCT_MOBILE | DECIMAL(5,2) |
| DAYS_SINCE_LAST_TXN | INTEGER |
| LOAD_TS | TIMESTAMP(6) |

`PCT_POS` is computed in `02_stg_txn_summary.bteq` as
`COUNT(CHANNEL_CODE = 'POS') * 100.0 / COUNT(*)` per account — the only POS-specific
field in the entire pipeline.

### STG_RISK_FACTORS
Primary index: `CUSTOMER_ID`. Produced by `bteq/03_stg_risk_factors.bteq`, consumed by `sas/03_sas_risk_scoring.sas`.

| Column | Type |
|---|---|
| CUSTOMER_ID | BIGINT NOT NULL (PI) |
| ACCOUNT_OVERDRAFT_CNT | INTEGER |
| NSF_FEE_TOTAL | DECIMAL(15,2) |
| LARGE_WITHDRAWAL_CNT | INTEGER |
| LARGE_WITHDRAWAL_AMT | DECIMAL(18,2) |
| AVG_DAILY_BALANCE_30D / AVG_DAILY_BALANCE_90D | DECIMAL(15,2) |
| BALANCE_VOLATILITY | DECIMAL(10,4) |
| CREDIT_UTIL_RATIO | DECIMAL(5,4) |
| PAYMENT_ONTIME_PCT | DECIMAL(5,2) |
| PAYMENT_LATE_CNT | INTEGER |
| MONTHS_SINCE_LAST_LATE | INTEGER |
| EXTERNAL_CREDIT_SCORE | INTEGER |
| DEBIT_VELOCITY_7D / DEBIT_VELOCITY_30D | DECIMAL(15,2) |
| NEW_MERCHANT_CNT_30D | INTEGER |
| INTERNATIONAL_TXN_CNT | INTEGER |
| HIGH_RISK_MERCHANT_CNT | INTEGER |
| LOAD_TS | TIMESTAMP(6) |

Transient work tables created and dropped inside the BTEQ scripts (`WRK_*`, e.g.
`WRK_PAYMENT_HISTORY`) are not part of the published schema.

---

## 3. Data product layer (`DATA_PRODUCTS_DB`, the downstream contract)

### CUSTOMER_SEGMENTS
Primary index: `CUSTOMER_ID`. Daily refresh; consumers: marketing campaign engine, CRM.

| Column | Type |
|---|---|
| CUSTOMER_ID | BIGINT NOT NULL (PI) |
| SEGMENT_NAME | VARCHAR(40) |
| SEGMENT_ID / SUBSEGMENT_ID | SMALLINT |
| LIFETIME_VALUE_SCORE | DECIMAL(10,2) |
| ENGAGEMENT_SCORE | DECIMAL(5,2) |
| DIGITAL_ADOPTION_SCORE | DECIMAL(5,2) |
| PRODUCT_BREADTH_INDEX | DECIMAL(5,2) |
| TENURE_GROUP / AGE_GROUP / BALANCE_TIER | VARCHAR(20) |
| CHANNEL_PREFERENCE | VARCHAR(10) |
| CROSS_SELL_FLAG / UPSELL_FLAG / RETENTION_RISK_FLAG | CHAR(1) DEFAULT 'N' |
| MODEL_VERSION | VARCHAR(20) |
| EFFECTIVE_DATE | DATE |
| LOAD_TS | TIMESTAMP(6) |

### TRANSACTION_ANALYTICS
Primary index: `CUSTOMER_ID`; `PARTITION BY COLUMN(REPORTING_PERIOD VARCHAR(7))`.

| Column | Type | Notes |
|---|---|---|
| CUSTOMER_ID | BIGINT NOT NULL | PI |
| REPORTING_PERIOD | VARCHAR(7) | YYYY-MM |
| TOTAL_ACCOUNTS / ACTIVE_ACCOUNTS | SMALLINT | |
| TOTAL_TRANSACTIONS | INTEGER | |
| TOTAL_DEBIT_AMT / TOTAL_CREDIT_AMT / NET_CASH_FLOW | DECIMAL(18,2) | |
| AVG_TRANSACTION_SIZE | DECIMAL(15,2) | |
| MONTHLY_SPEND_TREND | VARCHAR(10) | UP, DOWN, STABLE |
| SPEND_PERCENTILE | DECIMAL(5,2) | |
| TOP_SPEND_CATEGORY | VARCHAR(60) | |
| DIGITAL_TXN_PCT | DECIMAL(5,2) | |
| FEE_INCOME / INTEREST_INCOME / REVENUE_CONTRIBUTION | DECIMAL(15,2) | |
| ANOMALY_FLAG | CHAR(1) DEFAULT 'N' | |
| MODEL_VERSION | VARCHAR(20) | |
| EFFECTIVE_DATE | DATE | |
| LOAD_TS | TIMESTAMP(6) | |

### CUSTOMER_RISK_SCORES
Primary index: `CUSTOMER_ID`.

| Column | Type | Notes |
|---|---|---|
| CUSTOMER_ID | BIGINT NOT NULL | PI |
| COMPOSITE_RISK_SCORE | DECIMAL(6,2) | |
| RISK_TIER | VARCHAR(20) | LOW, MODERATE, ELEVATED, HIGH, CRITICAL |
| PROBABILITY_OF_DEFAULT | DECIMAL(7,6) | |
| CREDIT_RISK_COMPONENT / BEHAVIOUR_RISK_COMPONENT / VELOCITY_RISK_COMPONENT / BUREAU_SCORE_COMPONENT / PAYMENT_HISTORY_COMPONENT | DECIMAL(5,2) | |
| PRIMARY_RISK_DRIVER / SECONDARY_RISK_DRIVER | VARCHAR(40) | |
| SCORE_DELTA_30D | DECIMAL(6,2) | |
| WATCH_LIST_FLAG / REVIEW_REQUIRED_FLAG | CHAR(1) DEFAULT 'N' | |
| MODEL_VERSION | VARCHAR(20) | |
| EFFECTIVE_DATE | DATE | |
| LOAD_TS | TIMESTAMP(6) | |

### CUSTOMER_MASTER_PROFILE
Primary index: `CUSTOMER_ID`. Golden record assembled from the three products above plus `STG_CUSTOMER_360`.

| Group | Columns |
|---|---|
| Identity | CUSTOMER_ID BIGINT NOT NULL, FULL_NAME VARCHAR(120), AGE SMALLINT, STATE_CODE CHAR(2), CUSTOMER_SINCE DATE, TENURE_MONTHS INTEGER, CUSTOMER_STATUS CHAR(1) |
| Segment | SEGMENT_NAME VARCHAR(40), LIFETIME_VALUE_SCORE DECIMAL(10,2), ENGAGEMENT_SCORE DECIMAL(5,2) |
| Accounts | TOTAL_ACCOUNTS SMALLINT, ACTIVE_ACCOUNTS SMALLINT, TOTAL_BALANCE DECIMAL(18,2), TOTAL_CREDIT_LIMIT DECIMAL(18,2), CREDIT_UTILIZATION_PCT DECIMAL(5,2) |
| Transactions | MONTHLY_TRANSACTIONS INTEGER, MONTHLY_SPEND DECIMAL(18,2), NET_CASH_FLOW DECIMAL(18,2), TOP_SPEND_CATEGORY VARCHAR(60), DIGITAL_TXN_PCT DECIMAL(5,2) |
| Risk | COMPOSITE_RISK_SCORE DECIMAL(6,2), RISK_TIER VARCHAR(20), PROBABILITY_OF_DEFAULT DECIMAL(7,6), WATCH_LIST_FLAG CHAR(1) |
| Flags | CROSS_SELL_FLAG, UPSELL_FLAG, RETENTION_RISK_FLAG (all CHAR(1) DEFAULT 'N') |
| Metadata | MODEL_VERSION VARCHAR(20), EFFECTIVE_DATE DATE, LOAD_TS TIMESTAMP(6) |

---

## DDL vs. DuckDB/CSV cross-check

`export_data.py` (lines 64–69) mirrors only the **source** tables; staging and data
product tables are created inside `local/duckdb/run_demo.py`, which is **not present in
this repository** (`export_data.py` imports it from `local/duckdb`), so those layers were
verified against the exported CSV headers in `data/` instead.

Column names and ordering match the DDL exactly for `CUSTOMERS`, `ACCOUNTS`,
`ADDRESSES`, `TRANSACTIONS`, `TRANSACTION_TYPES`, `STG_CUSTOMER_360`,
`STG_TXN_SUMMARY`, `STG_RISK_FACTORS`, `CUSTOMER_SEGMENTS`, `CUSTOMER_RISK_SCORES`
and `CUSTOMER_MASTER_PROFILE`. Differences found:

1. **`CUSTOMER_BUREAU_SCORES` has no DDL.** Created in `export_data.py` and queried by
   `bteq/03_stg_risk_factors.bteq`, but missing from `ddl/00_source_tables.sql`.
2. **`TRANSACTION_ANALYTICS` has an extra column in the data.** The CSV contains
   `total_fees` (between `total_credit_amt` and `top_spend_category`), which is not in
   the DDL; the CSV column order also differs from the DDL (`reporting_period` is second
   in the DDL, second-to-last in the CSV). Column order is irrelevant for a
   name-addressed contract but `total_fees` is a real gap.
3. **Type fidelity is lossy in the DuckDB mirror** (expected, but relevant if the webapp
   reads CSVs rather than Teradata): every `CHAR(n)`/`VARCHAR(n)` becomes unbounded
   `VARCHAR`, so `CUSTOMER_STATUS`, `STATUS_CODE`, `IS_PRIMARY`, `IS_REVENUE`,
   `COUNTRY_CODE`, `STATE_CODE`, `SSN_HASH` lose their length constraints;
   `TIMESTAMP(6)` becomes `TIMESTAMP`; `DEFAULT` clauses and the `NOT NULL` on
   `ACCOUNTS.CUSTOMER_ID` / `TRANSACTIONS.ACCOUNT_ID` are not reproduced.
4. **Partitioning and statistics are Teradata-only** — no equivalent in the DuckDB
   mirror. `TRANSACTIONS` is monthly range-partitioned; `TRANSACTION_ANALYTICS` is
   column-partitioned by `REPORTING_PERIOD`.
5. **`TRANSACTION_TYPES.CATEGORY` documents four values** (DEBIT, CREDIT, FEE, INTEREST)
   but only three are seeded — `INT` (Interest) is categorised as `CREDIT`, so
   `INTEREST` never appears.
6. **`CHANNEL_CODE = 'INTL'`** is counted by `bteq/03_stg_risk_factors.bteq`
   (`INTERNATIONAL_TXN_CNT`) but is not in the DDL's documented value list and never
   occurs in the generated data, so `INTERNATIONAL_TXN_CNT` is always 0.

Generated data volumes (`data/`): 500 customers, 1,251 accounts, 1,000 addresses,
80,528 transactions, 8 transaction types, 500 bureau scores; 478 `STG_CUSTOMER_360`
rows, 1,251 `STG_TXN_SUMMARY` rows, 478 `STG_RISK_FACTORS` rows; 407 rows in each
customer-level data product and 500 in `TRANSACTION_ANALYTICS`.

---

## POS-relevant surface, and gaps

There is **no POS entity model in this schema.** POS exists in exactly two places:

- `TXN_PROCESSING_DB.TRANSACTIONS.CHANNEL_CODE = 'POS'` — a value, not a table.
- `ETL_STAGING_DB.STG_TXN_SUMMARY.PCT_POS` — the share of an account's transactions on
  that channel.

Everything a POS webapp could read today is on the transaction row:

| Field | Type | Availability in generated data |
|---|---|---|
| TRANSACTION_ID | BIGINT | always populated, unique |
| ACCOUNT_ID | BIGINT | always populated; customer only reachable via `ACCOUNTS` |
| TRANSACTION_TYPE_CD | VARCHAR(10) | PUR / WDR / TRF / DEP / PMT / FEE / NSF / INT — not POS-specific |
| TRANSACTION_TS | TIMESTAMP(6) | populated (plus `TRANSACTION_DATE`) |
| AMOUNT | DECIMAL(15,2) | signed, gross only — no tax/tip/discount split |
| MERCHANT_NAME | VARCHAR(100) | **NULL on ~65% of POS rows** (10,403 of 16,100) |
| MERCHANT_CATEGORY | VARCHAR(60) | same ~65% NULL; free text, no lookup table |
| CHANNEL_CODE | VARCHAR(10) | 16,100 POS rows out of 80,528 (~20%) |
| REFERENCE_NUM | VARCHAR(40) | populated, `REF%08d`, unique per transaction |
| STATUS_CODE | CHAR(1) | P=Posted (67%), H=Hold (17%), R=Reversed (17%) |

Also available if needed: `RUNNING_BALANCE`, `CREATED_TS`, and — via `ACCOUNTS` —
`CUSTOMER_ID`, `ACCOUNT_TYPE`, `ACCOUNT_STATUS`, `BRANCH_ID`.

### Gaps versus what a POS webapp typically needs

Nothing below exists anywhere in the schema:

- **Acquiring/terminal context:** no `TERMINAL_ID`, `STORE_ID`, `LANE_ID`,
  `CASHIER_ID`/operator, `MERCHANT_ID` (merchants are free-text names, not entities),
  `BATCH_ID`, or settlement/batch close.
- **Basket detail:** no line items, SKU/product, quantity, unit price, tax, tip,
  discount, or order/receipt entity. `AMOUNT` is a single gross figure.
- **Card/payment instrument:** no PAN last-four, card brand, entry mode
  (chip/swipe/contactless/keyed), `AUTH_CODE`, approval/decline reason, EMV data,
  or tokenized card reference. `REFERENCE_NUM` is a generic reference, not an auth code.
- **Authorization lifecycle:** `STATUS_CODE` has only P/R/H — no
  authorized/captured/settled/voided/partially-refunded distinction, no link between a
  refund/reversal and the original transaction (no `ORIGINAL_TRANSACTION_ID`).
- **Geography/device:** no store address, geolocation, timezone, or device/POS software
  version. `BRANCH_ID` is a bank branch, not a merchant store, and has no dimension table.
- **Currency:** no `CURRENCY_CODE`, FX rate, or amount-in-original-currency; USD is implied.
- **Idempotency/audit:** no client-generated idempotency key, no update timestamp on
  transactions (only `CREATED_TS`), and no soft-delete/versioning.
- **Referential integrity and grain:** no foreign keys; a POS webapp writing here would
  have to enforce `ACCOUNT_ID` validity itself, and there is no per-merchant or
  per-terminal aggregate anywhere in the staging or data product layers (only the
  per-account `PCT_POS`).

### Practical notes for contract comparison

- The pipeline layers are **customer/account-grained**, not merchant-grained. Any POS
  analytics beyond `PCT_POS` requires new staging and data product tables, not just new
  source columns.
- `MERCHANT_CATEGORY` is not an MCC code — values are labels such as `RETAIL`,
  `GROCERY`, `RESTAURANT`, `GAS_STATION`, `TRAVEL`, `HEALTHCARE`, `UTILITIES`,
  `GAMBLING`, `CRYPTO_EXCHANGE`. `bteq/03_stg_risk_factors.bteq` hard-codes
  `GAMBLING`, `WIRE_TRANSFER_INTL`, `CRYPTO_EXCHANGE`, `PAWN_SHOP` as high-risk, so
  changing this vocabulary breaks risk scoring.
- Adding columns to `TRANSACTIONS` affects a partitioned table that three BTEQ scripts
  read with `SELECT`-listed columns; adding a separate POS detail table keyed by
  `TRANSACTION_ID` would leave the existing pipeline untouched.
- No new tables have been added in this document — it is a description of the current
  state only, pending the POS webapp's required schema.
