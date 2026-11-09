# C1 — Schema contract (tables, columns, types, distribution)

| | |
|---|---|
| **Owner** | [T2](../../tickets/TICKET-02.md) — implements it as DDL in `synapse/sql/ddl/` and is the only ticket allowed to amend it |
| **Consumers** | T3 (loads source tables), T4–T6 (read source, write staging), T7–T9 (read staging, write data products), T10 (post-run row counts), T9 audit (`ETL_RUN_LOG`) |
| **Status** | **FROZEN v1.0** (prerequisite contract — must not change after dispatch without a contract-change PR, see [README](README.md)) |
| **Source of truth** | `ddl/00_source_tables.sql`, `ddl/01_staging_tables.sql`, `ddl/02_data_product_tables.sql`; `export_data.py` lines 64-80 (source table/column lists, incl. `customer_bureau_scores`); `bteq/03_stg_risk_factors.bteq` (`CUSTOMER_BUREAU_SCORES`, `ETL_RUN_LOG` usage) |

## 1. Database / schema mapping

All four Teradata databases become **schemas with the same names** inside one dedicated SQL
pool database (`rbapdw`, see C6). Object names are unchanged, so `CORE_BANKING_DB.CUSTOMERS`
is still referenced as `CORE_BANKING_DB.CUSTOMERS` in T-SQL (two-part names; never three-part).

| Teradata database | Synapse schema | SAS libref (old) | Contents |
|---|---|---|---|
| `CORE_BANKING_DB` | `CORE_BANKING_DB` | `COREDB` | `CUSTOMERS`, `ACCOUNTS`, `ADDRESSES`, `CUSTOMER_BUREAU_SCORES` |
| `TXN_PROCESSING_DB` | `TXN_PROCESSING_DB` | `TXNDB` | `TRANSACTIONS`, `TRANSACTION_TYPES` |
| `ETL_STAGING_DB` | `ETL_STAGING_DB` | `STGDB` | `STG_CUSTOMER_360`, `STG_TXN_SUMMARY`, `STG_RISK_FACTORS`, `ETL_RUN_LOG`, all stored procedures |
| `DATA_PRODUCTS_DB` | `DATA_PRODUCTS_DB` | `DPDB` | `CUSTOMER_SEGMENTS`, `TRANSACTION_ANALYTICS`, `CUSTOMER_RISK_SCORES`, `CUSTOMER_MASTER_PROFILE` |

Spark jobs refer to tables as `"<SCHEMA>.<TABLE>"` strings, e.g. `"ETL_STAGING_DB.STG_CUSTOMER_360"`.
Column names are **UPPER_SNAKE_CASE** exactly as below in SQL and in Spark DataFrames that are
written back. (The reference CSVs under `data/` use lower-case headers; map them case-insensitively.)

## 2. Global type and DDL rules

| Teradata | Synapse dedicated SQL pool |
|---|---|
| `CREATE MULTISET TABLE ..., NO FALLBACK` | `CREATE TABLE ... WITH (DISTRIBUTION = ..., <index>)` (dedicated pools have no SET/MULTISET; duplicates always allowed) |
| `PRIMARY INDEX (c)` / `UNIQUE PRIMARY INDEX (c)` | `DISTRIBUTION = HASH(c)` / `REPLICATE` / `ROUND_ROBIN` per table below. **No** `PRIMARY KEY`/`UNIQUE` constraints (NOT ENFORCED constraints give wrong results if violated); uniqueness is checked by the C3 validation helpers |
| (implicit row store) | `CLUSTERED COLUMNSTORE INDEX` unless the table spec says otherwise |
| `PARTITION BY RANGE_N(TRANSACTION_DATE ... EACH INTERVAL '1' MONTH)` | `PARTITION (TRANSACTION_DATE RANGE RIGHT FOR VALUES ('2021-01-01', ..., '2030-01-01'))` — **yearly**, not monthly (60 distributions × monthly partitions would starve columnstore row groups) |
| `PARTITION BY COLUMN(...)` (Teradata columnar) | dropped — CCI is already columnar |
| `COLLECT STATISTICS COLUMN (c) ON t` | `CREATE STATISTICS stat_<TABLE>_<COL>[_<COL2>] ON <schema>.<TABLE> (<cols>)` created once in DDL; loaders run `UPDATE STATISTICS <schema>.<TABLE>` after each load |
| `VARCHAR(n) CHARACTER SET LATIN NOT CASESPECIFIC` | `VARCHAR(n)`; case-insensitivity comes from the database collation `SQL_Latin1_General_CP1_CI_AS` (set by T1) |
| `CHAR(n)` | `CHAR(n)` |
| `BIGINT` / `INTEGER` / `SMALLINT` | `BIGINT` / `INT` / `SMALLINT` |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` |
| `DATE FORMAT 'YYYY-MM-DD'` | `DATE` (FORMAT clauses dropped) |
| `TIMESTAMP(6)` | `DATETIME2(6)`; all `LOAD_TS` values are **UTC** |
| `DEFAULT 'N'` | `DEFAULT 'N'` (constant defaults only) |
| `NOT NULL` | `NOT NULL` (kept exactly where Teradata had it, plus the additions marked ★) |

Statistics objects are named `stat_<TABLE>_<COL>` (multi-column: `stat_<TABLE>_<COL1>_<COL2>`).

## 3. Tables

Notation: `COLUMN  TYPE  [NOT NULL]  [DEFAULT x]`, in the exact physical column order that
`INSERT ... SELECT` / Spark write-back must follow.

### 3.1 `CORE_BANKING_DB.CUSTOMERS` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
```
CUSTOMER_ID      BIGINT        NOT NULL
FIRST_NAME       VARCHAR(60)
LAST_NAME        VARCHAR(60)
DATE_OF_BIRTH    DATE
SSN_HASH         CHAR(64)
EMAIL            VARCHAR(120)
PHONE_PRIMARY    VARCHAR(20)
CUSTOMER_SINCE   DATE
CUSTOMER_STATUS  CHAR(1)                       -- A=Active, I=Inactive, C=Closed
SEGMENT_CODE     VARCHAR(10)
BRANCH_ID        INT
CREATED_TS       DATETIME2(6)
UPDATED_TS       DATETIME2(6)
```
Statistics: `CUSTOMER_ID`, `CUSTOMER_STATUS`.

### 3.2 `CORE_BANKING_DB.ACCOUNTS` — DISTRIBUTION = HASH(ACCOUNT_ID), CCI
(Hash on `ACCOUNT_ID` to co-locate the large `TRANSACTIONS ⋈ ACCOUNTS` join.)
```
ACCOUNT_ID         BIGINT        NOT NULL
CUSTOMER_ID        BIGINT        NOT NULL
ACCOUNT_TYPE       VARCHAR(20)                 -- CHECKING, SAVINGS, CREDIT, LOAN
ACCOUNT_STATUS     CHAR(1)                     -- O=Open, C=Closed, F=Frozen
OPEN_DATE          DATE
CLOSE_DATE         DATE
CURRENT_BALANCE    DECIMAL(15,2)
AVAILABLE_BALANCE  DECIMAL(15,2)
CREDIT_LIMIT       DECIMAL(15,2)
INTEREST_RATE      DECIMAL(5,4)
BRANCH_ID          INT
CREATED_TS         DATETIME2(6)
UPDATED_TS         DATETIME2(6)
```
Statistics: `ACCOUNT_ID`, `CUSTOMER_ID`, `ACCOUNT_TYPE`.

### 3.3 `CORE_BANKING_DB.ADDRESSES` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
```
ADDRESS_ID       BIGINT        NOT NULL
CUSTOMER_ID      BIGINT        NOT NULL
ADDRESS_TYPE     VARCHAR(10)                   -- MAIL, HOME, WORK
ADDRESS_LINE_1   VARCHAR(100)
ADDRESS_LINE_2   VARCHAR(100)
CITY             VARCHAR(60)
STATE_CODE       CHAR(2)
ZIP_CODE         VARCHAR(10)
COUNTRY_CODE     CHAR(2)                       DEFAULT 'US'
IS_PRIMARY       CHAR(1)                       DEFAULT 'N'
EFFECTIVE_DATE   DATE
EXPIRATION_DATE  DATE
CREATED_TS       DATETIME2(6)
UPDATED_TS       DATETIME2(6)
```
Statistics: `CUSTOMER_ID`, `ADDRESS_TYPE`.

### 3.4 `CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES` ★ new — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
Not defined anywhere in `ddl/` but read by `bteq/03_stg_risk_factors.bteq` and exported by
`export_data.py` (line 69: `customer_id BIGINT, external_credit_score INTEGER, report_date DATE`).
```
CUSTOMER_ID            BIGINT   NOT NULL
EXTERNAL_CREDIT_SCORE  INT
REPORT_DATE            DATE
```
Statistics: `CUSTOMER_ID`.

### 3.5 `TXN_PROCESSING_DB.TRANSACTIONS` — DISTRIBUTION = HASH(ACCOUNT_ID), CCI, yearly RANGE RIGHT partitions on `TRANSACTION_DATE` (boundaries `2021-01-01` … `2030-01-01`)
(No `CUSTOMER_ID` column — customer is reached via `ACCOUNTS`.)
```
TRANSACTION_ID       BIGINT        NOT NULL
ACCOUNT_ID           BIGINT        NOT NULL
TRANSACTION_TYPE_CD  VARCHAR(10)
TRANSACTION_DATE     DATE
TRANSACTION_TS       DATETIME2(6)
AMOUNT               DECIMAL(15,2)
RUNNING_BALANCE      DECIMAL(15,2)
MERCHANT_NAME        VARCHAR(100)
MERCHANT_CATEGORY    VARCHAR(60)
CHANNEL_CODE         VARCHAR(10)               -- ATM, POS, WEB, MOB, ACH, WIRE (INTL referenced by BTEQ 03)
REFERENCE_NUM        VARCHAR(40)
STATUS_CODE          CHAR(1)                   -- P=Posted, R=Reversed, H=Hold
CREATED_TS           DATETIME2(6)
```
Statistics: `ACCOUNT_ID`, `TRANSACTION_DATE`, `TRANSACTION_TYPE_CD`, `STATUS_CODE`.

### 3.6 `TXN_PROCESSING_DB.TRANSACTION_TYPES` — DISTRIBUTION = REPLICATE, CLUSTERED INDEX (TRANSACTION_TYPE_CD)
(Small lookup table; Teradata `UNIQUE PRIMARY INDEX`.)
```
TRANSACTION_TYPE_CD  VARCHAR(10)   NOT NULL
DESCRIPTION          VARCHAR(60)
CATEGORY             VARCHAR(30)               -- DEBIT, CREDIT, FEE, INTEREST
IS_REVENUE           CHAR(1)                   DEFAULT 'N'
EFFECTIVE_DATE       DATE
EXPIRATION_DATE      DATE
```
Statistics: `TRANSACTION_TYPE_CD`, `CATEGORY`.

### 3.7 `ETL_STAGING_DB.STG_CUSTOMER_360` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
Written by T4 (`usp_stg_customer_360`); read by T7, T8 (risk), T9 (golden record).
```
CUSTOMER_ID             BIGINT         NOT NULL
FIRST_NAME              VARCHAR(60)
LAST_NAME               VARCHAR(60)
DATE_OF_BIRTH           DATE
AGE                     SMALLINT
CUSTOMER_SINCE          DATE
TENURE_MONTHS           INT
CUSTOMER_STATUS         CHAR(1)
SEGMENT_CODE            VARCHAR(10)
BRANCH_ID               INT
PRIMARY_ADDRESS         VARCHAR(200)
CITY                    VARCHAR(60)
STATE_CODE              CHAR(2)
ZIP_CODE                VARCHAR(10)
NUM_ACCOUNTS            SMALLINT
NUM_ACTIVE_ACCOUNTS     SMALLINT
HAS_CHECKING            CHAR(1)                       DEFAULT 'N'
HAS_SAVINGS             CHAR(1)                       DEFAULT 'N'
HAS_CREDIT              CHAR(1)                       DEFAULT 'N'
HAS_LOAN                CHAR(1)                       DEFAULT 'N'
TOTAL_BALANCE           DECIMAL(18,2)
TOTAL_CREDIT_LIMIT      DECIMAL(18,2)
CREDIT_UTILIZATION_PCT  DECIMAL(5,2)
LOAD_TS                 DATETIME2(6)
```
Statistics: `CUSTOMER_ID`, `STATE_CODE`, `CUSTOMER_STATUS`, `SEGMENT_CODE` (union of `ddl/01_staging_tables.sql` and `bteq/01_stg_customer_360.bteq`).

### 3.8 `ETL_STAGING_DB.STG_TXN_SUMMARY` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
Written by T5 (`usp_stg_txn_summary`); read by T8 (txn analytics). Grain: one row per (CUSTOMER_ID, ACCOUNT_ID).
No transaction-date column (only the period bounds).
```
CUSTOMER_ID             BIGINT         NOT NULL
ACCOUNT_ID              BIGINT         NOT NULL
ACCOUNT_TYPE            VARCHAR(20)
SUMMARY_PERIOD_START    DATE
SUMMARY_PERIOD_END      DATE
TXN_COUNT_TOTAL         INT
TXN_COUNT_DEBIT         INT
TXN_COUNT_CREDIT        INT
TXN_COUNT_FEE           INT
AMT_TOTAL_DEBIT         DECIMAL(18,2)
AMT_TOTAL_CREDIT        DECIMAL(18,2)
AMT_TOTAL_FEES          DECIMAL(18,2)
AMT_AVG_DEBIT           DECIMAL(15,2)
AMT_AVG_CREDIT          DECIMAL(15,2)
AMT_MAX_SINGLE_DEBIT    DECIMAL(15,2)
AMT_MAX_SINGLE_CREDIT   DECIMAL(15,2)
DISTINCT_MERCHANTS      INT
TOP_MERCHANT_CATEGORY   VARCHAR(60)
PCT_ATM                 DECIMAL(5,2)
PCT_POS                 DECIMAL(5,2)
PCT_WEB                 DECIMAL(5,2)
PCT_MOBILE              DECIMAL(5,2)
DAYS_SINCE_LAST_TXN     INT
LOAD_TS                 DATETIME2(6)
```
Statistics: `CUSTOMER_ID`, `(CUSTOMER_ID, ACCOUNT_ID)`, `ACCOUNT_TYPE`.

### 3.9 `ETL_STAGING_DB.STG_RISK_FACTORS` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
Written by T6 (`usp_stg_risk_factors`); read by T8 (risk scoring).
```
CUSTOMER_ID             BIGINT         NOT NULL
ACCOUNT_OVERDRAFT_CNT   INT
NSF_FEE_TOTAL           DECIMAL(15,2)
LARGE_WITHDRAWAL_CNT    INT
LARGE_WITHDRAWAL_AMT    DECIMAL(18,2)
AVG_DAILY_BALANCE_30D   DECIMAL(15,2)
AVG_DAILY_BALANCE_90D   DECIMAL(15,2)
BALANCE_VOLATILITY      DECIMAL(10,4)
CREDIT_UTIL_RATIO       DECIMAL(5,4)
PAYMENT_ONTIME_PCT      DECIMAL(5,2)
PAYMENT_LATE_CNT        INT
MONTHS_SINCE_LAST_LATE  INT
EXTERNAL_CREDIT_SCORE   INT
DEBIT_VELOCITY_7D       DECIMAL(15,2)
DEBIT_VELOCITY_30D      DECIMAL(15,2)
NEW_MERCHANT_CNT_30D    INT
INTERNATIONAL_TXN_CNT   INT
HIGH_RISK_MERCHANT_CNT  INT
LOAD_TS                 DATETIME2(6)
```
Statistics: `CUSTOMER_ID`.

> The BTEQ work tables `WRK_DAILY_BALANCE` / `WRK_PAYMENT_HISTORY` and the volatile table
> `VT_RUN_PARAMS` are **not** part of this contract: they become `#temp` tables / local
> variables private to the T6 / T5 procedures.

### 3.10 `ETL_STAGING_DB.ETL_RUN_LOG` ★ new — DISTRIBUTION = ROUND_ROBIN, HEAP
Used by every BTEQ script but never defined in `ddl/`. Column shape is owned by
**C2 ([audit logging contract](02_audit_logging_contract.md))**; T2 creates it with exactly the
columns listed in C2 §2. Rows are written **only** via `ETL_STAGING_DB.usp_log_step` (SQL) or
`common.audit.log_step` (Spark).

### 3.11 `DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
Written by T7. Read by T9.
```
CUSTOMER_ID             BIGINT         NOT NULL
SEGMENT_NAME            VARCHAR(40)
SEGMENT_ID              SMALLINT
SUBSEGMENT_ID           SMALLINT
LIFETIME_VALUE_SCORE    DECIMAL(10,2)
ENGAGEMENT_SCORE        DECIMAL(5,2)
DIGITAL_ADOPTION_SCORE  DECIMAL(5,2)
PRODUCT_BREADTH_INDEX   DECIMAL(5,2)
TENURE_GROUP            VARCHAR(20)
AGE_GROUP               VARCHAR(20)
BALANCE_TIER            VARCHAR(20)
CHANNEL_PREFERENCE      VARCHAR(10)
CROSS_SELL_FLAG         CHAR(1)                       DEFAULT 'N'
UPSELL_FLAG             CHAR(1)                       DEFAULT 'N'
RETENTION_RISK_FLAG     CHAR(1)                       DEFAULT 'N'
MODEL_VERSION           VARCHAR(20)
EFFECTIVE_DATE          DATE
LOAD_TS                 DATETIME2(6)
```
Statistics: `CUSTOMER_ID`, `SEGMENT_NAME`.

### 3.12 `DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
Written by T8 (`txn_analytics.py`). Read by T9. Teradata `PARTITION BY COLUMN(REPORTING_PERIOD)` is dropped.
```
CUSTOMER_ID             BIGINT         NOT NULL
REPORTING_PERIOD        VARCHAR(7)                    -- 'YYYY-MM'
TOTAL_ACCOUNTS          SMALLINT
ACTIVE_ACCOUNTS         SMALLINT
TOTAL_TRANSACTIONS      INT
TOTAL_DEBIT_AMT         DECIMAL(18,2)
TOTAL_CREDIT_AMT        DECIMAL(18,2)
NET_CASH_FLOW           DECIMAL(18,2)
AVG_TRANSACTION_SIZE    DECIMAL(15,2)
MONTHLY_SPEND_TREND     VARCHAR(10)                   -- UP, DOWN, STABLE
SPEND_PERCENTILE        DECIMAL(5,2)
TOP_SPEND_CATEGORY      VARCHAR(60)
DIGITAL_TXN_PCT         DECIMAL(5,2)
FEE_INCOME              DECIMAL(15,2)
INTEREST_INCOME         DECIMAL(15,2)
REVENUE_CONTRIBUTION    DECIMAL(15,2)
ANOMALY_FLAG            CHAR(1)                       DEFAULT 'N'
MODEL_VERSION           VARCHAR(20)
EFFECTIVE_DATE          DATE
LOAD_TS                 DATETIME2(6)
```
Statistics: `CUSTOMER_ID`, `REPORTING_PERIOD`.
Note: SAS computes a `TOTAL_FEES` column that is **not** in the target table; it is dropped on write-back (C5 `conform_to_contract`).

### 3.13 `DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
Written by T8 (`risk_scoring.py`). Read by T9.
```
CUSTOMER_ID                 BIGINT         NOT NULL
COMPOSITE_RISK_SCORE        DECIMAL(6,2)
RISK_TIER                   VARCHAR(20)               -- LOW, MODERATE, ELEVATED, HIGH, CRITICAL
PROBABILITY_OF_DEFAULT      DECIMAL(7,6)
CREDIT_RISK_COMPONENT       DECIMAL(5,2)
BEHAVIOUR_RISK_COMPONENT    DECIMAL(5,2)
VELOCITY_RISK_COMPONENT     DECIMAL(5,2)
BUREAU_SCORE_COMPONENT      DECIMAL(5,2)
PAYMENT_HISTORY_COMPONENT   DECIMAL(5,2)
PRIMARY_RISK_DRIVER         VARCHAR(40)
SECONDARY_RISK_DRIVER       VARCHAR(40)
SCORE_DELTA_30D             DECIMAL(6,2)
WATCH_LIST_FLAG             CHAR(1)                   DEFAULT 'N'
REVIEW_REQUIRED_FLAG        CHAR(1)                   DEFAULT 'N'
MODEL_VERSION               VARCHAR(20)
EFFECTIVE_DATE              DATE
LOAD_TS                     DATETIME2(6)
```
Statistics: `CUSTOMER_ID`, `RISK_TIER`.

### 3.14 `DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE` — DISTRIBUTION = HASH(CUSTOMER_ID), CCI
Written by T9 (`data_products.py`). Read by downstream consumers and T10 post-run checks.
```
CUSTOMER_ID                 BIGINT         NOT NULL
FULL_NAME                   VARCHAR(120)
AGE                         SMALLINT
STATE_CODE                  CHAR(2)
CUSTOMER_SINCE              DATE
TENURE_MONTHS               INT
CUSTOMER_STATUS             CHAR(1)
SEGMENT_NAME                VARCHAR(40)
LIFETIME_VALUE_SCORE        DECIMAL(10,2)
ENGAGEMENT_SCORE            DECIMAL(5,2)
TOTAL_ACCOUNTS              SMALLINT
ACTIVE_ACCOUNTS             SMALLINT
TOTAL_BALANCE               DECIMAL(18,2)
TOTAL_CREDIT_LIMIT          DECIMAL(18,2)
CREDIT_UTILIZATION_PCT      DECIMAL(5,2)
MONTHLY_TRANSACTIONS        INT
MONTHLY_SPEND               DECIMAL(18,2)
NET_CASH_FLOW               DECIMAL(18,2)
TOP_SPEND_CATEGORY          VARCHAR(60)
DIGITAL_TXN_PCT             DECIMAL(5,2)
COMPOSITE_RISK_SCORE        DECIMAL(6,2)
RISK_TIER                   VARCHAR(20)
PROBABILITY_OF_DEFAULT      DECIMAL(7,6)
WATCH_LIST_FLAG             CHAR(1)                   DEFAULT 'N'
CROSS_SELL_FLAG             CHAR(1)                   DEFAULT 'N'
UPSELL_FLAG                 CHAR(1)                   DEFAULT 'N'
RETENTION_RISK_FLAG         CHAR(1)                   DEFAULT 'N'
MODEL_VERSION               VARCHAR(20)
EFFECTIVE_DATE              DATE
LOAD_TS                     DATETIME2(6)
```
Statistics: `CUSTOMER_ID`.

## 4. Producer / consumer matrix

| Table | Producer | Consumers |
|---|---|---|
| `CORE_BANKING_DB.*`, `TXN_PROCESSING_DB.*` | T3 (`usp_load_source_tables`) | T4, T5, T6 |
| `ETL_STAGING_DB.STG_CUSTOMER_360` | T4 | T7, T8 (`risk_scoring`), T9 (`data_products`) |
| `ETL_STAGING_DB.STG_TXN_SUMMARY` | T5 | T8 (`txn_analytics`) |
| `ETL_STAGING_DB.STG_RISK_FACTORS` | T6 | T8 (`risk_scoring`) |
| `ETL_STAGING_DB.ETL_RUN_LOG` | everyone, via C2 helpers | T10 (monitoring), T9 (`data_products` audit print) |
| `DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS` | T7 | T9 |
| `DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS` | T8 | T9 |
| `DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES` | T8 | T9 |
| `DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE` | T9 | T10 post-run validation, downstream |

## 5. Rules for consumers

- Never `DROP`/`CREATE`/`ALTER` a contract table outside `synapse/sql/ddl/`. Loaders `TRUNCATE` (or scoped `DELETE`) and `INSERT`; they never rebuild the table (that would lose the distribution/CCI/statistics defined here).
- Spark jobs must emit exactly the columns above, in order, with compatible types (use `common.writeback.conform_to_contract`, C5).
- The machine-readable form of this contract (column list + types per table) is implemented by T9 as `common.schema` (signature in [`stubs/schema.pyi`](stubs/schema.pyi)), generated from this document; T2's DDL and `common.schema` must agree.
