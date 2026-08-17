-- =============================================================================
-- Retail Point-of-Sale (POS) Table DDL
-- =============================================================================
-- SEPARATE DOMAIN NOTICE
-- ---------------------
-- This schema is a NET-NEW, standalone retail point-of-sale data model that
-- backs the POS checkout webapp (hardware / home-improvement store lanes).
-- It is a SEPARATE DOMAIN from the retail banking analytics pipeline in this
-- repository: it does NOT read from, write to, join with, or otherwise
-- integrate with CORE_BANKING_DB, TXN_PROCESSING_DB, ETL_STAGING_DB or
-- DATA_PRODUCTS_DB. The word "TRANSACTION" here means a sales/checkout
-- transaction at a register, NOT a banking financial transaction.
--
-- Target platform: Teradata (same dialect/conventions as the rest of ddl/).
-- Database:        POS_DB
--
-- Referential integrity is declared with FOREIGN KEY ... REFERENCES WITH NO
-- CHECK OPTION (soft RI), which is the standard Teradata pattern: the
-- relationships are documented and available to the optimizer without
-- imposing per-row enforcement cost on the checkout write path.
--
-- Table load order (parents first):
--   STORE -> REGISTER -> EMPLOYEE -> PRODUCT -> CUSTOMER_ACCOUNT
--         -> QUOTE -> QUOTE_LINE
--         -> POS_TRANSACTION -> POS_TRANSACTION_LINE -> POS_TENDER
-- =============================================================================

-- -----------------------------------------------------------------------------
-- POS_DB.STORE
-- Physical store locations. STORE_ID is the number printed on the receipt and
-- shown in the lane header (e.g. 6631).
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.STORE, NO FALLBACK
(
    STORE_ID            INTEGER         NOT NULL,
    STORE_NAME          VARCHAR(80)     CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    ADDRESS_LINE_1      VARCHAR(100)    CHARACTER SET LATIN NOT CASESPECIFIC,
    CITY                VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    STATE_CODE          CHAR(2)         CHARACTER SET LATIN NOT CASESPECIFIC,
    ZIP_CODE            VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    PHONE               VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    TIME_ZONE           VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'America/Los_Angeles',
    DEFAULT_TAX_RATE    DECIMAL(7,5),                                          -- store's prevailing sales tax rate, e.g. 0.09250
    STORE_STATUS        CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'O',  -- O=Open, C=Closed, T=Temporarily closed
    OPENED_DATE         DATE            FORMAT 'YYYY-MM-DD',
    CREATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (STORE_ID);

ALTER TABLE POS_DB.STORE
    ADD CONSTRAINT PK_STORE PRIMARY KEY (STORE_ID);

-- -----------------------------------------------------------------------------
-- POS_DB.REGISTER
-- Checkout terminals / lanes within a store. REGISTER_ID is unique only within
-- a store, so the PK is composite (STORE_ID, REGISTER_ID) -- e.g. 6631 / 12.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.REGISTER, NO FALLBACK
(
    STORE_ID            INTEGER         NOT NULL,
    REGISTER_ID         SMALLINT        NOT NULL,
    REGISTER_AREA       VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- 'front end', 'pro desk', 'garden', 'lumber yard', 'service desk'
    REGISTER_TYPE       VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'STAFFED',  -- STAFFED, SELF_CHECKOUT, MOBILE, RETURNS
    TERMINAL_SERIAL_NO  VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    REGISTER_STATUS     CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'A',  -- A=Active, I=Inactive, M=Maintenance
    CREATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (STORE_ID, REGISTER_ID);

ALTER TABLE POS_DB.REGISTER
    ADD CONSTRAINT PK_REGISTER PRIMARY KEY (STORE_ID, REGISTER_ID);

ALTER TABLE POS_DB.REGISTER
    ADD CONSTRAINT FK_REGISTER_STORE FOREIGN KEY (STORE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.STORE (STORE_ID);

-- -----------------------------------------------------------------------------
-- POS_DB.EMPLOYEE
-- Cashiers and other associates who can operate a register. DISPLAY_NAME is the
-- abbreviated name shown in the lane header (e.g. 'M. Reyes').
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.EMPLOYEE, NO FALLBACK
(
    EMPLOYEE_ID         BIGINT          NOT NULL,
    HOME_STORE_ID       INTEGER         NOT NULL,
    FIRST_NAME          VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    LAST_NAME           VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    DISPLAY_NAME        VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- receipt/lane display, e.g. 'M. Reyes'
    ROLE_CODE           VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'CASHIER',  -- CASHIER, HEAD_CASHIER, SUPERVISOR, PRO_DESK
    EMPLOYEE_STATUS     CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'A',  -- A=Active, L=Leave, T=Terminated
    HIRE_DATE           DATE            FORMAT 'YYYY-MM-DD',
    CREATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (EMPLOYEE_ID);

ALTER TABLE POS_DB.EMPLOYEE
    ADD CONSTRAINT PK_EMPLOYEE PRIMARY KEY (EMPLOYEE_ID);

ALTER TABLE POS_DB.EMPLOYEE
    ADD CONSTRAINT FK_EMPLOYEE_STORE FOREIGN KEY (HOME_STORE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.STORE (STORE_ID);

-- -----------------------------------------------------------------------------
-- POS_DB.PRODUCT
-- SKU catalog. Covers scannable merchandise AND non-scan service SKUs
-- (lumber cut charge, paint mix, tool rental, delivery, will call, special
-- order) and warranty/protection-plan SKUs, so that every sellable line on a
-- receipt resolves to a catalog row.
-- Location (AISLE / BAY / DEPARTMENT) backs the "aisle 12, bay 004" style hints
-- rendered next to each line in the UI.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.PRODUCT, NO FALLBACK
(
    SKU                 VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    PRODUCT_DESC        VARCHAR(120)    CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    LONG_DESC           VARCHAR(400)    CHARACTER SET LATIN NOT CASESPECIFIC,
    UNIT_OF_MEASURE     VARCHAR(6)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,  -- EA, GAL, BOX, BAG, LF, SQFT, HR
    UNIT_PRICE          DECIMAL(11,2)   NOT NULL,
    PRODUCT_TYPE        VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'MERCHANDISE',  -- MERCHANDISE, SERVICE, WARRANTY, FEE
    IS_SCANNABLE        CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'Y',  -- N for service / non-scan lines keyed by the cashier
    DEPARTMENT          VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- 'Paint desk', 'Lumber yard', 'Saw station', 'Hardware'
    AISLE               VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    BAY                 VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    TAXABLE_FLAG        CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'Y',
    QUANTITY_PRECISION  SMALLINT        DEFAULT 0,                             -- 0 for EA/BOX, 2 for GAL/LF cut-to-length
    VENDOR_NAME         VARCHAR(80)     CHARACTER SET LATIN NOT CASESPECIFIC,
    PRODUCT_STATUS      CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'A',  -- A=Active, D=Discontinued
    CREATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (SKU);

ALTER TABLE POS_DB.PRODUCT
    ADD CONSTRAINT PK_PRODUCT PRIMARY KEY (SKU);

-- -----------------------------------------------------------------------------
-- POS_DB.PRODUCT_STORE_PRICE
-- Optional store-level price / location override. Absent a row here, the
-- catalog UNIT_PRICE and AISLE/BAY apply.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.PRODUCT_STORE_PRICE, NO FALLBACK
(
    STORE_ID            INTEGER         NOT NULL,
    SKU                 VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    UNIT_PRICE          DECIMAL(11,2)   NOT NULL,
    AISLE               VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    BAY                 VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    EFFECTIVE_DATE      DATE            FORMAT 'YYYY-MM-DD' NOT NULL,
    EXPIRATION_DATE     DATE            FORMAT 'YYYY-MM-DD' DEFAULT DATE '9999-12-31',
    CREATED_TS          TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (STORE_ID, SKU);

ALTER TABLE POS_DB.PRODUCT_STORE_PRICE
    ADD CONSTRAINT PK_PRODUCT_STORE_PRICE PRIMARY KEY (STORE_ID, SKU, EFFECTIVE_DATE);

ALTER TABLE POS_DB.PRODUCT_STORE_PRICE
    ADD CONSTRAINT FK_PSP_STORE FOREIGN KEY (STORE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.STORE (STORE_ID);

ALTER TABLE POS_DB.PRODUCT_STORE_PRICE
    ADD CONSTRAINT FK_PSP_PRODUCT FOREIGN KEY (SKU)
    REFERENCES WITH NO CHECK OPTION POS_DB.PRODUCT (SKU);

-- -----------------------------------------------------------------------------
-- POS_DB.CUSTOMER_ACCOUNT
-- Customer of record on a sale. Supports both walk-in consumers and
-- commercial / B2B job accounts (company name, Pro loyalty program, tax
-- exemption certificate, house charge account).
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.CUSTOMER_ACCOUNT, NO FALLBACK
(
    CUSTOMER_ACCOUNT_ID     BIGINT          NOT NULL,
    ACCOUNT_NUMBER          VARCHAR(24)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- customer-facing account no. shown in the lane header
    ACCOUNT_TYPE            VARCHAR(12)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'CONSUMER',  -- CONSUMER, COMMERCIAL
    COMPANY_NAME            VARCHAR(120)    CHARACTER SET LATIN NOT CASESPECIFIC,  -- e.g. 'Bay Area Renovations LLC'
    CONTACT_FIRST_NAME      VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    CONTACT_LAST_NAME       VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    PHONE                   VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    EMAIL                   VARCHAR(120)    CHARACTER SET LATIN NOT CASESPECIFIC,
    ADDRESS_LINE_1          VARCHAR(100)    CHARACTER SET LATIN NOT CASESPECIFIC,
    CITY                    VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,
    STATE_CODE              CHAR(2)         CHARACTER SET LATIN NOT CASESPECIFIC,
    ZIP_CODE                VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    LOYALTY_PROGRAM         VARCHAR(30)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- e.g. 'PRO_XTRA'
    LOYALTY_MEMBER_ID       VARCHAR(24)     CHARACTER SET LATIN NOT CASESPECIFIC,
    LOYALTY_TIER            VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- e.g. 'PERKS', 'GOLD'
    TAX_EXEMPT_FLAG         CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'N',
    TAX_EXEMPT_CERT_NO      VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    TAX_EXEMPT_EXPIRY       DATE            FORMAT 'YYYY-MM-DD',
    CHARGE_ACCOUNT_NO       VARCHAR(24)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- commercial revolving charge account
    CREDIT_LIMIT            DECIMAL(13,2),
    HOME_STORE_ID           INTEGER,
    ACCOUNT_STATUS          CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'A',  -- A=Active, S=Suspended, C=Closed
    CREATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (CUSTOMER_ACCOUNT_ID);

ALTER TABLE POS_DB.CUSTOMER_ACCOUNT
    ADD CONSTRAINT PK_CUSTOMER_ACCOUNT PRIMARY KEY (CUSTOMER_ACCOUNT_ID);

ALTER TABLE POS_DB.CUSTOMER_ACCOUNT
    ADD CONSTRAINT FK_CUSTACCT_STORE FOREIGN KEY (HOME_STORE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.STORE (STORE_ID);

CREATE UNIQUE INDEX UX_CUSTOMER_ACCOUNT_NUMBER (ACCOUNT_NUMBER) ON POS_DB.CUSTOMER_ACCOUNT;

-- -----------------------------------------------------------------------------
-- POS_DB.JOB_REFERENCE
-- Purchase-order / job references belonging to a commercial account. A single
-- Pro account typically runs several concurrent jobs; the cashier picks one at
-- checkout and it is stamped onto the transaction header.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.JOB_REFERENCE, NO FALLBACK
(
    JOB_REFERENCE_ID        BIGINT          NOT NULL,
    CUSTOMER_ACCOUNT_ID     BIGINT          NOT NULL,
    PO_NUMBER               VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- customer PO shown on the receipt
    JOB_NAME                VARCHAR(120)    CHARACTER SET LATIN NOT CASESPECIFIC,  -- e.g. 'Elm St remodel'
    JOB_SITE_ADDRESS        VARCHAR(160)    CHARACTER SET LATIN NOT CASESPECIFIC,
    JOB_STATUS              CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'O',  -- O=Open, C=Closed
    CREATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (JOB_REFERENCE_ID);

ALTER TABLE POS_DB.JOB_REFERENCE
    ADD CONSTRAINT PK_JOB_REFERENCE PRIMARY KEY (JOB_REFERENCE_ID);

ALTER TABLE POS_DB.JOB_REFERENCE
    ADD CONSTRAINT FK_JOBREF_CUSTACCT FOREIGN KEY (CUSTOMER_ACCOUNT_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.CUSTOMER_ACCOUNT (CUSTOMER_ACCOUNT_ID);

-- -----------------------------------------------------------------------------
-- POS_DB.QUOTE
-- Pro-desk quote header. A quote is priced and saved ahead of time and later
-- pulled into a lane by the "Convert quote" action, which copies QUOTE_LINE
-- rows into POS_TRANSACTION_LINE and stamps CONVERTED_* below.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.QUOTE, NO FALLBACK
(
    QUOTE_ID                BIGINT          NOT NULL,
    QUOTE_NUMBER            VARCHAR(24)     CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,  -- customer-facing quote no.
    STORE_ID                INTEGER         NOT NULL,
    CUSTOMER_ACCOUNT_ID     BIGINT,
    JOB_REFERENCE_ID        BIGINT,
    CREATED_BY_EMPLOYEE_ID  BIGINT,
    QUOTE_TS                TIMESTAMP(6)    NOT NULL,
    EXPIRATION_DATE         DATE            FORMAT 'YYYY-MM-DD',
    SUBTOTAL_AMT            DECIMAL(13,2)   DEFAULT 0.00,
    DISCOUNT_TOTAL_AMT      DECIMAL(13,2)   DEFAULT 0.00,
    TAX_RATE                DECIMAL(7,5)    DEFAULT 0.00000,
    TAX_AMT                 DECIMAL(13,2)   DEFAULT 0.00,
    TOTAL_AMT               DECIMAL(13,2)   DEFAULT 0.00,
    QUOTE_STATUS            VARCHAR(12)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'OPEN',  -- OPEN, CONVERTED, EXPIRED, CANCELLED
    CONVERTED_STORE_ID      INTEGER,                                           -- ) identify the transaction the quote
    CONVERTED_REGISTER_ID   SMALLINT,                                          -- ) became, once "Convert quote" runs
    CONVERTED_TXN_NUMBER    INTEGER,                                           -- )
    CONVERTED_TS            TIMESTAMP(6),
    NOTES                   VARCHAR(400)    CHARACTER SET LATIN NOT CASESPECIFIC,
    CREATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (QUOTE_ID);

ALTER TABLE POS_DB.QUOTE
    ADD CONSTRAINT PK_QUOTE PRIMARY KEY (QUOTE_ID);

ALTER TABLE POS_DB.QUOTE
    ADD CONSTRAINT FK_QUOTE_STORE FOREIGN KEY (STORE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.STORE (STORE_ID);

ALTER TABLE POS_DB.QUOTE
    ADD CONSTRAINT FK_QUOTE_CUSTACCT FOREIGN KEY (CUSTOMER_ACCOUNT_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.CUSTOMER_ACCOUNT (CUSTOMER_ACCOUNT_ID);

ALTER TABLE POS_DB.QUOTE
    ADD CONSTRAINT FK_QUOTE_JOBREF FOREIGN KEY (JOB_REFERENCE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.JOB_REFERENCE (JOB_REFERENCE_ID);

ALTER TABLE POS_DB.QUOTE
    ADD CONSTRAINT FK_QUOTE_EMPLOYEE FOREIGN KEY (CREATED_BY_EMPLOYEE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.EMPLOYEE (EMPLOYEE_ID);

CREATE UNIQUE INDEX UX_QUOTE_NUMBER (QUOTE_NUMBER) ON POS_DB.QUOTE;

-- -----------------------------------------------------------------------------
-- POS_DB.QUOTE_LINE
-- Quote detail. Mirrors POS_TRANSACTION_LINE column-for-column so conversion is
-- a straight INSERT ... SELECT.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.QUOTE_LINE, NO FALLBACK
(
    QUOTE_ID                BIGINT          NOT NULL,
    LINE_NUMBER             SMALLINT        NOT NULL,
    SKU                     VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    LINE_TYPE               VARCHAR(16)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'MERCHANDISE',  -- MERCHANDISE, SERVICE, WARRANTY, DISCOUNT, FEE
    DESCRIPTION_SNAPSHOT    VARCHAR(120)    CHARACTER SET LATIN NOT CASESPECIFIC,
    QUANTITY                DECIMAL(11,3)   DEFAULT 1.000,
    UNIT_OF_MEASURE         VARCHAR(6)      CHARACTER SET LATIN NOT CASESPECIFIC,
    UNIT_PRICE              DECIMAL(11,2),
    EXTENDED_AMT            DECIMAL(13,2),
    TAXABLE_FLAG            CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'Y',
    NOTES                   VARCHAR(200)    CHARACTER SET LATIN NOT CASESPECIFIC,
    CREATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (QUOTE_ID);

ALTER TABLE POS_DB.QUOTE_LINE
    ADD CONSTRAINT PK_QUOTE_LINE PRIMARY KEY (QUOTE_ID, LINE_NUMBER);

ALTER TABLE POS_DB.QUOTE_LINE
    ADD CONSTRAINT FK_QUOTELINE_QUOTE FOREIGN KEY (QUOTE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.QUOTE (QUOTE_ID);

ALTER TABLE POS_DB.QUOTE_LINE
    ADD CONSTRAINT FK_QUOTELINE_PRODUCT FOREIGN KEY (SKU)
    REFERENCES WITH NO CHECK OPTION POS_DB.PRODUCT (SKU);

-- -----------------------------------------------------------------------------
-- POS_DB.POS_TRANSACTION
-- Sale header -- one row per checkout. The business key is the receipt triple
-- STORE_ID / REGISTER_ID / TRANSACTION_NUMBER (e.g. 6631/12/84291), which is
-- what the lane header and printed receipt display; TRANSACTION_ID is an
-- opaque surrogate for child tables to reference.
-- Monetary invariant:
--   TOTAL_AMT = SUBTOTAL_AMT - DISCOUNT_TOTAL_AMT + TAX_AMT
-- where SUBTOTAL_AMT is the sum of non-DISCOUNT line EXTENDED_AMT and
-- DISCOUNT_TOTAL_AMT is the absolute sum of DISCOUNT lines.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.POS_TRANSACTION, NO FALLBACK
(
    TRANSACTION_ID          BIGINT          NOT NULL,
    STORE_ID                INTEGER         NOT NULL,
    REGISTER_ID             SMALLINT        NOT NULL,
    TRANSACTION_NUMBER      INTEGER         NOT NULL,                          -- sequential per store/register/business date
    BUSINESS_DATE           DATE            FORMAT 'YYYY-MM-DD' NOT NULL,
    TRANSACTION_TS          TIMESTAMP(6)    NOT NULL,
    CASHIER_EMPLOYEE_ID     BIGINT,
    CUSTOMER_ACCOUNT_ID     BIGINT,                                            -- NULL for anonymous walk-in sales
    JOB_REFERENCE_ID        BIGINT,
    PO_NUMBER               VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- PO typed at the lane; overrides JOB_REFERENCE.PO_NUMBER
    SOURCE_QUOTE_ID         BIGINT,                                            -- populated by the "Convert quote" flow
    TRANSACTION_TYPE        VARCHAR(12)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'SALE',  -- SALE, RETURN, EXCHANGE, VOID
    SUBTOTAL_AMT            DECIMAL(13,2)   DEFAULT 0.00,
    DISCOUNT_TOTAL_AMT      DECIMAL(13,2)   DEFAULT 0.00,                      -- stored positive
    TAXABLE_AMT             DECIMAL(13,2)   DEFAULT 0.00,
    TAX_RATE                DECIMAL(7,5)    DEFAULT 0.00000,
    TAX_AMT                 DECIMAL(13,2)   DEFAULT 0.00,
    TAX_EXEMPT_FLAG         CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'N',
    TAX_EXEMPT_CERT_NO      VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    TOTAL_AMT               DECIMAL(13,2)   DEFAULT 0.00,
    TENDERED_AMT            DECIMAL(13,2)   DEFAULT 0.00,                      -- sum of POS_TENDER rows
    BALANCE_DUE_AMT         DECIMAL(13,2)   DEFAULT 0.00,                      -- TOTAL_AMT - TENDERED_AMT; > 0 while split tender in progress
    CHANGE_DUE_AMT          DECIMAL(13,2)   DEFAULT 0.00,
    LINE_COUNT              SMALLINT        DEFAULT 0,
    UNIT_COUNT              DECIMAL(11,3)   DEFAULT 0.000,
    TRANSACTION_STATUS      VARCHAR(12)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'OPEN',  -- OPEN, SUSPENDED, TENDERING, COMPLETED, VOIDED
    SUSPENDED_TS            TIMESTAMP(6),
    COMPLETED_TS            TIMESTAMP(6),
    CREATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (TRANSACTION_ID);

ALTER TABLE POS_DB.POS_TRANSACTION
    ADD CONSTRAINT PK_POS_TRANSACTION PRIMARY KEY (TRANSACTION_ID);

ALTER TABLE POS_DB.POS_TRANSACTION
    ADD CONSTRAINT FK_TXN_REGISTER FOREIGN KEY (STORE_ID, REGISTER_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.REGISTER (STORE_ID, REGISTER_ID);

ALTER TABLE POS_DB.POS_TRANSACTION
    ADD CONSTRAINT FK_TXN_EMPLOYEE FOREIGN KEY (CASHIER_EMPLOYEE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.EMPLOYEE (EMPLOYEE_ID);

ALTER TABLE POS_DB.POS_TRANSACTION
    ADD CONSTRAINT FK_TXN_CUSTACCT FOREIGN KEY (CUSTOMER_ACCOUNT_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.CUSTOMER_ACCOUNT (CUSTOMER_ACCOUNT_ID);

ALTER TABLE POS_DB.POS_TRANSACTION
    ADD CONSTRAINT FK_TXN_JOBREF FOREIGN KEY (JOB_REFERENCE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.JOB_REFERENCE (JOB_REFERENCE_ID);

ALTER TABLE POS_DB.POS_TRANSACTION
    ADD CONSTRAINT FK_TXN_QUOTE FOREIGN KEY (SOURCE_QUOTE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.QUOTE (QUOTE_ID);

-- Receipt business key: unique per store/register/business date.
CREATE UNIQUE INDEX UX_POS_TRANSACTION_BK
    (STORE_ID, REGISTER_ID, BUSINESS_DATE, TRANSACTION_NUMBER)
    ON POS_DB.POS_TRANSACTION;

-- -----------------------------------------------------------------------------
-- POS_DB.POS_TRANSACTION_LINE
-- Sale detail -- one row per printed receipt line, in LINE_NUMBER order.
--
-- LINE_TYPE drives how the line behaves:
--   MERCHANDISE - scanned SKU (2x4 stud, paint, screws ...)
--   SERVICE     - non-scan service SKU: lumber cut charge, paint mix, tool
--                 rental, special order, will call, delivery
--   WARRANTY    - protection plan attached to a merchandise line via
--                 RELATED_LINE_NUMBER (e.g. '2-year protection plan')
--   DISCOUNT    - negative-amount line such as volume/Pro pricing; carries
--                 DISCOUNT_REASON and may point at the line it discounts
--   FEE         - non-merchandise charges (core charge, environmental fee)
--
-- DESCRIPTION_SNAPSHOT, UNIT_OF_MEASURE and UNIT_PRICE are snapshots taken at
-- ring time so the receipt is reproducible after catalog changes. SKU is
-- nullable for ad-hoc keyed lines.
-- Sign convention: DISCOUNT lines store negative UNIT_PRICE/EXTENDED_AMT.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.POS_TRANSACTION_LINE, NO FALLBACK
(
    TRANSACTION_ID          BIGINT          NOT NULL,
    LINE_NUMBER             SMALLINT        NOT NULL,
    LINE_TYPE               VARCHAR(16)     CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL DEFAULT 'MERCHANDISE',
    SKU                     VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    DESCRIPTION_SNAPSHOT    VARCHAR(120)    CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    QUANTITY                DECIMAL(11,3)   NOT NULL DEFAULT 1.000,
    UNIT_OF_MEASURE         VARCHAR(6)      CHARACTER SET LATIN NOT CASESPECIFIC,
    UNIT_PRICE              DECIMAL(11,2)   NOT NULL DEFAULT 0.00,
    EXTENDED_AMT            DECIMAL(13,2)   NOT NULL DEFAULT 0.00,             -- QUANTITY * UNIT_PRICE, negative for DISCOUNT
    AISLE_SNAPSHOT          VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    BAY_SNAPSHOT            VARCHAR(10)     CHARACTER SET LATIN NOT CASESPECIFIC,
    DEPARTMENT_SNAPSHOT     VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    RELATED_LINE_NUMBER     SMALLINT,                                          -- warranty/discount/service line -> parent merchandise line
    DISCOUNT_REASON         VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- e.g. 'VOLUME_PRICING', 'PRO_XTRA', 'MANAGER_OVERRIDE'
    WARRANTY_TERM_MONTHS    SMALLINT,                                          -- 24 for a 2-year protection plan
    SERVICE_DETAIL          VARCHAR(200)    CHARACTER SET LATIN NOT CASESPECIFIC,  -- cut list, paint formula, rental window, delivery slot
    FULFILLMENT_TYPE        VARCHAR(16)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'CARRY_OUT',  -- CARRY_OUT, WILL_CALL, DELIVERY, SPECIAL_ORDER
    TAXABLE_FLAG            CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'Y',
    TAX_AMT                 DECIMAL(13,2)   DEFAULT 0.00,
    OVERRIDE_EMPLOYEE_ID    BIGINT,                                            -- associate who authorized a price/discount override
    VOID_FLAG               CHAR(1)         CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'N',
    CREATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (TRANSACTION_ID);

ALTER TABLE POS_DB.POS_TRANSACTION_LINE
    ADD CONSTRAINT PK_POS_TRANSACTION_LINE PRIMARY KEY (TRANSACTION_ID, LINE_NUMBER);

ALTER TABLE POS_DB.POS_TRANSACTION_LINE
    ADD CONSTRAINT FK_TXNLINE_TXN FOREIGN KEY (TRANSACTION_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.POS_TRANSACTION (TRANSACTION_ID);

ALTER TABLE POS_DB.POS_TRANSACTION_LINE
    ADD CONSTRAINT FK_TXNLINE_PRODUCT FOREIGN KEY (SKU)
    REFERENCES WITH NO CHECK OPTION POS_DB.PRODUCT (SKU);

ALTER TABLE POS_DB.POS_TRANSACTION_LINE
    ADD CONSTRAINT FK_TXNLINE_OVERRIDE_EMP FOREIGN KEY (OVERRIDE_EMPLOYEE_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.EMPLOYEE (EMPLOYEE_ID);

-- -----------------------------------------------------------------------------
-- POS_DB.POS_TENDER
-- Payments applied to a transaction. Multiple rows per transaction support
-- SPLIT TENDER (e.g. part on the commercial revolving charge, remainder on a
-- debit card, change back in cash). Sum(AMOUNT_AMT) for APPROVED rows equals
-- POS_TRANSACTION.TENDERED_AMT.
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE POS_DB.POS_TENDER, NO FALLBACK
(
    TRANSACTION_ID          BIGINT          NOT NULL,
    TENDER_SEQ              SMALLINT        NOT NULL,                          -- 1..n, order applied at the lane
    TENDER_TYPE             VARCHAR(24)     CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,  -- COMMERCIAL_CHARGE, CREDIT, DEBIT, CASH, GIFT_CARD, CHECK, STORE_CREDIT
    TENDER_LABEL            VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- display text, e.g. 'Commercial revolving charge'
    AMOUNT_AMT              DECIMAL(13,2)   NOT NULL,
    CHANGE_AMT              DECIMAL(13,2)   DEFAULT 0.00,                      -- cash change returned on this tender
    CARD_BRAND              VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- VISA, MC, AMEX, PRIVATE_LABEL
    CARD_LAST4              CHAR(4)         CHARACTER SET LATIN NOT CASESPECIFIC,
    ENTRY_METHOD            VARCHAR(12)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- SWIPE, CHIP, CONTACTLESS, MANUAL, ACCOUNT_LOOKUP
    CHARGE_ACCOUNT_NO       VARCHAR(24)     CHARACTER SET LATIN NOT CASESPECIFIC,  -- for COMMERCIAL_CHARGE tenders
    AUTHORIZATION_CODE      VARCHAR(20)     CHARACTER SET LATIN NOT CASESPECIFIC,
    PROCESSOR_REFERENCE     VARCHAR(40)     CHARACTER SET LATIN NOT CASESPECIFIC,
    TENDER_STATUS           VARCHAR(12)     CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'APPROVED',  -- APPROVED, DECLINED, VOIDED, REVERSED
    TENDER_TS               TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (TRANSACTION_ID);

ALTER TABLE POS_DB.POS_TENDER
    ADD CONSTRAINT PK_POS_TENDER PRIMARY KEY (TRANSACTION_ID, TENDER_SEQ);

ALTER TABLE POS_DB.POS_TENDER
    ADD CONSTRAINT FK_TENDER_TXN FOREIGN KEY (TRANSACTION_ID)
    REFERENCES WITH NO CHECK OPTION POS_DB.POS_TRANSACTION (TRANSACTION_ID);

-- =============================================================================
-- End of POS DDL
-- =============================================================================
