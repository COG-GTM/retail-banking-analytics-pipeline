-- =============================================================================
-- Reference Table Seed - REF_HIGH_RISK_MERCHANT_CATEGORY
-- =============================================================================
-- Run once after 03_reference_tables.sql. To add a category later, INSERT a
-- new row with its EFFECTIVE_DATE; to retire one, UPDATE its EXPIRATION_DATE.
-- Never delete rows: the dates are the audit trail of what was in force when.
-- The UNIQUE PRIMARY INDEX rejects duplicate (category, effective date) pairs,
-- so re-running this script on a seeded table fails instead of double-counting.
--
-- After any change to this table, rerun (in order):
--   1. bteq/03_stg_risk_factors.bteq
--   2. sas/03_sas_risk_scoring.sas
--   3. sas/04_sas_data_products.sas
-- =============================================================================

-- Original hard-coded list (moved from bteq/03_stg_risk_factors.bteq)
INSERT INTO ETL_STAGING_DB.REF_HIGH_RISK_MERCHANT_CATEGORY
    (MERCHANT_CATEGORY, EFFECTIVE_DATE, EXPIRATION_DATE, CHANGE_REFERENCE)
VALUES ('GAMBLING',           DATE '2020-01-01', NULL, 'Initial list');

INSERT INTO ETL_STAGING_DB.REF_HIGH_RISK_MERCHANT_CATEGORY
    (MERCHANT_CATEGORY, EFFECTIVE_DATE, EXPIRATION_DATE, CHANGE_REFERENCE)
VALUES ('WIRE_TRANSFER_INTL', DATE '2020-01-01', NULL, 'Initial list');

INSERT INTO ETL_STAGING_DB.REF_HIGH_RISK_MERCHANT_CATEGORY
    (MERCHANT_CATEGORY, EFFECTIVE_DATE, EXPIRATION_DATE, CHANGE_REFERENCE)
VALUES ('CRYPTO_EXCHANGE',    DATE '2020-01-01', NULL, 'Initial list');

INSERT INTO ETL_STAGING_DB.REF_HIGH_RISK_MERCHANT_CATEGORY
    (MERCHANT_CATEGORY, EFFECTIVE_DATE, EXPIRATION_DATE, CHANGE_REFERENCE)
VALUES ('PAWN_SHOP',          DATE '2020-01-01', NULL, 'Initial list');

-- Compliance addition, effective 1 Nov 2026
INSERT INTO ETL_STAGING_DB.REF_HIGH_RISK_MERCHANT_CATEGORY
    (MERCHANT_CATEGORY, EFFECTIVE_DATE, EXPIRATION_DATE, CHANGE_REFERENCE)
VALUES ('PAYDAY_LENDER',      DATE '2026-11-01', NULL, 'Compliance: payday lenders added to high-risk list');
