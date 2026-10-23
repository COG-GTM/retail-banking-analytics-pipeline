-- =============================================================================
-- Reference Table DDL - Business Rule Lookups
-- =============================================================================
-- Small, slowly-changing lookup tables that drive business rules in the BTEQ
-- scripts. Unlike the STG_* tables they are NOT dropped each run; they are
-- created once and maintained by data changes (see 04_reference_seed.sql).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- REF_HIGH_RISK_MERCHANT_CATEGORY
-- Merchant categories that Compliance classifies as high-risk. A category is
-- in force on a run date D when EFFECTIVE_DATE <= D and
-- (EXPIRATION_DATE IS NULL OR EXPIRATION_DATE > D).
-- Maintained by:  Compliance-approved INSERT/UPDATE (04_reference_seed.sql)
-- Consumed by:    bteq/03_stg_risk_factors.bteq  (HIGH_RISK_MERCHANT_CNT)
-- -----------------------------------------------------------------------------
CREATE MULTISET TABLE ETL_STAGING_DB.REF_HIGH_RISK_MERCHANT_CATEGORY, NO FALLBACK
(
    MERCHANT_CATEGORY       VARCHAR(60)     CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    EFFECTIVE_DATE          DATE            FORMAT 'YYYY-MM-DD' NOT NULL,
    EXPIRATION_DATE         DATE            FORMAT 'YYYY-MM-DD',
    CHANGE_REFERENCE        VARCHAR(100)    CHARACTER SET LATIN NOT CASESPECIFIC,
    CREATED_TS              TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6)
)
UNIQUE PRIMARY INDEX (MERCHANT_CATEGORY, EFFECTIVE_DATE);

COLLECT STATISTICS COLUMN (MERCHANT_CATEGORY) ON ETL_STAGING_DB.REF_HIGH_RISK_MERCHANT_CATEGORY;
