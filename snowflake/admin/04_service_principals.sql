-- =============================================================================
-- Snowflake Service Principals - Retail Banking Analytics Pipeline
-- Ticket: MBA-2203 (TICKET-02)
-- =============================================================================
-- Replaces the Teradata LDAP service account (TD_USERNAME=svc_etl_pipeline,
-- TD_LOGMECH=LDAP) and the {SAS004} passwords that used to sit in
-- sas/macros/connect_teradata.sas.
--
-- Two authentication paths are provisioned; both are password-less:
--
--   1. Key-pair (default, used by the Synapse pipeline activities).
--      The PRIVATE key lives only in Azure Key Vault
--      (secret: snowflake-svc-synapse-private-key). Only the PUBLIC key is
--      loaded into Snowflake, and it is passed in as a SnowSQL variable so no
--      key material is committed to this repository.
--
--   2. External OAuth via Microsoft Entra ID (used by Synapse linked services
--      that authenticate with the workspace managed identity, and by analysts
--      federating through Entra).
--
-- Variables:
--   &{env}
--   &{svc_synapse_user}     service user name (default SVC_SYNAPSE_SPARK_<env>)
--   &{svc_loader_user}      service user name (default SVC_RB_LOADER_<env>)
--   &{svc_synapse_pubkey}   current RSA public key body (base64, no PEM header)
--   &{svc_loader_pubkey}    current RSA public key body (base64, no PEM header)
--   &{entra_tenant_id}      Microsoft Entra tenant id
--   &{db_dp}
-- =============================================================================

USE ROLE USERADMIN;

-- -----------------------------------------------------------------------------
-- Synapse Spark service principal (transform + data product writes)
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS &{svc_synapse_user}
    TYPE = SERVICE
    DEFAULT_ROLE = RB_TRANSFORMER_&{env}
    DEFAULT_WAREHOUSE = WH_RB_SPARK_&{env}
    DEFAULT_NAMESPACE = &{db_dp}
    COMMENT = 'Azure Synapse Spark service principal, key-pair auth (MBA-2203)';

ALTER USER &{svc_synapse_user} SET
    TYPE = SERVICE
    DEFAULT_ROLE = RB_TRANSFORMER_&{env}
    DEFAULT_WAREHOUSE = WH_RB_SPARK_&{env}
    DEFAULT_NAMESPACE = &{db_dp}
    RSA_PUBLIC_KEY = '&{svc_synapse_pubkey}';

-- -----------------------------------------------------------------------------
-- Ingestion/loader service principal (lands source extracts into staging)
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS &{svc_loader_user}
    TYPE = SERVICE
    DEFAULT_ROLE = RB_LOADER_&{env}
    DEFAULT_WAREHOUSE = WH_RB_ELT_&{env}
    COMMENT = 'Ingestion service principal, key-pair auth (MBA-2203)';

ALTER USER &{svc_loader_user} SET
    TYPE = SERVICE
    DEFAULT_ROLE = RB_LOADER_&{env}
    DEFAULT_WAREHOUSE = WH_RB_ELT_&{env}
    RSA_PUBLIC_KEY = '&{svc_loader_pubkey}';

-- Service users are TYPE = SERVICE, which cannot hold a password at all; the
-- statement below is the belt-and-braces check that no legacy password remains
-- if the user pre-dates this script.
ALTER USER &{svc_synapse_user} UNSET PASSWORD;
ALTER USER &{svc_loader_user}  UNSET PASSWORD;

-- -----------------------------------------------------------------------------
-- Role assignment
-- -----------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

GRANT ROLE RB_TRANSFORMER_&{env} TO USER &{svc_synapse_user};
GRANT ROLE RB_LOADER_&{env}      TO USER &{svc_loader_user};

-- -----------------------------------------------------------------------------
-- External OAuth (Microsoft Entra ID) - alternative to key-pair
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE SECURITY INTEGRATION IF NOT EXISTS RB_ENTRA_OAUTH_&{env}
    TYPE = EXTERNAL_OAUTH
    ENABLED = TRUE
    EXTERNAL_OAUTH_TYPE = AZURE
    EXTERNAL_OAUTH_ISSUER = 'https://sts.windows.net/&{entra_tenant_id}/'
    EXTERNAL_OAUTH_JWS_KEYS_URL = 'https://login.microsoftonline.com/&{entra_tenant_id}/discovery/v2.0/keys'
    EXTERNAL_OAUTH_AUDIENCE_LIST = ('https://analysis.windows.net/powerbi/connector/Snowflake')
    EXTERNAL_OAUTH_TOKEN_USER_MAPPING_CLAIM = ('upn', 'sub')
    EXTERNAL_OAUTH_SNOWFLAKE_USER_MAPPING_ATTRIBUTE = 'LOGIN_NAME'
    EXTERNAL_OAUTH_ANY_ROLE_MODE = 'DISABLE'
    COMMENT = 'Entra ID external OAuth for Synapse/BI access (MBA-2203)';
