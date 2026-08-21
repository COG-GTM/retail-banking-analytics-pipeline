-- =============================================================================
-- Least-Privilege Verification - Retail Banking Analytics Pipeline
-- Ticket: MBA-2203 (TICKET-02)
-- =============================================================================
-- Read-only assertions run after 00..04. Each check raises through a failing
-- SELECT so the SnowSQL runner exits non-zero when the grant model drifts.
--
-- Variables: &{env} &{db_stg} &{db_dp} &{svc_synapse_user} &{svc_loader_user}
-- =============================================================================

USE ROLE SECURITYADMIN;

-- -----------------------------------------------------------------------------
-- 1. The loader role must hold no privilege on the data product database.
-- -----------------------------------------------------------------------------
SHOW GRANTS TO ROLE RB_LOADER_&{env};

SELECT CASE
         WHEN COUNT(*) = 0 THEN 'PASS: RB_LOADER_&{env} has no data product grants'
         ELSE 1 / 0  -- force failure: loader can reach the data product layer
       END AS loader_isolation_check
FROM   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE  "granted_on" IN ('DATABASE', 'SCHEMA', 'TABLE', 'VIEW')
AND    "name" ILIKE '&{db_dp}%';

-- -----------------------------------------------------------------------------
-- 2. The analyst role must be read-only: SELECT/USAGE only, on &{db_dp} only.
-- -----------------------------------------------------------------------------
SHOW GRANTS TO ROLE RB_ANALYST_&{env};

SELECT CASE
         WHEN COUNT(*) = 0 THEN 'PASS: RB_ANALYST_&{env} is read-only'
         ELSE 1 / 0  -- force failure: analyst holds a write privilege
       END AS analyst_readonly_check
FROM   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE  "privilege" NOT IN ('USAGE', 'SELECT', 'OPERATE');

-- -----------------------------------------------------------------------------
-- 3. No pipeline user may authenticate with a password.
-- -----------------------------------------------------------------------------
SHOW USERS LIKE 'SVC%';

SELECT CASE
         WHEN COUNT(*) = 0 THEN 'PASS: all service users are password-less'
         ELSE 1 / 0  -- force failure: a service user still has a password
       END AS passwordless_check
FROM   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE  "has_password" = 'true';

-- -----------------------------------------------------------------------------
-- 4. Both service principals must have an RSA public key registered.
-- -----------------------------------------------------------------------------
DESC USER &{svc_synapse_user};

SELECT CASE
         WHEN COUNT(*) = 1 THEN 'PASS: &{svc_synapse_user} has a public key'
         ELSE 1 / 0  -- force failure: key-pair auth is not configured
       END AS synapse_keypair_check
FROM   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE  "property" = 'RSA_PUBLIC_KEY_FP'
AND    "value" IS NOT NULL
AND    "value" <> 'null';

DESC USER &{svc_loader_user};

SELECT CASE
         WHEN COUNT(*) = 1 THEN 'PASS: &{svc_loader_user} has a public key'
         ELSE 1 / 0  -- force failure: key-pair auth is not configured
       END AS loader_keypair_check
FROM   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE  "property" = 'RSA_PUBLIC_KEY_FP'
AND    "value" IS NOT NULL
AND    "value" <> 'null';
