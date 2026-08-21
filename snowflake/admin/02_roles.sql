-- =============================================================================
-- Snowflake Functional Roles - Retail Banking Analytics Pipeline
-- Ticket: MBA-2203 (TICKET-02)
-- =============================================================================
-- Re-runnable. Four functional roles replace the single Teradata LDAP service
-- account (svc_etl_pipeline) that previously held read/write on every database.
--
--   RB_LOADER_<env>       Lands raw source data into the staging database.
--                         No access to the data product layer.
--   RB_TRANSFORMER_<env>  Runs the staging + data product transformations.
--                         Reads sources and staging, owns data products.
--   RB_ANALYST_<env>      Read-only consumer of the data product layer.
--   RB_ADMIN_<env>        Owns the pipeline objects, manages grants; parent of
--                         the three roles above and granted to SYSADMIN so the
--                         standard Snowflake role hierarchy stays connected.
--
-- Role hierarchy (child -> parent, privileges flow upwards):
--
--   RB_LOADER_<env>      ┐
--   RB_TRANSFORMER_<env> ├──▶ RB_ADMIN_<env> ──▶ SYSADMIN ──▶ ACCOUNTADMIN
--   RB_ANALYST_<env>     ┘
--
-- RB_ANALYST is deliberately NOT granted to RB_LOADER or RB_TRANSFORMER and
-- RB_LOADER is deliberately NOT granted to RB_TRANSFORMER: the roles are
-- siblings so no pipeline role inherits another workload's privileges.
--
-- Variables: &{env}
-- =============================================================================

USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS RB_LOADER_&{env}
    COMMENT = 'Write access to the staging database; no data product access (MBA-2203)';

CREATE ROLE IF NOT EXISTS RB_TRANSFORMER_&{env}
    COMMENT = 'Reads sources/staging, owns and writes data product tables (MBA-2203)';

CREATE ROLE IF NOT EXISTS RB_ANALYST_&{env}
    COMMENT = 'Read-only access to certified data products (MBA-2203)';

CREATE ROLE IF NOT EXISTS RB_ADMIN_&{env}
    COMMENT = 'Owning/administrative role for the retail banking pipeline (MBA-2203)';

-- -----------------------------------------------------------------------------
-- Hierarchy
-- -----------------------------------------------------------------------------
GRANT ROLE RB_LOADER_&{env}      TO ROLE RB_ADMIN_&{env};
GRANT ROLE RB_TRANSFORMER_&{env} TO ROLE RB_ADMIN_&{env};
GRANT ROLE RB_ANALYST_&{env}     TO ROLE RB_ADMIN_&{env};
GRANT ROLE RB_ADMIN_&{env}       TO ROLE SYSADMIN;
