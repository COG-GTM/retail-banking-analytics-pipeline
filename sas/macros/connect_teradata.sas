/*******************************************************************************
 * Macro:   connect_teradata.sas
 * Purpose: Establish a SAS/ACCESS LIBNAME connection to Teradata and define
 *          standard library references used throughout the pipeline.
 *
 * DEPRECATED (MBA-2203 / TICKET-02)
 *   Teradata is being retired in favour of Snowflake. New work must use the
 *   Snowflake service principals provisioned by snowflake/admin/ and the
 *   credential helper in snowflake/connection/snowflake_credentials.py.
 *   This macro remains only for the legacy SAS programs that still run during
 *   the parallel-run window.
 *
 * Credentials
 *   No password, host name or service account is stored in this file any more.
 *   Every value is resolved at runtime from Azure Key Vault by
 *   scripts/keyvault.sh, which exports TD_SERVER / TD_USERNAME / TD_LOGMECH /
 *   TD_PASSWORD into the SAS process environment (see sas/run_sas_pipeline.sh).
 *   The macro aborts if any of them is missing rather than falling back to a
 *   built-in default.
 *
 * Parameters:
 *   server   - Teradata hostname (default: TD_SERVER from the environment)
 *   username - Service account   (default: TD_USERNAME from the environment)
 *   auth     - Auth mechanism    (default: TD_LOGMECH from the environment)
 *
 * Usage:
 *   %connect_teradata();
 ******************************************************************************/

%macro connect_teradata(
    server   = %sysget(TD_SERVER),
    username = %sysget(TD_USERNAME),
    auth     = %sysget(TD_LOGMECH)
);

    /* Password is never passed as a parameter so it cannot be echoed into the
       SAS log by a caller; it is read straight from the environment. */
    %local td_password;
    %let td_password = %sysget(TD_PASSWORD);

    %if %superq(server) = %str() or %superq(username) = %str()
        or %superq(auth) = %str() or %superq(td_password) = %str() %then %do;
        %put ERROR: [connect_teradata] TD_SERVER, TD_USERNAME, TD_LOGMECH and;
        %put ERROR- [connect_teradata] TD_PASSWORD must be exported from Azure;
        %put ERROR- [connect_teradata] Key Vault before SAS starts. See;
        %put ERROR- [connect_teradata] docs/modernization/snowflake_security.md.;
        %abort cancel;
    %end;

    %put NOTE: [connect_teradata] Connecting to &server. as &username. via &auth.;

    /* Core Banking source tables (read-only) */
    libname COREDB teradata
        server   = "&server."
        user     = "&username."
        password = "&td_password."
        database = "CORE_BANKING_DB"
        logmech  = &auth.
        bulkload = YES
        fastload = YES
    ;

    /* Transaction Processing source tables (read-only) */
    libname TXNDB teradata
        server   = "&server."
        user     = "&username."
        password = "&td_password."
        database = "TXN_PROCESSING_DB"
        logmech  = &auth.
    ;

    /* ETL Staging tables (read; populated by BTEQ) */
    libname STGDB teradata
        server   = "&server."
        user     = "&username."
        password = "&td_password."
        database = "ETL_STAGING_DB"
        logmech  = &auth.
    ;

    /* Data Products output tables (read/write) */
    libname DPDB teradata
        server   = "&server."
        user     = "&username."
        password = "&td_password."
        database = "DATA_PRODUCTS_DB"
        logmech  = &auth.
        bulkload = YES
    ;

    %let td_password = ;

    %put NOTE: [connect_teradata] All library connections established.;

%mend connect_teradata;
