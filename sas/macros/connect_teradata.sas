/*******************************************************************************
 * Macro:   connect_teradata.sas
 * Purpose: Establish a SAS/ACCESS LIBNAME connection to Teradata and define
 *          standard library references used throughout the pipeline.
 *
 * Parameters:
 *   server   - Teradata hostname (default: from environment)
 *   username - Service account (default: from environment)
 *   auth     - Authentication mechanism LDAP|TD (default: LDAP)
 *
 * Usage:
 *   %connect_teradata();
 *   %connect_teradata(server=tddev.corp.bankdemo.com, auth=TD);
 ******************************************************************************/

%macro connect_teradata(
    server   = %sysget(TD_SERVER),
    username = %sysget(TD_USERNAME),
    auth     = %sysget(TD_LOGMECH)
);

    %put NOTE: [connect_teradata] Connecting to &server. as &username. via &auth.;

    /* Core Banking source tables (read-only) */
    libname COREDB teradata
        server   = "&server."
        user     = "&username."
        password = "{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX"  /* encrypted via PROC PWENCODE */
        database = "CORE_BANKING_DB"
        logmech  = &auth.
        bulkload = YES
        fastload = YES
    ;

    /* Transaction Processing source tables (read-only) */
    libname TXNDB teradata
        server   = "&server."
        user     = "&username."
        password = "{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX"
        database = "TXN_PROCESSING_DB"
        logmech  = &auth.
    ;

    /* ETL Staging tables (read; populated by BTEQ) */
    libname STGDB teradata
        server   = "&server."
        user     = "&username."
        password = "{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX"
        database = "ETL_STAGING_DB"
        logmech  = &auth.
    ;

    /* Data Products output tables (read/write) */
    libname DPDB teradata
        server   = "&server."
        user     = "&username."
        password = "{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX"
        database = "DATA_PRODUCTS_DB"
        logmech  = &auth.
        bulkload = YES
    ;

    %put NOTE: [connect_teradata] All library connections established.;

%mend connect_teradata;
