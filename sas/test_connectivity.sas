/*******************************************************************************
 * Program: test_connectivity.sas
 * Purpose: Pre-cutover connectivity gate for the SAS layer. Establishes the
 *          standard LIBNAMEs via %connect_teradata against whatever endpoint
 *          TD_SERVER / TD_LOGMECH currently resolve to, validates that each
 *          library returns non-empty, well-formed data, and round-trips a
 *          small table through the bulkload/fastload path (the protocol most
 *          likely to break against cloud Teradata).
 *
 * Usage:
 *   TD_SERVER=<cloud-host> TD_LOGMECH=TD2 \
 *       sas -sysin sas/test_connectivity.sas -log conn_test.log
 *
 * Exit:  0 = all checks passed, non-zero = at least one check failed
 *        (return code is set via %abort return &_CONN_RC.)
 ******************************************************************************/

options mprint mlogic symbolgen compress=yes;

/* Load shared macros */
%include "/opt/etl/retail_banking_analytics/sas/macros/connect_teradata.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/log_step.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/validate_table.sas";

%global _CONN_RC;
%let _CONN_RC = 0;

%init_audit;


/* ========================================================================= */
/* STEP 0: Echo the resolved endpoint so the operator can confirm the target  */
/* ========================================================================= */
%macro echo_endpoint;
    %local _srv _usr _auth;
    %let _srv  = %sysget(TD_SERVER);
    %let _usr  = %sysget(TD_USERNAME);
    %let _auth = %sysget(TD_LOGMECH);

    %put NOTE: ================================================================;
    %put NOTE: [CONNTEST] TD_SERVER   = &_srv.;
    %put NOTE: [CONNTEST] TD_USERNAME = &_usr.;
    %put NOTE: [CONNTEST] TD_LOGMECH  = &_auth.;
    %put NOTE: ================================================================;
%mend echo_endpoint;

%echo_endpoint;
%log_step(step=CONN_TEST, status=START, msg=Beginning SAS connectivity smoke test);


/* ========================================================================= */
/* STEP 1: Establish COREDB / TXNDB / STGDB / DPDB LIBNAMEs                   */
/* ========================================================================= */
%connect_teradata();

%macro check_libs;
    %local _i _lib;
    %let _i = 1;
    %let _lib = %scan(COREDB TXNDB STGDB DPDB, &_i., %str( ));

    %do %while(%length(&_lib.) > 0);
        %if %sysfunc(libref(&_lib.)) ne 0 %then %do;
            %put ERROR: [CONNTEST] LIBNAME &_lib. was not assigned;
            %let _CONN_RC = 1;
        %end;
        %else %do;
            %put NOTE: [CONNTEST] LIBNAME &_lib. assigned;
        %end;
        %let _i = %eval(&_i. + 1);
        %let _lib = %scan(COREDB TXNDB STGDB DPDB, &_i., %str( ));
    %end;
%mend check_libs;

%check_libs;

%if &_CONN_RC. ne 0 %then %do;
    %log_step(step=CONN_TEST, status=ERROR, msg=One or more LIBNAMEs failed to assign);
    %abort return 1;
%end;


/* ========================================================================= */
/* STEP 2: Validate a representative table in each library                    */
/* ========================================================================= */
%macro check_table(lib=, table=, key_cols=, not_null=, min_rows=1);

    %validate_table(lib=&lib., table=&table., key_cols=&key_cols.,
                    not_null=&not_null., min_rows=&min_rows.);

    %if &VALIDATION_RC. ne 0 %then %do;
        %put ERROR: [CONNTEST] Validation failed for &lib..&table.;
        %log_step(step=CONN_TEST, status=ERROR, msg=Validation failed for &lib..&table.);
        %let _CONN_RC = 1;
    %end;
    %else %do;
        %put NOTE: [CONNTEST] Validation passed for &lib..&table.;
    %end;

%mend check_table;

%check_table(lib=COREDB, table=CUSTOMERS,        key_cols=CUSTOMER_ID,
             not_null=CUSTOMER_ID LAST_NAME);
%check_table(lib=TXNDB,  table=TRANSACTIONS,     key_cols=TRANSACTION_ID,
             not_null=TRANSACTION_ID ACCOUNT_ID);
%check_table(lib=STGDB,  table=STG_CUSTOMER_360, key_cols=CUSTOMER_ID,
             not_null=CUSTOMER_ID);
%check_table(lib=DPDB,   table=CUSTOMER_MASTER_PROFILE, key_cols=CUSTOMER_ID,
             not_null=CUSTOMER_ID);


/* ========================================================================= */
/* STEP 3: Exercise the bulkload / fastload write path                        */
/* ------------------------------------------------------------------------- */
/* COREDB and DPDB are declared with bulkload=YES (and COREDB fastload=YES).  */
/* Those protocols use separate ports/handshakes from the ordinary CLIv2      */
/* session, so they can fail against a cloud endpoint even when plain SELECTs */
/* succeed. Round-trip a small table through DPDB to prove the path works.    */
/* ========================================================================= */
%let _RT_TABLE = TMP_CONN_TEST;

data WORK.CONN_PROBE;
    length PROBE_ID 8 PROBE_LABEL $40 PROBE_TS 8;
    format PROBE_TS datetime22.3;
    do PROBE_ID = 1 to 100;
        PROBE_LABEL = cats('CONN_PROBE_', put(PROBE_ID, z4.));
        PROBE_TS    = datetime();
        output;
    end;
run;

/* Drop any leftover probe table from a previous run */
proc sql;
    connect to teradata (server="%sysget(TD_SERVER)" user="%sysget(TD_USERNAME)"
                         password="{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX"
                         logmech=%sysget(TD_LOGMECH));
    execute (DROP TABLE DATA_PRODUCTS_DB.&_RT_TABLE.) by teradata;
    disconnect from teradata;
quit;

/* Reset the return code from the expected "table does not exist" failure */
%let SYSCC = 0;

/* Write via the bulkload path */
data DPDB.&_RT_TABLE. (bulkload=YES fastload=YES bl_log=BL_CONN_TEST);
    set WORK.CONN_PROBE;
run;

%macro check_roundtrip;
    %if &SYSERR. > 4 %then %do;
        %put ERROR: [CONNTEST] bulkload/fastload write to DPDB.&_RT_TABLE. failed (SYSERR=&SYSERR.);
        %log_step(step=CONN_TEST, status=ERROR, msg=bulkload write failed);
        %let _CONN_RC = 1;
        %return;
    %end;

    %local _rt_rows;
    proc sql noprint;
        select count(*) into :_rt_rows trimmed from DPDB.&_RT_TABLE.;
    quit;

    %if &_rt_rows. ne 100 %then %do;
        %put ERROR: [CONNTEST] Round-trip read returned &_rt_rows. rows (expected 100);
        %log_step(step=CONN_TEST, status=ERROR, msg=bulkload round-trip row count mismatch);
        %let _CONN_RC = 1;
        %return;
    %end;

    %put NOTE: [CONNTEST] bulkload/fastload round-trip OK (&_rt_rows. rows);

    /* Confirm the values survived the fast-load path intact */
    %validate_table(lib=DPDB, table=&_RT_TABLE., key_cols=PROBE_ID,
                    not_null=PROBE_ID PROBE_LABEL, min_rows=100);

    %if &VALIDATION_RC. ne 0 %then %do;
        %put ERROR: [CONNTEST] Round-trip table failed validation;
        %let _CONN_RC = 1;
    %end;
%mend check_roundtrip;

%check_roundtrip;

/* Clean up the probe table regardless of outcome */
proc sql;
    connect to teradata (server="%sysget(TD_SERVER)" user="%sysget(TD_USERNAME)"
                         password="{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX"
                         logmech=%sysget(TD_LOGMECH));
    execute (DROP TABLE DATA_PRODUCTS_DB.&_RT_TABLE.) by teradata;
    disconnect from teradata;
quit;
%let SYSCC = 0;


/* ========================================================================= */
/* STEP 4: Summary and exit code                                             */
/* ========================================================================= */
proc datasets library=WORK nolist;
    delete CONN_PROBE;
quit;

libname COREDB clear;
libname TXNDB  clear;
libname STGDB  clear;
libname DPDB   clear;

%macro conn_test_exit;
    %if &_CONN_RC. ne 0 %then %do;
        %put ERROR: [CONNTEST] Connectivity smoke test FAILED against %sysget(TD_SERVER);
        %log_step(step=CONN_TEST, status=ERROR, msg=Connectivity smoke test failed);
        %abort return &_CONN_RC.;
    %end;

    %put NOTE: [CONNTEST] Connectivity smoke test PASSED against %sysget(TD_SERVER);
    %log_step(step=CONN_TEST, status=SUCCESS, msg=All connectivity checks passed);
%mend conn_test_exit;

%conn_test_exit;

proc print data=WORK.PIPELINE_AUDIT noobs;
    title "SAS Connectivity Smoke Test - Audit Trail";
run;
title;
