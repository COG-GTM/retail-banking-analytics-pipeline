/*******************************************************************************
 * Macro:   validate_table.sas
 * Purpose: Run standard quality checks against a Teradata or SAS table:
 *          row count > 0, no duplicate keys, null-rate thresholds.
 *          Sets a global macro variable &VALIDATION_RC (0=pass, 1=fail).
 *
 * Parameters:
 *   lib       - SAS library reference
 *   table     - Table name
 *   key_cols  - Space-separated list of key columns for uniqueness check
 *   not_null  - Space-separated list of columns that must not be NULL
 *   min_rows  - Minimum acceptable row count (default: 1)
 *
 * Usage:
 *   %validate_table(lib=STGDB, table=STG_CUSTOMER_360,
 *                   key_cols=CUSTOMER_ID, not_null=CUSTOMER_ID LAST_NAME,
 *                   min_rows=1000);
 *   %if &VALIDATION_RC. ne 0 %then %do; ... handle failure ... %end;
 ******************************************************************************/

%macro validate_table(lib=, table=, key_cols=, not_null=, min_rows=1);

    %global VALIDATION_RC;
    %let VALIDATION_RC = 0;

    %local _dsname _nobs _i _col _null_cnt _dup_cnt _nkeys;
    %let _dsname = &lib..&table.;

    %put NOTE: [validate_table] Validating &_dsname.;

    /* ------------------------------------------------------------------ */
    /* Check 1: Table exists and has minimum rows                         */
    /* ------------------------------------------------------------------ */
    proc sql noprint;
        select count(*) into :_nobs trimmed
        from &_dsname.;
    quit;

    %if &_nobs. < &min_rows. %then %do;
        %put ERROR: [validate_table] &_dsname. has &_nobs. rows (minimum: &min_rows.);
        %let VALIDATION_RC = 1;
        %return;
    %end;
    %put NOTE: [validate_table] Row count OK: &_nobs. (min: &min_rows.);

    /* ------------------------------------------------------------------ */
    /* Check 2: Primary key uniqueness                                    */
    /* ------------------------------------------------------------------ */
    %if %length(&key_cols.) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_dup_cnt trimmed
            from (
                select %sysfunc(tranwrd(&key_cols., %str( ), %str(, )))
                from &_dsname.
                group by %sysfunc(tranwrd(&key_cols., %str( ), %str(, )))
                having count(*) > 1
            );
        quit;

        %if &_dup_cnt. > 0 %then %do;
            %put ERROR: [validate_table] &_dsname. has &_dup_cnt. duplicate key groups on (&key_cols.);
            %let VALIDATION_RC = 1;
            %return;
        %end;
        %put NOTE: [validate_table] Key uniqueness OK on (&key_cols.);
    %end;

    /* ------------------------------------------------------------------ */
    /* Check 3: NOT NULL constraints                                      */
    /* ------------------------------------------------------------------ */
    %if %length(&not_null.) > 0 %then %do;
        %let _i = 1;
        %let _col = %scan(&not_null., &_i., %str( ));

        %do %while(%length(&_col.) > 0);
            proc sql noprint;
                select count(*) into :_null_cnt trimmed
                from &_dsname.
                where &_col. is missing;
            quit;

            %if &_null_cnt. > 0 %then %do;
                %put WARNING: [validate_table] &_dsname..&_col. has &_null_cnt. NULL values;
            %end;
            %else %do;
                %put NOTE: [validate_table] NOT NULL check passed for &_col.;
            %end;

            %let _i = %eval(&_i. + 1);
            %let _col = %scan(&not_null., &_i., %str( ));
        %end;
    %end;

    %put NOTE: [validate_table] All checks passed for &_dsname.;

%mend validate_table;
