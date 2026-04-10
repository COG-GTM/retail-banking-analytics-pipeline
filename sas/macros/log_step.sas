/*******************************************************************************
 * Macro:   log_step.sas
 * Purpose: Write a standardised log message with timestamp, step name, and
 *          optional row counts. Also inserts a record into the SAS-side
 *          ETL audit dataset for traceability.
 *
 * Parameters:
 *   step     - Name of the pipeline step
 *   status   - START | SUCCESS | WARNING | ERROR
 *   msg      - Free-text description (optional)
 *   rowcount - Number of rows processed (optional)
 *
 * Usage:
 *   %log_step(step=CUSTOMER_SEG, status=START);
 *   %log_step(step=CUSTOMER_SEG, status=SUCCESS, rowcount=125000);
 ******************************************************************************/

%macro log_step(step=, status=, msg=, rowcount=);

    %local _ts;
    %let _ts = %sysfunc(datetime(), datetime22.3);

    %put NOTE: ================================================================;
    %put NOTE: [PIPELINE] &_ts. | &step. | &status.;
    %if %length(&msg.) > 0 %then %put NOTE: [PIPELINE] &msg.;
    %if %length(&rowcount.) > 0 %then %put NOTE: [PIPELINE] Rows: &rowcount.;
    %put NOTE: ================================================================;

    /* Append to the in-session audit trail dataset */
    proc sql noprint;
        insert into WORK.PIPELINE_AUDIT
        set JOB_NAME       = "&step.",
            STATUS          = "&status.",
            MESSAGE         = "&msg.",
            ROW_COUNT       = %if %length(&rowcount.) > 0 %then &rowcount.; %else .;,
            LOG_TS          = dhms(today(), 0, 0, time())
        ;
    quit;

%mend log_step;


/* Initialise the audit trail if it does not exist */
%macro init_audit;
    %if %sysfunc(exist(WORK.PIPELINE_AUDIT)) = 0 %then %do;
        data WORK.PIPELINE_AUDIT;
            length JOB_NAME $40 STATUS $10 MESSAGE $200;
            format LOG_TS datetime22.3;
            ROW_COUNT = .;
            LOG_TS = .;
            delete;  /* empty dataset with structure */
        run;
    %end;
%mend init_audit;
