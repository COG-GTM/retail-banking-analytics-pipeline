/*******************************************************************************
 * Program: 04_sas_data_products.sas
 * Purpose: Assemble the CUSTOMER_MASTER_PROFILE "golden record" by joining
 *          all three upstream data products:
 *            - CUSTOMER_SEGMENTS
 *            - TRANSACTION_ANALYTICS
 *            - CUSTOMER_RISK_SCORES
 *          with the base STG_CUSTOMER_360 staging table.
 *
 *          This is the final, canonical customer view consumed enterprise-wide.
 *
 * Upstream:  DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS
 *            DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS
 *            DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES
 *            ETL_STAGING_DB.STG_CUSTOMER_360
 * Downstream: DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE
 *
 * SAS Products Used: Base SAS
 ******************************************************************************/

options mprint mlogic symbolgen compress=yes;

%include "/opt/etl/retail_banking_analytics/sas/macros/connect_teradata.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/log_step.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/validate_table.sas";

%init_audit;
%connect_teradata();

%let MODEL_VERSION = MASTER_V1.5;
%log_step(step=04_MASTER_PROFILE, status=START, msg=Building golden record);


/* ========================================================================= */
/* STEP 1: Read all upstream data products in parallel via SQL pass-through   */
/* ========================================================================= */
%log_step(step=04_MASTER_PROFILE, status=START, msg=Extracting upstream data products);

proc sql;
    /* Base customer attributes */
    create table WORK.BASE as
    select
        CUSTOMER_ID,
        trim(FIRST_NAME) || ' ' || trim(LAST_NAME) as FULL_NAME length=120,
        AGE,
        STATE_CODE,
        CUSTOMER_SINCE,
        TENURE_MONTHS,
        CUSTOMER_STATUS,
        NUM_ACCOUNTS        as TOTAL_ACCOUNTS,
        NUM_ACTIVE_ACCOUNTS as ACTIVE_ACCOUNTS,
        TOTAL_BALANCE,
        TOTAL_CREDIT_LIMIT,
        CREDIT_UTILIZATION_PCT
    from STGDB.STG_CUSTOMER_360
    where CUSTOMER_STATUS = 'A'
    ;

    /* Customer segments */
    create table WORK.SEGMENTS as
    select
        CUSTOMER_ID,
        SEGMENT_NAME,
        LIFETIME_VALUE_SCORE,
        ENGAGEMENT_SCORE,
        CROSS_SELL_FLAG,
        UPSELL_FLAG,
        RETENTION_RISK_FLAG
    from DPDB.CUSTOMER_SEGMENTS
    ;

    /* Transaction analytics (current period only) */
    create table WORK.TXN as
    select
        CUSTOMER_ID,
        TOTAL_TRANSACTIONS  as MONTHLY_TRANSACTIONS,
        TOTAL_DEBIT_AMT     as MONTHLY_SPEND,
        NET_CASH_FLOW,
        TOP_SPEND_CATEGORY,
        DIGITAL_TXN_PCT
    from DPDB.TRANSACTION_ANALYTICS
    where EFFECTIVE_DATE = today()
    ;

    /* Risk scores */
    create table WORK.RISK as
    select
        CUSTOMER_ID,
        COMPOSITE_RISK_SCORE,
        RISK_TIER,
        PROBABILITY_OF_DEFAULT,
        WATCH_LIST_FLAG
    from DPDB.CUSTOMER_RISK_SCORES
    ;
quit;

%log_step(step=04_MASTER_PROFILE, status=SUCCESS, msg=All upstream data extracted);


/* ========================================================================= */
/* STEP 2: Merge all components into the golden record                       */
/* ========================================================================= */
%log_step(step=04_MASTER_PROFILE, status=START, msg=Merging data products);

/* Sort all datasets by key for merge */
proc sort data=WORK.BASE;       by CUSTOMER_ID; run;
proc sort data=WORK.SEGMENTS;   by CUSTOMER_ID; run;
proc sort data=WORK.TXN;        by CUSTOMER_ID; run;
proc sort data=WORK.RISK;       by CUSTOMER_ID; run;

data WORK.MASTER_PROFILE;
    merge
        WORK.BASE       (in=_base)
        WORK.SEGMENTS   (in=_seg)
        WORK.TXN        (in=_txn)
        WORK.RISK       (in=_risk)
    ;
    by CUSTOMER_ID;

    /* Only keep customers that exist in the base */
    if _base;

    /* Default missing segment/txn/risk fields */
    if not _seg then do;
        SEGMENT_NAME         = 'UNCLASSIFIED';
        LIFETIME_VALUE_SCORE = 0;
        ENGAGEMENT_SCORE     = 0;
        CROSS_SELL_FLAG      = 'N';
        UPSELL_FLAG          = 'N';
        RETENTION_RISK_FLAG  = 'N';
    end;

    if not _txn then do;
        MONTHLY_TRANSACTIONS = 0;
        MONTHLY_SPEND        = 0;
        NET_CASH_FLOW        = 0;
        TOP_SPEND_CATEGORY   = '';
        DIGITAL_TXN_PCT      = 0;
    end;

    if not _risk then do;
        COMPOSITE_RISK_SCORE   = .;
        RISK_TIER              = 'UNKNOWN';
        PROBABILITY_OF_DEFAULT = .;
        WATCH_LIST_FLAG        = 'N';
    end;

    /* Metadata */
    length MODEL_VERSION $20;
    MODEL_VERSION  = "&MODEL_VERSION.";
    EFFECTIVE_DATE = today();
    LOAD_TS        = datetime();
    format EFFECTIVE_DATE yymmdd10. LOAD_TS datetime22.3;
run;

%let _master_rows = %sysfunc(attrn(%sysfunc(open(WORK.MASTER_PROFILE)), nobs));
%log_step(step=04_MASTER_PROFILE, status=SUCCESS, msg=Master profile built, rowcount=&_master_rows.);


/* ========================================================================= */
/* STEP 3: Data quality report                                               */
/* ========================================================================= */
proc sql;
    title "Master Profile - Segment Distribution";
    select SEGMENT_NAME, count(*) as N, round(mean(LIFETIME_VALUE_SCORE), 0.01) as AVG_LTV
    from WORK.MASTER_PROFILE
    group by SEGMENT_NAME
    order by N desc;

    title "Master Profile - Risk Tier Distribution";
    select RISK_TIER, count(*) as N, round(mean(COMPOSITE_RISK_SCORE), 0.01) as AVG_SCORE
    from WORK.MASTER_PROFILE
    group by RISK_TIER
    order by AVG_SCORE desc;

    title "Master Profile - Completeness Check";
    select
        count(*) as TOTAL,
        sum(case when SEGMENT_NAME   ne 'UNCLASSIFIED' then 1 else 0 end) as HAS_SEGMENT,
        sum(case when MONTHLY_TRANSACTIONS > 0          then 1 else 0 end) as HAS_TXN,
        sum(case when RISK_TIER      ne 'UNKNOWN'       then 1 else 0 end) as HAS_RISK_SCORE,
        sum(CROSS_SELL_FLAG   = 'Y') as CROSS_SELL_ELIGIBLE,
        sum(UPSELL_FLAG       = 'Y') as UPSELL_ELIGIBLE,
        sum(RETENTION_RISK_FLAG = 'Y') as RETENTION_AT_RISK,
        sum(WATCH_LIST_FLAG   = 'Y') as ON_WATCH_LIST
    from WORK.MASTER_PROFILE;
    title;
quit;


/* ========================================================================= */
/* STEP 4: Validate                                                          */
/* ========================================================================= */
%validate_table(lib=WORK, table=MASTER_PROFILE,
                key_cols=CUSTOMER_ID,
                not_null=CUSTOMER_ID FULL_NAME CUSTOMER_STATUS,
                min_rows=1000);

%if &VALIDATION_RC. ne 0 %then %do;
    %log_step(step=04_MASTER_PROFILE, status=ERROR, msg=Validation failed);
    %abort cancel;
%end;


/* ========================================================================= */
/* STEP 5: Load to Teradata                                                  */
/* ========================================================================= */
%log_step(step=04_MASTER_PROFILE, status=START, msg=Loading DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE);

proc sql;
    connect to teradata (server="&TD_SERVER." user="&TD_USERNAME."
                         password="{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX" logmech=LDAP);
    execute (DELETE FROM DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE) by teradata;
    disconnect from teradata;
quit;

proc append base=DPDB.CUSTOMER_MASTER_PROFILE
            data=WORK.MASTER_PROFILE
            force;
run;

%log_step(step=04_MASTER_PROFILE, status=SUCCESS, msg=Golden record loaded, rowcount=&_master_rows.);


/* ========================================================================= */
/* STEP 6: Collect statistics on all data product tables via pass-through     */
/* ========================================================================= */
proc sql;
    connect to teradata (server="&TD_SERVER." user="&TD_USERNAME."
                         password="{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX" logmech=LDAP);
    execute (COLLECT STATISTICS COLUMN (CUSTOMER_ID) ON DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE) by teradata;
    execute (COLLECT STATISTICS COLUMN (SEGMENT_NAME) ON DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS)      by teradata;
    execute (COLLECT STATISTICS COLUMN (RISK_TIER)    ON DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES)   by teradata;
    disconnect from teradata;
quit;


/* ========================================================================= */
/* STEP 7: Print final audit trail                                           */
/* ========================================================================= */
proc print data=WORK.PIPELINE_AUDIT noobs;
    title "Pipeline Audit Trail - Complete Run";
    format LOG_TS datetime22.3;
run;
title;

%log_step(step=04_MASTER_PROFILE, status=SUCCESS, msg=Full pipeline complete);

proc datasets library=WORK nolist;
    delete BASE SEGMENTS TXN RISK MASTER_PROFILE;
quit;
