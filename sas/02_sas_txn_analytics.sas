/*******************************************************************************
 * Program: 02_sas_txn_analytics.sas
 * Purpose: Read the BTEQ-produced STG_TXN_SUMMARY staging table, aggregate
 *          to customer level, compute spend trends and percentile rankings,
 *          and produce the TRANSACTION_ANALYTICS data product.
 *
 * Upstream:  ETL_STAGING_DB.STG_TXN_SUMMARY  (from 02_stg_txn_summary.bteq)
 *            ETL_STAGING_DB.STG_CUSTOMER_360  (for account counts)
 * Downstream: DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS
 *
 * SAS Products Used: Base SAS, SAS/STAT (PROC RANK, PROC MEANS)
 ******************************************************************************/

options mprint mlogic symbolgen compress=yes;

%include "/opt/etl/retail_banking_analytics/sas/macros/connect_teradata.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/log_step.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/validate_table.sas";

%init_audit;
%connect_teradata();

%let MODEL_VERSION = TXN_V2.1;
%let REPORTING_PERIOD = %sysfunc(putn(%sysfunc(intnx(month, %sysfunc(today()), 0, beginning)),
                        yymmn7.));
%log_step(step=02_TXN_ANALYTICS, status=START, msg=Period: &REPORTING_PERIOD.);


/* ========================================================================= */
/* STEP 1: Pull transaction summary from staging                             */
/* ========================================================================= */
proc sql;
    create table WORK.TXN_STG as
    select *
    from STGDB.STG_TXN_SUMMARY
    ;
quit;

%let _txn_rows = &sqlobs.;
%log_step(step=02_TXN_ANALYTICS, status=SUCCESS, msg=Extracted STG_TXN_SUMMARY, rowcount=&_txn_rows.);


/* ========================================================================= */
/* STEP 2: Aggregate account-level to customer-level                         */
/* ========================================================================= */
proc sql;
    create table WORK.CUST_TXN as
    select
        CUSTOMER_ID,
        count(distinct ACCOUNT_ID)      as TOTAL_ACCOUNTS,
        sum(case when DAYS_SINCE_LAST_TXN <= 30 then 1 else 0 end) as ACTIVE_ACCOUNTS,
        sum(TXN_COUNT_TOTAL)            as TOTAL_TRANSACTIONS,
        sum(AMT_TOTAL_DEBIT)            as TOTAL_DEBIT_AMT,
        sum(AMT_TOTAL_CREDIT)           as TOTAL_CREDIT_AMT,
        sum(AMT_TOTAL_CREDIT) - sum(AMT_TOTAL_DEBIT) as NET_CASH_FLOW,
        case when sum(TXN_COUNT_TOTAL) > 0
             then sum(AMT_TOTAL_DEBIT + AMT_TOTAL_CREDIT) / sum(TXN_COUNT_TOTAL)
             else 0 end                 as AVG_TRANSACTION_SIZE,
        sum(AMT_TOTAL_FEES)             as TOTAL_FEES,
        /* Top spending category across all accounts */
        max(TOP_MERCHANT_CATEGORY)      as TOP_SPEND_CATEGORY,
        /* Digital transaction percentage */
        case when sum(TXN_COUNT_TOTAL) > 0
             then (sum(TXN_COUNT_TOTAL * (PCT_WEB + PCT_MOBILE) / 100))
                  / sum(TXN_COUNT_TOTAL) * 100
             else 0 end                 as DIGITAL_TXN_PCT
    from WORK.TXN_STG
    group by CUSTOMER_ID
    ;
quit;


/* ========================================================================= */
/* STEP 3: Compute spend trend using 3-month moving window comparison        */
/* ========================================================================= */
/* In a full implementation we'd compare current period vs prior period.      */
/* Here we simulate the trend based on net cash flow direction.              */
data WORK.CUST_TXN_TREND;
    set WORK.CUST_TXN;

    length MONTHLY_SPEND_TREND $10;
    if NET_CASH_FLOW > AVG_TRANSACTION_SIZE * 5 then MONTHLY_SPEND_TREND = 'UP';
    else if NET_CASH_FLOW < -AVG_TRANSACTION_SIZE * 5 then MONTHLY_SPEND_TREND = 'DOWN';
    else MONTHLY_SPEND_TREND = 'STABLE';

    /* Placeholder revenue components */
    FEE_INCOME      = TOTAL_FEES;
    INTEREST_INCOME = TOTAL_DEBIT_AMT * 0.02;  /* Simplified interest proxy */
    REVENUE_CONTRIBUTION = FEE_INCOME + INTEREST_INCOME;

    /* Anomaly flag: transactions > 3x median */
    ANOMALY_FLAG = 'N';
run;


/* ========================================================================= */
/* STEP 4: Percentile ranking for spend                                      */
/* ========================================================================= */
proc rank data=WORK.CUST_TXN_TREND
          out=WORK.CUST_TXN_RANKED
          groups=100;
    var TOTAL_DEBIT_AMT;
    ranks SPEND_PERCENTILE;
run;


/* ========================================================================= */
/* STEP 5: Flag anomalies using PROC MEANS (IQR method)                      */
/* ========================================================================= */
proc means data=WORK.CUST_TXN_RANKED noprint;
    var TOTAL_DEBIT_AMT;
    output out=WORK._TXN_STATS
        median=_MEDIAN
        qrange=_IQR;
run;

data WORK.TXN_ANALYTICS_FINAL;
    if _N_ = 1 then set WORK._TXN_STATS(keep=_MEDIAN _IQR);

    set WORK.CUST_TXN_RANKED;

    /* Flag customers whose spend exceeds median + 3*IQR */
    if TOTAL_DEBIT_AMT > _MEDIAN + (3 * _IQR) and _IQR > 0 then ANOMALY_FLAG = 'Y';

    /* Add reporting period metadata */
    length REPORTING_PERIOD $7 MODEL_VERSION $20;
    REPORTING_PERIOD    = "&REPORTING_PERIOD.";
    MODEL_VERSION       = "&MODEL_VERSION.";
    EFFECTIVE_DATE      = today();
    LOAD_TS             = datetime();

    format EFFECTIVE_DATE yymmdd10. LOAD_TS datetime22.3;
    format TOTAL_DEBIT_AMT TOTAL_CREDIT_AMT NET_CASH_FLOW
           AVG_TRANSACTION_SIZE FEE_INCOME INTEREST_INCOME
           REVENUE_CONTRIBUTION comma18.2;

    drop _MEDIAN _IQR _TYPE_ _FREQ_;
run;

%let _final_rows = %sysfunc(attrn(%sysfunc(open(WORK.TXN_ANALYTICS_FINAL)), nobs));
%log_step(step=02_TXN_ANALYTICS, status=SUCCESS, msg=Analytics table built, rowcount=&_final_rows.);


/* ========================================================================= */
/* STEP 6: Validate                                                          */
/* ========================================================================= */
%validate_table(lib=WORK, table=TXN_ANALYTICS_FINAL,
                key_cols=CUSTOMER_ID,
                not_null=CUSTOMER_ID REPORTING_PERIOD TOTAL_TRANSACTIONS,
                min_rows=1000);

%if &VALIDATION_RC. ne 0 %then %do;
    %log_step(step=02_TXN_ANALYTICS, status=ERROR, msg=Validation failed);
    %abort cancel;
%end;


/* ========================================================================= */
/* STEP 7: Load to Teradata                                                  */
/* ========================================================================= */
%log_step(step=02_TXN_ANALYTICS, status=START, msg=Loading DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS);

proc sql;
    connect to teradata (server="&TD_SERVER." user="&TD_USERNAME."
                         password="{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX" logmech=LDAP);
    execute (
        DELETE FROM DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS
        WHERE REPORTING_PERIOD = %unquote(%str(%')&REPORTING_PERIOD.%str(%'))
    ) by teradata;
    disconnect from teradata;
quit;

proc append base=DPDB.TRANSACTION_ANALYTICS
            data=WORK.TXN_ANALYTICS_FINAL
            force;
run;

%log_step(step=02_TXN_ANALYTICS, status=SUCCESS, msg=Pipeline complete, rowcount=&_final_rows.);

proc datasets library=WORK nolist;
    delete TXN_STG CUST_TXN CUST_TXN_TREND CUST_TXN_RANKED _TXN_STATS TXN_ANALYTICS_FINAL;
quit;
