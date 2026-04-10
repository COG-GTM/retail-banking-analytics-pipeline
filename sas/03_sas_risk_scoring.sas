/*******************************************************************************
 * Program: 03_sas_risk_scoring.sas
 * Purpose: Consume the BTEQ-produced STG_RISK_FACTORS table, build a composite
 *          risk score using a weighted logistic regression model, classify
 *          customers into risk tiers, and write CUSTOMER_RISK_SCORES.
 *
 * Upstream:  ETL_STAGING_DB.STG_RISK_FACTORS   (from 03_stg_risk_factors.bteq)
 *            ETL_STAGING_DB.STG_CUSTOMER_360    (for baseline attributes)
 * Downstream: DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES
 *
 * SAS Products Used: Base SAS, SAS/STAT (PROC LOGISTIC, PROC SCORE)
 ******************************************************************************/

options mprint mlogic symbolgen compress=yes;

%include "/opt/etl/retail_banking_analytics/sas/macros/connect_teradata.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/log_step.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/validate_table.sas";

%init_audit;
%connect_teradata();

%let MODEL_VERSION = RISK_V4.0;
%let RISK_THRESHOLD = %sysget(RISK_SCORE_THRESHOLD);  /* from config: 700 */
%log_step(step=03_RISK_SCORING, status=START, msg=Model version &MODEL_VERSION.);


/* ========================================================================= */
/* STEP 1: Extract risk factor staging data                                  */
/* ========================================================================= */
proc sql;
    create table WORK.RISK_RAW as
    select
        r.*,
        c.TENURE_MONTHS,
        c.NUM_ACTIVE_ACCOUNTS,
        c.TOTAL_BALANCE,
        c.CUSTOMER_STATUS
    from STGDB.STG_RISK_FACTORS r
    inner join STGDB.STG_CUSTOMER_360 c
        on r.CUSTOMER_ID = c.CUSTOMER_ID
    where c.CUSTOMER_STATUS = 'A'
    ;
quit;

%let _risk_rows = &sqlobs.;
%log_step(step=03_RISK_SCORING, status=SUCCESS, msg=Extracted risk factors, rowcount=&_risk_rows.);


/* ========================================================================= */
/* STEP 2: Feature preparation - handle missing values and derive ratios     */
/* ========================================================================= */
data WORK.RISK_FEATURES;
    set WORK.RISK_RAW;

    /* Impute missing bureau scores with population median (placeholder) */
    if EXTERNAL_CREDIT_SCORE <= 0 or EXTERNAL_CREDIT_SCORE = . then EXTERNAL_CREDIT_SCORE = 680;

    /* Normalise bureau score to 0-100 scale */
    BUREAU_SCORE_NORM = (EXTERNAL_CREDIT_SCORE - 300) / (850 - 300) * 100;

    /* Balance trend: ratio of 30D avg to 90D avg */
    if AVG_DAILY_BALANCE_90D > 0 then
        BALANCE_TREND_RATIO = AVG_DAILY_BALANCE_30D / AVG_DAILY_BALANCE_90D;
    else
        BALANCE_TREND_RATIO = 1;

    /* Velocity ratio: 7-day vs 30-day debit velocity */
    if DEBIT_VELOCITY_30D > 0 then
        VELOCITY_RATIO = (DEBIT_VELOCITY_7D * (30/7)) / DEBIT_VELOCITY_30D;
    else
        VELOCITY_RATIO = 1;

    /* Binary target for model training (simplified: late payments > 2 = default proxy) */
    DEFAULT_FLAG = (PAYMENT_LATE_CNT > 2);
run;


/* ========================================================================= */
/* STEP 3: Logistic regression model for probability of default              */
/* ========================================================================= */
%log_step(step=03_RISK_SCORING, status=START, msg=Training logistic regression model);

proc logistic data=WORK.RISK_FEATURES
              outmodel=WORK.RISK_MODEL
              descending
              noprint;
    model DEFAULT_FLAG =
        BUREAU_SCORE_NORM
        CREDIT_UTIL_RATIO
        PAYMENT_ONTIME_PCT
        BALANCE_VOLATILITY
        VELOCITY_RATIO
        ACCOUNT_OVERDRAFT_CNT
        LARGE_WITHDRAWAL_CNT
        HIGH_RISK_MERCHANT_CNT
        TENURE_MONTHS
    / selection=stepwise
      slentry=0.10
      slstay=0.05
      lackfit;
    output out=WORK.RISK_SCORED predicted=PROB_DEFAULT;
run;


/* ========================================================================= */
/* STEP 4: Build composite risk score and classify into tiers                */
/* ========================================================================= */
%log_step(step=03_RISK_SCORING, status=START, msg=Computing composite scores);

data WORK.RISK_CLASSIFIED;
    set WORK.RISK_SCORED;

    /* Component scores (each 0-100 scale) */
    CREDIT_RISK_COMPONENT    = max(0, min(100, 100 - BUREAU_SCORE_NORM));
    BEHAVIOUR_RISK_COMPONENT = max(0, min(100, 100 - PAYMENT_ONTIME_PCT));
    VELOCITY_RISK_COMPONENT  = max(0, min(100, (VELOCITY_RATIO - 1) * 50));
    BUREAU_SCORE_COMPONENT   = max(0, min(100, BUREAU_SCORE_NORM));
    PAYMENT_HISTORY_COMP     = max(0, min(100, PAYMENT_ONTIME_PCT));

    /* Weighted composite (higher = higher risk) */
    COMPOSITE_RISK_SCORE = round(
        CREDIT_RISK_COMPONENT    * 0.30 +
        BEHAVIOUR_RISK_COMPONENT * 0.25 +
        VELOCITY_RISK_COMPONENT  * 0.15 +
        (100 - BUREAU_SCORE_COMPONENT) * 0.20 +
        (100 - PAYMENT_HISTORY_COMP)   * 0.10
    , 0.01);

    /* Probability of default from logistic model */
    PROBABILITY_OF_DEFAULT = round(coalesce(PROB_DEFAULT, 0), 0.000001);

    /* Risk tier classification */
    length RISK_TIER $20;
    if      COMPOSITE_RISK_SCORE < 20 then RISK_TIER = 'LOW';
    else if COMPOSITE_RISK_SCORE < 40 then RISK_TIER = 'MODERATE';
    else if COMPOSITE_RISK_SCORE < 60 then RISK_TIER = 'ELEVATED';
    else if COMPOSITE_RISK_SCORE < 80 then RISK_TIER = 'HIGH';
    else                                    RISK_TIER = 'CRITICAL';

    /* Primary and secondary risk drivers */
    length PRIMARY_RISK_DRIVER SECONDARY_RISK_DRIVER $40;
    array _comp[4] CREDIT_RISK_COMPONENT BEHAVIOUR_RISK_COMPONENT
                   VELOCITY_RISK_COMPONENT (100 - BUREAU_SCORE_COMPONENT);
    array _lbl[4] $40 _temporary_ (
        'CREDIT_UTILIZATION' 'PAYMENT_BEHAVIOUR' 'TRANSACTION_VELOCITY' 'BUREAU_SCORE'
    );

    /* Find top two drivers */
    _max1 = 0; _max2 = 0;
    do i = 1 to 4;
        if _comp[i] > _max1 then do;
            _max2 = _max1;
            SECONDARY_RISK_DRIVER = PRIMARY_RISK_DRIVER;
            _max1 = _comp[i];
            PRIMARY_RISK_DRIVER = _lbl[i];
        end;
        else if _comp[i] > _max2 then do;
            _max2 = _comp[i];
            SECONDARY_RISK_DRIVER = _lbl[i];
        end;
    end;

    /* Score delta (placeholder: would compare to prior day in production) */
    SCORE_DELTA_30D = 0;

    /* Watch list: critical + high probability of default */
    WATCH_LIST_FLAG = ifc(RISK_TIER = 'CRITICAL' and PROBABILITY_OF_DEFAULT > 0.5, 'Y', 'N');

    /* Review required: elevated or above with recent velocity spike */
    REVIEW_REQUIRED_FLAG = ifc(COMPOSITE_RISK_SCORE >= 60 and VELOCITY_RATIO > 2.0, 'Y', 'N');

    MODEL_VERSION  = "&MODEL_VERSION.";
    EFFECTIVE_DATE = today();
    LOAD_TS        = datetime();
    format EFFECTIVE_DATE yymmdd10. LOAD_TS datetime22.3;

    keep CUSTOMER_ID COMPOSITE_RISK_SCORE RISK_TIER PROBABILITY_OF_DEFAULT
         CREDIT_RISK_COMPONENT BEHAVIOUR_RISK_COMPONENT VELOCITY_RISK_COMPONENT
         BUREAU_SCORE_COMPONENT PAYMENT_HISTORY_COMP
         PRIMARY_RISK_DRIVER SECONDARY_RISK_DRIVER SCORE_DELTA_30D
         WATCH_LIST_FLAG REVIEW_REQUIRED_FLAG MODEL_VERSION EFFECTIVE_DATE LOAD_TS;
run;

/* Rename to match target schema */
data WORK.CUSTOMER_RISK_FINAL;
    set WORK.RISK_CLASSIFIED(rename=(PAYMENT_HISTORY_COMP=PAYMENT_HISTORY_COMPONENT));
run;

%let _risk_final = %sysfunc(attrn(%sysfunc(open(WORK.CUSTOMER_RISK_FINAL)), nobs));
%log_step(step=03_RISK_SCORING, status=SUCCESS, msg=Risk scores computed, rowcount=&_risk_final.);


/* ========================================================================= */
/* STEP 5: Validate                                                          */
/* ========================================================================= */
%validate_table(lib=WORK, table=CUSTOMER_RISK_FINAL,
                key_cols=CUSTOMER_ID,
                not_null=CUSTOMER_ID COMPOSITE_RISK_SCORE RISK_TIER,
                min_rows=1000);

%if &VALIDATION_RC. ne 0 %then %do;
    %log_step(step=03_RISK_SCORING, status=ERROR, msg=Validation failed);
    %abort cancel;
%end;

/* Risk tier distribution for monitoring */
proc freq data=WORK.CUSTOMER_RISK_FINAL noprint;
    tables RISK_TIER / out=WORK._TIER_DIST;
run;

proc print data=WORK._TIER_DIST noobs;
    title "Risk Tier Distribution - &MODEL_VERSION.";
run;
title;


/* ========================================================================= */
/* STEP 6: Load to Teradata                                                  */
/* ========================================================================= */
%log_step(step=03_RISK_SCORING, status=START, msg=Loading DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES);

proc sql;
    connect to teradata (server="&TD_SERVER." user="&TD_USERNAME."
                         password="{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX" logmech=LDAP);
    execute (DELETE FROM DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES) by teradata;
    disconnect from teradata;
quit;

proc append base=DPDB.CUSTOMER_RISK_SCORES
            data=WORK.CUSTOMER_RISK_FINAL
            force;
run;

%log_step(step=03_RISK_SCORING, status=SUCCESS, msg=Pipeline complete, rowcount=&_risk_final.);

proc datasets library=WORK nolist;
    delete RISK_RAW RISK_FEATURES RISK_MODEL RISK_SCORED RISK_CLASSIFIED
           CUSTOMER_RISK_FINAL _TIER_DIST;
quit;
