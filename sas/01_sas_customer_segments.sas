/*******************************************************************************
 * Program: 01_sas_customer_segments.sas
 * Purpose: Read the BTEQ-produced STG_CUSTOMER_360 staging table, engineer
 *          features, run k-means clustering to segment customers, and write
 *          the final CUSTOMER_SEGMENTS data product to Teradata.
 *
 * Upstream:  ETL_STAGING_DB.STG_CUSTOMER_360  (from 01_stg_customer_360.bteq)
 * Downstream: DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS
 *
 * SAS Products Used: Base SAS, SAS/STAT (PROC FASTCLUS, PROC STDIZE)
 ******************************************************************************/

options mprint mlogic symbolgen compress=yes;

/* Load shared macros */
%include "/opt/etl/retail_banking_analytics/sas/macros/connect_teradata.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/log_step.sas";
%include "/opt/etl/retail_banking_analytics/sas/macros/validate_table.sas";

/* Initialise audit trail and Teradata connections */
%init_audit;
%connect_teradata();

%let MODEL_VERSION = SEG_V3.2;
%log_step(step=01_CUSTOMER_SEG, status=START, msg=Beginning customer segmentation pipeline);


/* ========================================================================= */
/* STEP 1: Pull staging data into SAS work library                           */
/* ========================================================================= */
%log_step(step=01_CUSTOMER_SEG, status=START, msg=Extracting STG_CUSTOMER_360);

proc sql;
    create table WORK.CUST_360 as
    select
        CUSTOMER_ID,
        AGE,
        TENURE_MONTHS,
        CUSTOMER_STATUS,
        SEGMENT_CODE,
        STATE_CODE,
        NUM_ACCOUNTS,
        NUM_ACTIVE_ACCOUNTS,
        HAS_CHECKING,
        HAS_SAVINGS,
        HAS_CREDIT,
        HAS_LOAN,
        TOTAL_BALANCE,
        TOTAL_CREDIT_LIMIT,
        CREDIT_UTILIZATION_PCT
    from STGDB.STG_CUSTOMER_360
    where CUSTOMER_STATUS = 'A'
    ;
quit;

%let _extract_rows = &sqlobs.;
%log_step(step=01_CUSTOMER_SEG, status=SUCCESS, msg=Extracted staging data, rowcount=&_extract_rows.);


/* ========================================================================= */
/* STEP 2: Feature engineering for clustering                                */
/* ========================================================================= */
%log_step(step=01_CUSTOMER_SEG, status=START, msg=Engineering features);

data WORK.CUST_FEATURES;
    set WORK.CUST_360;

    /* Product breadth index: proportion of product types held */
    PRODUCT_BREADTH = mean(
        (HAS_CHECKING = 'Y'),
        (HAS_SAVINGS  = 'Y'),
        (HAS_CREDIT   = 'Y'),
        (HAS_LOAN     = 'Y')
    );

    /* Tenure grouping */
    length TENURE_GROUP $20;
    if TENURE_MONTHS < 12       then TENURE_GROUP = 'NEW (<1yr)';
    else if TENURE_MONTHS < 36  then TENURE_GROUP = 'DEVELOPING (1-3yr)';
    else if TENURE_MONTHS < 84  then TENURE_GROUP = 'ESTABLISHED (3-7yr)';
    else                             TENURE_GROUP = 'LOYAL (7yr+)';

    /* Age grouping */
    length AGE_GROUP $20;
    if      AGE < 25  then AGE_GROUP = 'GEN_Z';
    else if AGE < 41  then AGE_GROUP = 'MILLENNIAL';
    else if AGE < 57  then AGE_GROUP = 'GEN_X';
    else if AGE < 76  then AGE_GROUP = 'BOOMER';
    else                    AGE_GROUP = 'SILENT';

    /* Balance tier */
    length BALANCE_TIER $20;
    if      TOTAL_BALANCE < 1000    then BALANCE_TIER = 'LOW';
    else if TOTAL_BALANCE < 10000   then BALANCE_TIER = 'MODERATE';
    else if TOTAL_BALANCE < 100000  then BALANCE_TIER = 'AFFLUENT';
    else                                  BALANCE_TIER = 'HIGH_NET_WORTH';

    /* Digital adoption proxy (placeholder; will be enriched by txn analytics) */
    DIGITAL_ADOPTION_SCORE = 0;

    /* Numeric features for clustering */
    LOG_BALANCE = log(max(TOTAL_BALANCE, 1));
    ACCT_RATIO  = NUM_ACTIVE_ACCOUNTS / max(NUM_ACCOUNTS, 1);
run;


/* ========================================================================= */
/* STEP 3: Standardise numeric features for k-means                          */
/* ========================================================================= */
proc stdize data=WORK.CUST_FEATURES
            out=WORK.CUST_STANDARDISED
            method=std;
    var LOG_BALANCE TENURE_MONTHS CREDIT_UTILIZATION_PCT PRODUCT_BREADTH ACCT_RATIO AGE;
run;


/* ========================================================================= */
/* STEP 4: K-Means clustering (5 segments)                                   */
/* ========================================================================= */
%log_step(step=01_CUSTOMER_SEG, status=START, msg=Running FASTCLUS k=5);

proc fastclus data=WORK.CUST_STANDARDISED
              out=WORK.CUST_CLUSTERED
              maxclusters=5
              maxiter=50
              converge=0.001
              replace=full
              least=2;
    var LOG_BALANCE TENURE_MONTHS CREDIT_UTILIZATION_PCT PRODUCT_BREADTH ACCT_RATIO AGE;
    id CUSTOMER_ID;
run;


/* ========================================================================= */
/* STEP 5: Label clusters with business-meaningful segment names             */
/* ========================================================================= */
%log_step(step=01_CUSTOMER_SEG, status=START, msg=Labelling segments);

proc sql;
    create table WORK.CLUSTER_PROFILES as
    select
        CLUSTER,
        count(*)                        as N,
        avg(LOG_BALANCE)                as AVG_BALANCE,
        avg(TENURE_MONTHS)              as AVG_TENURE,
        avg(PRODUCT_BREADTH)            as AVG_BREADTH,
        avg(CREDIT_UTILIZATION_PCT)     as AVG_CREDIT_UTIL
    from WORK.CUST_CLUSTERED
    group by CLUSTER
    order by AVG_BALANCE desc
    ;
quit;

data WORK.SEGMENT_LABELS;
    set WORK.CLUSTER_PROFILES;
    length SEGMENT_NAME $40;
    /* Assign labels based on ordered cluster profiles */
    if _N_ = 1 then SEGMENT_NAME = 'PREMIUM_WEALTH';
    else if _N_ = 2 then SEGMENT_NAME = 'ENGAGED_MAINSTREAM';
    else if _N_ = 3 then SEGMENT_NAME = 'GROWING_DIGITAL';
    else if _N_ = 4 then SEGMENT_NAME = 'CREDIT_DEPENDENT';
    else SEGMENT_NAME = 'VALUE_BASIC';

    SUBSEGMENT_ID = 0;  /* Placeholder for future sub-segmentation */
    keep CLUSTER SEGMENT_NAME SUBSEGMENT_ID;
run;


/* ========================================================================= */
/* STEP 6: Merge cluster labels back, compute scores, set action flags       */
/* ========================================================================= */
proc sql;
    create table WORK.CUSTOMER_SEGMENTS_FINAL as
    select
        c.CUSTOMER_ID,
        s.SEGMENT_NAME,
        c.CLUSTER                           as SEGMENT_ID,
        s.SUBSEGMENT_ID,
        /* Lifetime value heuristic: balance * tenure * breadth */
        round(c.LOG_BALANCE * c.TENURE_MONTHS * c.PRODUCT_BREADTH * 10, 0.01)
                                            as LIFETIME_VALUE_SCORE,
        round(c.ACCT_RATIO * 100, 0.01)    as ENGAGEMENT_SCORE,
        f.DIGITAL_ADOPTION_SCORE,
        round(f.PRODUCT_BREADTH * 100, 0.01) as PRODUCT_BREADTH_INDEX,
        f.TENURE_GROUP,
        f.AGE_GROUP,
        f.BALANCE_TIER,
        /* Channel preference (placeholder; enriched later by txn analytics) */
        ''                                  as CHANNEL_PREFERENCE length=10,
        /* Cross-sell if low product breadth but good engagement */
        case when f.PRODUCT_BREADTH < 0.50 and c.ACCT_RATIO >= 0.75 then 'Y' else 'N' end
                                            as CROSS_SELL_FLAG,
        /* Upsell if moderate balance with room to grow */
        case when f.BALANCE_TIER = 'MODERATE' and f.TENURE_GROUP not in ('NEW (<1yr)') then 'Y' else 'N' end
                                            as UPSELL_FLAG,
        /* Retention risk if low engagement and long tenure */
        case when c.ACCT_RATIO < 0.50 and f.TENURE_MONTHS >= 60 then 'Y' else 'N' end
                                            as RETENTION_RISK_FLAG,
        "&MODEL_VERSION."                   as MODEL_VERSION length=20,
        today()                             as EFFECTIVE_DATE format=yymmdd10.,
        datetime()                          as LOAD_TS format=datetime22.3
    from WORK.CUST_CLUSTERED c
    inner join WORK.SEGMENT_LABELS s
        on c.CLUSTER = s.CLUSTER
    inner join WORK.CUST_FEATURES f
        on c.CUSTOMER_ID = f.CUSTOMER_ID
    ;
quit;

%let _seg_rows = &sqlobs.;
%log_step(step=01_CUSTOMER_SEG, status=SUCCESS, msg=Segment table built, rowcount=&_seg_rows.);


/* ========================================================================= */
/* STEP 7: Validate output                                                   */
/* ========================================================================= */
%validate_table(lib=WORK, table=CUSTOMER_SEGMENTS_FINAL,
                key_cols=CUSTOMER_ID,
                not_null=CUSTOMER_ID SEGMENT_NAME SEGMENT_ID,
                min_rows=1000);

%if &VALIDATION_RC. ne 0 %then %do;
    %log_step(step=01_CUSTOMER_SEG, status=ERROR, msg=Validation failed - aborting load);
    %abort cancel;
%end;


/* ========================================================================= */
/* STEP 8: Load to Teradata data product table                               */
/* ========================================================================= */
%log_step(step=01_CUSTOMER_SEG, status=START, msg=Loading DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS);

/* Truncate-and-load pattern */
proc sql;
    connect to teradata (server="&TD_SERVER." user="&TD_USERNAME."
                         password="{SAS004}XXXXXXXXXXXXXXXXXXXXXXXX" logmech=LDAP);

    execute (DELETE FROM DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS) by teradata;
    disconnect from teradata;
quit;

proc append base=DPDB.CUSTOMER_SEGMENTS
            data=WORK.CUSTOMER_SEGMENTS_FINAL
            force;
run;

%log_step(step=01_CUSTOMER_SEG, status=SUCCESS, msg=Pipeline complete, rowcount=&_seg_rows.);

/* Cleanup */
proc datasets library=WORK nolist;
    delete CUST_360 CUST_FEATURES CUST_STANDARDISED CUST_CLUSTERED
           CLUSTER_PROFILES SEGMENT_LABELS CUSTOMER_SEGMENTS_FINAL;
quit;
