/*******************************************************************************
 * Adapted from: sas/01_sas_customer_segments.sas
 * Customer segmentation via k-means clustering. Feature engineering, PROC
 * STDIZE standardisation, PROC FASTCLUS (k=5), and PROC SQL cluster profiling
 * are preserved exactly as written upstream. The STGDB.STG_CUSTOMER_360
 * Teradata extract is read from the staging sample materialised in autoexec,
 * and the Teradata pass-through load step is omitted.
 ******************************************************************************/

%let MODEL_VERSION = SEG_V3.2;

/* STEP 1: Active customers from staging */
proc sql;
    create table CUST_360 as
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
    from STG_CUSTOMER_360
    where CUSTOMER_STATUS = 'A'
    ;
quit;

/* STEP 2: Feature engineering for clustering */
data CUST_FEATURES;
    set CUST_360;

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

    /* Numeric features for clustering */
    LOG_BALANCE = log(max(TOTAL_BALANCE, 1));
    ACCT_RATIO  = NUM_ACTIVE_ACCOUNTS / max(NUM_ACCOUNTS, 1);
run;

/* STEP 3: Standardise numeric features for k-means */
proc stdize data=CUST_FEATURES
            out=CUST_STANDARDISED
            method=std;
    var LOG_BALANCE TENURE_MONTHS CREDIT_UTILIZATION_PCT PRODUCT_BREADTH ACCT_RATIO AGE;
run;

/* STEP 4: K-Means clustering (5 segments) */
proc fastclus data=CUST_STANDARDISED
              out=CUST_CLUSTERED
              maxclusters=5
              maxiter=50
              converge=0.001
              replace=full
              least=2;
    var LOG_BALANCE TENURE_MONTHS CREDIT_UTILIZATION_PCT PRODUCT_BREADTH ACCT_RATIO AGE;
    id CUSTOMER_ID;
run;

/* STEP 5: Profile clusters and order by average balance */
proc sql;
    create table CLUSTER_PROFILES as
    select
        CLUSTER,
        count(*)                        as N,
        avg(LOG_BALANCE)                as AVG_BALANCE,
        avg(TENURE_MONTHS)              as AVG_TENURE,
        avg(PRODUCT_BREADTH)            as AVG_BREADTH,
        avg(CREDIT_UTILIZATION_PCT)     as AVG_CREDIT_UTIL
    from CUST_CLUSTERED
    group by CLUSTER
    order by AVG_BALANCE desc
    ;
quit;

/* STEP 6: Label clusters with business-meaningful segment names */
data SEGMENT_LABELS;
    set CLUSTER_PROFILES;
    length SEGMENT_NAME $40;
    if _N_ = 1 then SEGMENT_NAME = 'PREMIUM_WEALTH';
    else if _N_ = 2 then SEGMENT_NAME = 'ENGAGED_MAINSTREAM';
    else if _N_ = 3 then SEGMENT_NAME = 'GROWING_DIGITAL';
    else if _N_ = 4 then SEGMENT_NAME = 'CREDIT_DEPENDENT';
    else SEGMENT_NAME = 'VALUE_BASIC';
    keep CLUSTER SEGMENT_NAME;
run;

title "Customer Segment Profiles - &MODEL_VERSION.";
proc print data=CLUSTER_PROFILES noobs;
run;
title;

title "Segment Labels by Cluster";
proc print data=SEGMENT_LABELS noobs;
run;
title;
