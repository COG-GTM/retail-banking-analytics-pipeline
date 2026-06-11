/*******************************************************************************
 * Adapted from: sas/04_sas_data_products.sas
 * Golden-record assembly: sort all four components by CUSTOMER_ID, then do a
 * 4-way DATA-step MERGE with IN= variables, defaulting missing segment /
 * transaction / risk fields, and report segment + risk-tier distribution and a
 * completeness check via PROC SQL. The sort, merge, IN= default handling, and
 * completeness reporting are preserved exactly as written upstream. The four
 * inputs are materialised from the staging + data-product samples in autoexec,
 * and the Teradata pass-through load step is omitted.
 ******************************************************************************/

%let MODEL_VERSION = MASTER_V1.5;

/* STEP 2: Merge all components into the golden record */

/* Sort all datasets by key for merge */
proc sort data=BASE;       by CUSTOMER_ID; run;
proc sort data=SEGMENTS;   by CUSTOMER_ID; run;
proc sort data=TXN;        by CUSTOMER_ID; run;
proc sort data=RISK;       by CUSTOMER_ID; run;

data MASTER_PROFILE;
    merge
        BASE       (in=_base)
        SEGMENTS   (in=_seg)
        TXN        (in=_txn)
        RISK       (in=_risk)
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
run;

title "Master Profile - Golden Record Sample (&MODEL_VERSION.)";
proc print data=MASTER_PROFILE(obs=15) noobs;
    var CUSTOMER_ID FULL_NAME SEGMENT_NAME RISK_TIER
        MONTHLY_TRANSACTIONS COMPOSITE_RISK_SCORE WATCH_LIST_FLAG;
run;
title;

/* STEP 3: Data quality report */
proc sql;
    title "Master Profile - Segment Distribution";
    select SEGMENT_NAME, count(*) as N, round(mean(LIFETIME_VALUE_SCORE), 0.01) as AVG_LTV
    from MASTER_PROFILE
    group by SEGMENT_NAME
    order by N desc;

    title "Master Profile - Risk Tier Distribution";
    select RISK_TIER, count(*) as N, round(mean(COMPOSITE_RISK_SCORE), 0.01) as AVG_SCORE
    from MASTER_PROFILE
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
    from MASTER_PROFILE;
    title;
quit;
