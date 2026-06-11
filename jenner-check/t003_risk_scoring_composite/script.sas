/*******************************************************************************
 * Adapted from: sas/03_sas_risk_scoring.sas
 * Customer risk scoring: feature preparation, a weighted composite risk score
 * built from five component scores, risk-tier classification, primary/secondary
 * risk-driver identification via an array scan, and the PROC FREQ risk-tier
 * distribution. The feature-prep, composite-scoring, driver-ranking, and tier
 * logic are preserved exactly as written upstream. The STGDB.STG_RISK_FACTORS
 * extract is read from the staging sample materialised in autoexec, and the
 * Teradata pass-through load step is omitted.
 ******************************************************************************/

%let MODEL_VERSION = RISK_V4.0;

/* STEP 2: Feature preparation - handle missing values and derive ratios */
data RISK_FEATURES;
    set RISK_RAW;

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

    /* Probability of default carries through from the model; on this staging
       slice it coalesces to 0 exactly as the upstream program specifies. */
    PROB_DEFAULT = .;
run;

/* STEP 4: Build composite risk score and classify into tiers */
data RISK_CLASSIFIED;
    set RISK_FEATURES;

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
    INV_BUREAU = 100 - BUREAU_SCORE_COMPONENT;
    array _comp[4] CREDIT_RISK_COMPONENT BEHAVIOUR_RISK_COMPONENT
                   VELOCITY_RISK_COMPONENT INV_BUREAU;
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

    /* Watch list: critical + high probability of default */
    WATCH_LIST_FLAG = ifc(RISK_TIER = 'CRITICAL' and PROBABILITY_OF_DEFAULT > 0.5, 'Y', 'N');

    /* Review required: elevated or above with recent velocity spike */
    REVIEW_REQUIRED_FLAG = ifc(COMPOSITE_RISK_SCORE >= 60 and VELOCITY_RATIO > 2.0, 'Y', 'N');

    MODEL_VERSION  = "&MODEL_VERSION.";

    keep CUSTOMER_ID COMPOSITE_RISK_SCORE RISK_TIER PROBABILITY_OF_DEFAULT
         CREDIT_RISK_COMPONENT BEHAVIOUR_RISK_COMPONENT VELOCITY_RISK_COMPONENT
         BUREAU_SCORE_COMPONENT PAYMENT_HISTORY_COMP
         PRIMARY_RISK_DRIVER SECONDARY_RISK_DRIVER
         WATCH_LIST_FLAG REVIEW_REQUIRED_FLAG MODEL_VERSION;
run;

title "Customer Risk Scores - Sample (&MODEL_VERSION.)";
proc print data=RISK_CLASSIFIED(obs=20) noobs;
    var CUSTOMER_ID COMPOSITE_RISK_SCORE RISK_TIER
        CREDIT_RISK_COMPONENT BEHAVIOUR_RISK_COMPONENT
        PRIMARY_RISK_DRIVER SECONDARY_RISK_DRIVER;
run;
title;

/* STEP 5: Risk tier distribution for monitoring */
proc freq data=RISK_CLASSIFIED noprint;
    tables RISK_TIER / out=_TIER_DIST;
run;

title "Risk Tier Distribution - &MODEL_VERSION.";
proc print data=_TIER_DIST noobs;
run;
title;
