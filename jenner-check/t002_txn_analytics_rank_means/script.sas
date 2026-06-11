/*******************************************************************************
 * Adapted from: sas/02_sas_txn_analytics.sas
 * Transaction analytics: aggregate account-level staging to customer level,
 * derive a spend trend, percentile-rank spend with PROC RANK, and flag
 * anomalies with the PROC MEANS IQR method. The aggregation, ranking, and
 * IQR logic are preserved exactly as written upstream. The STGDB.STG_TXN_SUMMARY
 * extract is read from the staging sample materialised in autoexec, and the
 * Teradata pass-through load step is omitted.
 ******************************************************************************/

%let MODEL_VERSION = TXN_V2.1;

/* STEP 1: Aggregate account-level to customer-level */
proc sql;
    create table CUST_TXN as
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
        max(TOP_MERCHANT_CATEGORY)      as TOP_SPEND_CATEGORY,
        case when sum(TXN_COUNT_TOTAL) > 0
             then (sum(TXN_COUNT_TOTAL * (PCT_WEB + PCT_MOBILE) / 100))
                  / sum(TXN_COUNT_TOTAL) * 100
             else 0 end                 as DIGITAL_TXN_PCT
    from STG_TXN_SUMMARY
    group by CUSTOMER_ID
    ;
quit;

/* STEP 2: Spend trend based on net cash flow direction */
data CUST_TXN_TREND;
    set CUST_TXN;

    length MONTHLY_SPEND_TREND $10;
    if NET_CASH_FLOW > AVG_TRANSACTION_SIZE * 5 then MONTHLY_SPEND_TREND = 'UP';
    else if NET_CASH_FLOW < -AVG_TRANSACTION_SIZE * 5 then MONTHLY_SPEND_TREND = 'DOWN';
    else MONTHLY_SPEND_TREND = 'STABLE';

    FEE_INCOME      = TOTAL_FEES;
    INTEREST_INCOME = TOTAL_DEBIT_AMT * 0.02;
    REVENUE_CONTRIBUTION = FEE_INCOME + INTEREST_INCOME;

    ANOMALY_FLAG = 'N';
run;

/* STEP 3: Percentile ranking for spend */
proc rank data=CUST_TXN_TREND
          out=CUST_TXN_RANKED
          groups=100;
    var TOTAL_DEBIT_AMT;
    ranks SPEND_PERCENTILE;
run;

/* STEP 4: Flag anomalies using PROC MEANS (IQR method) */
proc means data=CUST_TXN_RANKED noprint;
    var TOTAL_DEBIT_AMT;
    output out=_TXN_STATS
        median=_MEDIAN
        qrange=_IQR;
run;

data TXN_ANALYTICS_FINAL;
    if _N_ = 1 then set _TXN_STATS(keep=_MEDIAN _IQR);

    set CUST_TXN_RANKED;

    /* Flag customers whose spend exceeds median + 3*IQR */
    if TOTAL_DEBIT_AMT > _MEDIAN + (3 * _IQR) and _IQR > 0 then ANOMALY_FLAG = 'Y';

    length MODEL_VERSION $20;
    MODEL_VERSION = "&MODEL_VERSION.";

    format TOTAL_DEBIT_AMT TOTAL_CREDIT_AMT NET_CASH_FLOW
           AVG_TRANSACTION_SIZE FEE_INCOME INTEREST_INCOME
           REVENUE_CONTRIBUTION comma18.2;

    drop _MEDIAN _IQR _TYPE_ _FREQ_;
run;

title "Transaction Analytics - Spend Percentile & Anomaly Flags (&MODEL_VERSION.)";
proc print data=TXN_ANALYTICS_FINAL(obs=20) noobs;
    var CUSTOMER_ID TOTAL_TRANSACTIONS TOTAL_DEBIT_AMT NET_CASH_FLOW
        MONTHLY_SPEND_TREND SPEND_PERCENTILE ANOMALY_FLAG;
run;
title;

title "Anomaly Summary";
proc freq data=TXN_ANALYTICS_FINAL;
    tables ANOMALY_FLAG MONTHLY_SPEND_TREND;
run;
title;
