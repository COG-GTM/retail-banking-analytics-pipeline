"""PySpark translation of bteq/02_stg_txn_summary.bteq.

Aggregates posted transactions over a configurable lookback window into
per-customer/account summary metrics (volumes, amounts, channel mix,
merchant diversity, recency).
"""
from __future__ import annotations

import datetime as dt

from common import DEFAULT_LOOKBACK_MONTHS, DEFAULT_RUN_DATE
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession


def build_stg_txn_summary(
    spark: SparkSession,
    run_date: dt.date = DEFAULT_RUN_DATE,
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS,
) -> DataFrame:
    """Return STG_TXN_SUMMARY for the [run_date - lookback, run_date] window."""
    # BTEQ used a volatile VT_RUN_PARAMS table; here the window is inlined.
    period_start = run_date - relativedelta(months=lookback_months)
    sql = f"""
    WITH top_cat AS (
        -- Top merchant category per account by total absolute spend in window
        -- (BTEQ: QUALIFY ROW_NUMBER() ordered by windowed SUM(ABS(amount)))
        SELECT account_id, merchant_category
        FROM (
            SELECT account_id, merchant_category,
                   ROW_NUMBER() OVER (
                       PARTITION BY account_id
                       ORDER BY cat_amt DESC, merchant_category) AS rn
            FROM (
                SELECT t2.account_id, t2.merchant_category,
                       SUM(ABS(t2.amount)) AS cat_amt
                FROM transactions t2
                WHERE t2.transaction_date BETWEEN DATE'{period_start}' AND DATE'{run_date}'
                  AND t2.status_code = 'P'
                  AND t2.merchant_category IS NOT NULL
                GROUP BY t2.account_id, t2.merchant_category
            )
        ) WHERE rn = 1
    )
    SELECT
        acct.customer_id,
        acct.account_id,
        acct.account_type,
        DATE'{period_start}'                                        AS summary_period_start,
        DATE'{run_date}'                                            AS summary_period_end,
        -- Volume counts by transaction category
        COUNT(*)                                                    AS txn_count_total,
        SUM(CASE WHEN tt.category = 'DEBIT'  THEN 1 ELSE 0 END)     AS txn_count_debit,
        SUM(CASE WHEN tt.category = 'CREDIT' THEN 1 ELSE 0 END)     AS txn_count_credit,
        SUM(CASE WHEN tt.category = 'FEE'    THEN 1 ELSE 0 END)     AS txn_count_fee,
        -- Dollar amounts (debits/fees use absolute value)
        SUM(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_total_debit,
        SUM(CASE WHEN tt.category = 'CREDIT' THEN t.amount      ELSE 0 END) AS amt_total_credit,
        SUM(CASE WHEN tt.category = 'FEE'    THEN ABS(t.amount) ELSE 0 END) AS amt_total_fees,
        -- Averages (NULL for non-matching rows so AVG only spans the category)
        AVG(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) END)        AS amt_avg_debit,
        AVG(CASE WHEN tt.category = 'CREDIT' THEN t.amount      END)        AS amt_avg_credit,
        -- Maximums
        MAX(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_max_single_debit,
        MAX(CASE WHEN tt.category = 'CREDIT' THEN t.amount      ELSE 0 END) AS amt_max_single_credit,
        -- Merchant diversity
        COUNT(DISTINCT t.merchant_name)                             AS distinct_merchants,
        MAX(tc.merchant_category)                                   AS top_merchant_category,
        -- Channel mix percentages (Teradata NULLIFZERO -> NULLIF(x, 0))
        CAST(SUM(CASE WHEN t.channel_code = 'ATM' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) AS DECIMAL(5,2))                 AS pct_atm,
        CAST(SUM(CASE WHEN t.channel_code = 'POS' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) AS DECIMAL(5,2))                 AS pct_pos,
        CAST(SUM(CASE WHEN t.channel_code = 'WEB' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) AS DECIMAL(5,2))                 AS pct_web,
        CAST(SUM(CASE WHEN t.channel_code = 'MOB' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) AS DECIMAL(5,2))                 AS pct_mobile,
        -- Recency: days since the most recent transaction
        CAST(DATEDIFF(DATE'{run_date}', MAX(t.transaction_date)) AS INT) AS days_since_last_txn,
        current_timestamp() AS load_ts
    FROM transactions t
    INNER JOIN accounts acct          ON t.account_id = acct.account_id
    INNER JOIN transaction_types tt   ON t.transaction_type_cd = tt.transaction_type_cd
    LEFT JOIN top_cat tc              ON t.account_id = tc.account_id
    WHERE t.transaction_date BETWEEN DATE'{period_start}' AND DATE'{run_date}'
      AND t.status_code = 'P'   -- posted transactions only
    GROUP BY acct.customer_id, acct.account_id, acct.account_type, tc.merchant_category
    """
    return spark.sql(sql)
