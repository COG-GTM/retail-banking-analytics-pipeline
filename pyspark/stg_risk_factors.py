"""PySpark translation of bteq/03_stg_risk_factors.bteq.

Computes the per-customer risk feature vector consumed by the SAS risk
scoring model: overdraft/NSF activity, large withdrawals, balance behaviour,
credit utilization, payment history, bureau score, transaction velocity,
and merchant risk indicators.
"""
from __future__ import annotations

import datetime as dt

from common import DEFAULT_RUN_DATE
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession


def build_stg_risk_factors(
    spark: SparkSession, run_date: dt.date = DEFAULT_RUN_DATE
) -> DataFrame:
    """Return STG_RISK_FACTORS as of ``run_date``."""
    # Window boundaries mirroring the BTEQ ADD_MONTHS / CURRENT_DATE - N logic
    m3 = run_date - relativedelta(months=3)
    m6 = run_date - relativedelta(months=6)
    m12 = run_date - relativedelta(months=12)
    m24 = run_date - relativedelta(months=24)
    d7 = run_date - dt.timedelta(days=7)
    d30 = run_date - dt.timedelta(days=30)
    d90 = run_date - dt.timedelta(days=90)

    sql = f"""
    WITH wrk_daily_balance AS (
        -- Last posted transaction per account per day over trailing 3 months
        -- (BTEQ intermediate table WRK_DAILY_BALANCE)
        SELECT customer_id, account_id, transaction_date, eod_balance
        FROM (
            SELECT acct.customer_id, t.account_id, t.transaction_date,
                   t.running_balance AS eod_balance,
                   ROW_NUMBER() OVER (
                       PARTITION BY t.account_id, t.transaction_date
                       ORDER BY t.transaction_ts DESC) AS rn
            FROM transactions t
            INNER JOIN accounts acct ON t.account_id = acct.account_id
            WHERE t.transaction_date >= DATE'{m3}'
              AND t.status_code = 'P'
        ) WHERE rn = 1
    ),
    wrk_payment_history AS (
        -- Payment behaviour on CREDIT/LOAN accounts over trailing 24 months
        -- (BTEQ intermediate table WRK_PAYMENT_HISTORY); "on time" means the
        -- payment landed within the month following its cycle anchor date.
        SELECT
            acct.customer_id,
            acct.account_id,
            COUNT(*) AS total_payments,
            SUM(CASE WHEN t.transaction_date <=
                     ADD_MONTHS(acct.open_date,
                        CAST(MONTHS_BETWEEN(t.transaction_date, acct.open_date) AS INT) + 1)
                THEN 1 ELSE 0 END) AS ontime_payments,
            SUM(CASE WHEN t.transaction_date >
                     ADD_MONTHS(acct.open_date,
                        CAST(MONTHS_BETWEEN(t.transaction_date, acct.open_date) AS INT) + 1)
                THEN 1 ELSE 0 END) AS late_payments,
            -- Calendar-month boundary difference, matching the estate's
            -- reference output (DuckDB date_diff('month'))
            (YEAR(DATE'{run_date}')
             - YEAR(COALESCE(MAX(CASE WHEN t.transaction_date >
                     ADD_MONTHS(acct.open_date,
                        CAST(MONTHS_BETWEEN(t.transaction_date, acct.open_date) AS INT) + 1)
                     THEN t.transaction_date END), acct.open_date))) * 12
            + (MONTH(DATE'{run_date}')
               - MONTH(COALESCE(MAX(CASE WHEN t.transaction_date >
                     ADD_MONTHS(acct.open_date,
                        CAST(MONTHS_BETWEEN(t.transaction_date, acct.open_date) AS INT) + 1)
                     THEN t.transaction_date END), acct.open_date)))
                AS months_since_last_late
        FROM transactions t
        INNER JOIN accounts acct        ON t.account_id = acct.account_id
        INNER JOIN transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE acct.account_type IN ('CREDIT', 'LOAN')
          AND tt.category = 'CREDIT'         -- payment transactions
          AND t.status_code = 'P'
          AND t.transaction_date >= DATE'{m24}'
        GROUP BY acct.customer_id, acct.account_id, acct.open_date
    ),
    overdraft AS (
        -- Overdraft events and NSF fee totals over trailing 12 months
        SELECT acct.customer_id,
               SUM(CASE WHEN t.running_balance < 0 THEN 1 ELSE 0 END) AS overdraft_count,
               SUM(CASE WHEN tt.category = 'FEE' AND tt.description LIKE '%NSF%'
                        THEN ABS(t.amount) ELSE 0 END) AS nsf_total
        FROM transactions t
        INNER JOIN accounts acct        ON t.account_id = acct.account_id
        INNER JOIN transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE t.transaction_date >= DATE'{m12}' AND t.status_code = 'P'
        GROUP BY acct.customer_id
    ),
    lg_wd AS (
        -- Large single debits (>= $5,000) over trailing 12 months
        SELECT acct.customer_id,
               COUNT(*)           AS large_wd_cnt,
               SUM(ABS(t.amount)) AS large_wd_amt
        FROM transactions t
        INNER JOIN accounts acct        ON t.account_id = acct.account_id
        INNER JOIN transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE tt.category = 'DEBIT'
          AND ABS(t.amount) >= 5000
          AND t.transaction_date >= DATE'{m12}'
          AND t.status_code = 'P'
        GROUP BY acct.customer_id
    ),
    bal AS (
        -- Average daily balances (30/90 day) and volatility (population stddev)
        SELECT customer_id,
               AVG(CASE WHEN transaction_date >= DATE'{d30}' THEN eod_balance END) AS avg_bal_30d,
               AVG(CASE WHEN transaction_date >= DATE'{d90}' THEN eod_balance END) AS avg_bal_90d,
               STDDEV_POP(eod_balance) AS bal_stddev
        FROM wrk_daily_balance
        GROUP BY customer_id
    ),
    credit AS (
        -- Open credit account balances and limits for utilization ratio
        SELECT customer_id,
               SUM(COALESCE(current_balance, 0)) AS total_credit_bal,
               SUM(COALESCE(credit_limit, 0))    AS total_credit_limit
        FROM accounts
        WHERE account_type = 'CREDIT' AND account_status = 'O'
        GROUP BY customer_id
    ),
    pmh AS (
        -- Roll payment history up from account to customer level
        SELECT customer_id,
               SUM(total_payments)         AS total_payments,
               SUM(ontime_payments)        AS ontime_payments,
               SUM(late_payments)          AS late_payments,
               MIN(months_since_last_late) AS months_since_last_late
        FROM wrk_payment_history
        GROUP BY customer_id
    ),
    bureau AS (
        -- Most recent external bureau score per customer
        SELECT customer_id, external_credit_score AS credit_score
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY customer_id ORDER BY report_date DESC) AS rn
            FROM customer_bureau_scores
        ) WHERE rn = 1
    ),
    vel AS (
        -- Debit velocity: rolling 7-day and 30-day debit totals
        SELECT acct.customer_id,
               SUM(CASE WHEN t.transaction_date >= DATE'{d7}'
                        THEN ABS(t.amount) ELSE 0 END) AS debit_7d,
               SUM(CASE WHEN t.transaction_date >= DATE'{d30}'
                        THEN ABS(t.amount) ELSE 0 END) AS debit_30d
        FROM transactions t
        INNER JOIN accounts acct        ON t.account_id = acct.account_id
        INNER JOIN transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE tt.category = 'DEBIT'
          AND t.transaction_date >= DATE'{d30}'
          AND t.status_code = 'P'
        GROUP BY acct.customer_id
    ),
    prior_merchants AS (
        -- Merchants each account used before the 30-day window
        SELECT DISTINCT account_id, merchant_name
        FROM transactions
        WHERE transaction_date < DATE'{d30}' AND merchant_name IS NOT NULL
    ),
    merch AS (
        -- Merchant risk indicators over trailing 6 months
        SELECT acct.customer_id,
               COUNT(DISTINCT CASE
                   WHEN t.transaction_date >= DATE'{d30}'
                    AND pm.merchant_name IS NULL   -- never used before window
                   THEN t.merchant_name END) AS new_merch_30d,
               SUM(CASE WHEN t.channel_code = 'INTL' THEN 1 ELSE 0 END) AS intl_txn_cnt,
               SUM(CASE WHEN t.merchant_category IN
                        ('GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP')
                        THEN 1 ELSE 0 END) AS high_risk_cnt
        FROM transactions t
        INNER JOIN accounts acct ON t.account_id = acct.account_id
        LEFT JOIN prior_merchants pm
               ON t.account_id = pm.account_id AND t.merchant_name = pm.merchant_name
        WHERE t.transaction_date >= DATE'{m6}' AND t.status_code = 'P'
        GROUP BY acct.customer_id
    )
    SELECT
        c.customer_id,
        COALESCE(o.overdraft_count, 0)   AS account_overdraft_cnt,
        COALESCE(o.nsf_total, 0.00)      AS nsf_fee_total,
        COALESCE(w.large_wd_cnt, 0)      AS large_withdrawal_cnt,
        COALESCE(w.large_wd_amt, 0.00)   AS large_withdrawal_amt,
        COALESCE(b.avg_bal_30d, 0.00)    AS avg_daily_balance_30d,
        COALESCE(b.avg_bal_90d, 0.00)    AS avg_daily_balance_90d,
        COALESCE(b.bal_stddev, 0.0)      AS balance_volatility,
        CASE WHEN cr.total_credit_limit > 0
             THEN CAST(cr.total_credit_bal / cr.total_credit_limit AS DECIMAL(5,4))
             ELSE 0.0 END                AS credit_util_ratio,
        CASE WHEN p.total_payments > 0
             THEN CAST(p.ontime_payments * 100.0 / p.total_payments AS DECIMAL(5,2))
             ELSE 100.00 END             AS payment_ontime_pct,
        COALESCE(p.late_payments, 0)     AS payment_late_cnt,
        COALESCE(p.months_since_last_late, 999) AS months_since_last_late,
        COALESCE(bu.credit_score, 0)     AS external_credit_score,
        COALESCE(v.debit_7d, 0.00)       AS debit_velocity_7d,
        COALESCE(v.debit_30d, 0.00)      AS debit_velocity_30d,
        COALESCE(m.new_merch_30d, 0)     AS new_merchant_cnt_30d,
        COALESCE(m.intl_txn_cnt, 0)      AS international_txn_cnt,
        COALESCE(m.high_risk_cnt, 0)     AS high_risk_merchant_cnt,
        current_timestamp()              AS load_ts
    FROM customers c
    LEFT JOIN overdraft o ON c.customer_id = o.customer_id
    LEFT JOIN lg_wd w     ON c.customer_id = w.customer_id
    LEFT JOIN bal b       ON c.customer_id = b.customer_id
    LEFT JOIN credit cr   ON c.customer_id = cr.customer_id
    LEFT JOIN pmh p       ON c.customer_id = p.customer_id
    LEFT JOIN bureau bu   ON c.customer_id = bu.customer_id
    LEFT JOIN vel v       ON c.customer_id = v.customer_id
    LEFT JOIN merch m     ON c.customer_id = m.customer_id
    WHERE c.customer_status IN ('A', 'I')
    """
    return spark.sql(sql)
