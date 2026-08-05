"""PySpark translation of bteq/01_stg_customer_360.bteq.

Builds the denormalized customer-360 staging table by joining customers,
their most recent active HOME address, and aggregated account metrics.
"""
from __future__ import annotations

import datetime as dt

from common import DEFAULT_RUN_DATE
from pyspark.sql import DataFrame, SparkSession


def build_stg_customer_360(
    spark: SparkSession, run_date: dt.date = DEFAULT_RUN_DATE
) -> DataFrame:
    """Return STG_CUSTOMER_360 as of ``run_date`` (Teradata CURRENT_DATE)."""
    # Spark SQL mirrors the BTEQ CREATE TABLE ... AS SELECT statement.
    sql = f"""
    WITH primary_address AS (
        -- Most recent, non-expired HOME address per customer
        -- (BTEQ: QUALIFY ROW_NUMBER() OVER (... ORDER BY EFFECTIVE_DATE DESC) = 1)
        SELECT customer_id, address_line_1, address_line_2,
               city, state_code, zip_code
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY customer_id
                       ORDER BY effective_date DESC) AS rn
            FROM addresses
            WHERE address_type = 'HOME'
              AND (expiration_date IS NULL OR expiration_date > DATE'{run_date}')
        ) WHERE rn = 1
    ),
    acct_agg AS (
        -- Account portfolio metrics per customer
        SELECT
            customer_id,
            COUNT(*)                                                       AS num_accounts,
            SUM(CASE WHEN account_status = 'O' THEN 1 ELSE 0 END)          AS num_active_accounts,
            MAX(CASE WHEN account_type = 'CHECKING' THEN 'Y' ELSE 'N' END) AS has_checking,
            MAX(CASE WHEN account_type = 'SAVINGS'  THEN 'Y' ELSE 'N' END) AS has_savings,
            MAX(CASE WHEN account_type = 'CREDIT'   THEN 'Y' ELSE 'N' END) AS has_credit,
            MAX(CASE WHEN account_type = 'LOAN'     THEN 'Y' ELSE 'N' END) AS has_loan,
            SUM(COALESCE(current_balance, 0))                              AS total_balance,
            SUM(CASE WHEN account_type = 'CREDIT'
                     THEN COALESCE(credit_limit, 0) ELSE 0 END)            AS total_credit_limit,
            SUM(CASE WHEN account_type = 'CREDIT'
                     THEN COALESCE(current_balance, 0) ELSE 0 END)         AS credit_balance
        FROM accounts
        GROUP BY customer_id
    )
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.date_of_birth,
        -- Age from date of birth: calendar-year boundary difference,
        -- matching the estate's reference output (DuckDB date_diff('year'))
        CAST(YEAR(DATE'{run_date}') - YEAR(c.date_of_birth) AS SMALLINT) AS age,
        c.customer_since,
        -- Tenure: calendar-month boundary difference, matching the estate's
        -- reference output (DuckDB date_diff('month'))
        CAST((YEAR(DATE'{run_date}') - YEAR(c.customer_since)) * 12
             + (MONTH(DATE'{run_date}') - MONTH(c.customer_since)) AS INT) AS tenure_months,
        c.customer_status,
        c.segment_code,
        c.branch_id,
        -- Concatenated primary address (line 2 appended only when present)
        CONCAT(TRIM(a.address_line_1),
               COALESCE(CONCAT(', ', TRIM(a.address_line_2)), '')) AS primary_address,
        a.city,
        a.state_code,
        a.zip_code,
        g.num_accounts,
        g.num_active_accounts,
        g.has_checking,
        g.has_savings,
        g.has_credit,
        g.has_loan,
        g.total_balance,
        g.total_credit_limit,
        -- Credit utilization = credit balance / credit limit (percent, 2dp)
        CASE WHEN g.total_credit_limit > 0
             THEN CAST(g.credit_balance / g.total_credit_limit * 100 AS DECIMAL(5,2))
             ELSE 0.00
        END AS credit_utilization_pct,
        current_timestamp() AS load_ts
    FROM customers c
    LEFT JOIN primary_address a ON c.customer_id = a.customer_id
    LEFT JOIN acct_agg g        ON c.customer_id = g.customer_id
    WHERE c.customer_status IN ('A', 'I')   -- exclude closed customers
    """
    return spark.sql(sql)
