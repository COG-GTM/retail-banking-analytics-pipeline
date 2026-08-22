-- Amount and count metrics are absolute-value or credit aggregates and must never
-- be negative. Any returned row fails the test.
select
    CUSTOMER_ID,
    ACCOUNT_ID
from {{ ref('stg_txn_summary') }}
where TXN_COUNT_TOTAL < 0
   or TXN_COUNT_DEBIT < 0
   or TXN_COUNT_CREDIT < 0
   or TXN_COUNT_FEE < 0
   or TXN_COUNT_REVENUE < 0
   or AMT_TOTAL_DEBIT < 0
   or AMT_TOTAL_CREDIT < 0
   or AMT_TOTAL_FEES < 0
   or AMT_TOTAL_REVENUE < 0
   or AMT_MAX_SINGLE_DEBIT < 0
   or AMT_MAX_SINGLE_CREDIT < 0
   or coalesce(AMT_AVG_DEBIT, 0) < 0
   or coalesce(AMT_AVG_CREDIT, 0) < 0
   or DAYS_SINCE_LAST_TXN < 0
