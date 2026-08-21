-- Amount and count metrics derived from ABS()/COUNT() must never be negative.
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
   or AMT_TOTAL_FEES < 0
   or AMT_TOTAL_REVENUE < 0
   or coalesce(AMT_AVG_DEBIT, 0) < 0
   or AMT_MAX_SINGLE_DEBIT < 0
   or DISTINCT_MERCHANTS < 0
