-- Channel-mix percentages must sum to 100 for every row with transactions,
-- allowing for DECIMAL(5,2) rounding.
select
    CUSTOMER_ID,
    ACCOUNT_ID,
    PCT_ATM + PCT_POS + PCT_WEB + PCT_MOBILE + PCT_ACH + PCT_OTHER as PCT_SUM
from {{ ref('stg_txn_summary') }}
where TXN_COUNT_TOTAL > 0
  and abs(PCT_ATM + PCT_POS + PCT_WEB + PCT_MOBILE + PCT_ACH + PCT_OTHER - 100) > 0.05
