-- Channel-mix percentages must sum to 100 for every account with transactions,
-- allowing for DECIMAL(5,2) rounding across the five channel buckets.
select
    CUSTOMER_ID,
    ACCOUNT_ID,
    coalesce(PCT_ATM, 0) + coalesce(PCT_POS, 0) + coalesce(PCT_WEB, 0)
        + coalesce(PCT_MOBILE, 0) + coalesce(PCT_OTHER_CHANNEL, 0) as PCT_SUM
from {{ ref('stg_txn_summary') }}
where TXN_COUNT_TOTAL > 0
  and abs(coalesce(PCT_ATM, 0) + coalesce(PCT_POS, 0) + coalesce(PCT_WEB, 0)
          + coalesce(PCT_MOBILE, 0) + coalesce(PCT_OTHER_CHANNEL, 0) - 100) > 0.05
