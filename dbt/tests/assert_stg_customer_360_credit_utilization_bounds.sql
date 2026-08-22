-- Credit utilization must be non-negative and 0.00 whenever there is no credit limit.
select
    CUSTOMER_ID,
    TOTAL_CREDIT_LIMIT,
    CREDIT_UTILIZATION_PCT
from {{ ref('stg_customer_360') }}
where CREDIT_UTILIZATION_PCT < 0
   or (coalesce(TOTAL_CREDIT_LIMIT, 0) <= 0 and CREDIT_UTILIZATION_PCT <> 0)
