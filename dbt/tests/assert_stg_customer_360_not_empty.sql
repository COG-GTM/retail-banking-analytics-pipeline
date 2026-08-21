-- Replacement for the BTEQ ".IF ACTIVITYCOUNT = 0 THEN .EXIT 99" zero-row guard.
-- Returns a row (i.e. fails) when STG_CUSTOMER_360 was published empty.
select
    'STG_CUSTOMER_360' as TABLE_NAME,
    count(*)           as ROW_COUNT
from {{ ref('stg_customer_360') }}
having count(*) = 0
