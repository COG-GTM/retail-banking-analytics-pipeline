-- Replacement for BTEQ ".IF ACTIVITYCOUNT = 0 THEN .EXIT 99".
-- Returns a row (i.e. fails) when the model published zero rows.
select 0 as ROW_COUNT
from (select count(*) as n from {{ ref('stg_customer_360') }})
where n = 0
