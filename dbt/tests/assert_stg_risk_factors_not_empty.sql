-- Replacement for the BTEQ ".IF ACTIVITYCOUNT = 0 THEN .EXIT 99" zero-row guard.
-- Returns a row (i.e. fails the run) when STG_RISK_FACTORS was published empty.
select
    'STG_RISK_FACTORS' as TABLE_NAME,
    count(*)           as ROW_COUNT
from {{ ref('stg_risk_factors') }}
having count(*) = 0
