-- Replaces the BTEQ ".IF ACTIVITYCOUNT = 0 THEN .EXIT 99" zero-row guard:
-- an empty STG_TXN_SUMMARY fails the run instead of publishing an empty table.
select 'STG_TXN_SUMMARY' as TABLE_NAME, count(*) as ROW_COUNT
from {{ ref('stg_txn_summary') }}
having count(*) = 0
