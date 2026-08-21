-- Replacement for BTEQ `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99`:
-- a zero-row build fails the run (and is recorded as ZERO_ROWS in ETL_RUN_LOG
-- by the model post-hook).
select 'STG_TXN_SUMMARY' as TABLE_NAME, count(*) as ROW_COUNT
from {{ ref('stg_txn_summary') }}
having count(*) = 0
