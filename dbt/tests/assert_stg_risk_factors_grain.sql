-- Replacement for the BTEQ ".IF ACTIVITYCOUNT = 0 THEN .EXIT 99" check plus a
-- grain assertion: the model must cover every eligible customer exactly once.
-- Returns rows (i.e. fails) when the model is empty or when its row count does
-- not equal the number of customers with CUSTOMER_STATUS in ('A','I').

with model_rows as (
    select count(*) as row_count from {{ ref('stg_risk_factors') }}
),

expected_rows as (
    select count(*) as row_count
    from {{ source('core_banking', 'customers') }}
    where customer_status in ('A', 'I')
)

select
    model_rows.row_count as actual_row_count,
    expected_rows.row_count as expected_row_count
from model_rows
cross join expected_rows
where model_rows.row_count = 0
   or model_rows.row_count <> expected_rows.row_count
