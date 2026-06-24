{{
    config(
        materialized='view',
        tags=['staging', 'phase1_bteq']
    )
}}

-- Denormalized customer-360 view joining customer, account and address data.
-- Translated from bteq/01_stg_customer_360.bteq (CREATE TABLE AS SELECT body).
-- ADD_MONTHS / MONTHS_BETWEEN are retained as native Teradata date functions
-- (see dbt/README.md "Dialect notes"). QUALIFY is kept (Teradata-native).

with addr as (

    -- Primary address: most recent, non-expired HOME address per customer.
    select
        customer_id,
        address_line_1,
        address_line_2,
        city,
        state_code,
        zip_code
    from {{ source('core_banking', 'addresses') }}
    where address_type = 'HOME'
      and (expiration_date is null or expiration_date > current_date)
    qualify row_number() over (
        partition by customer_id
        order by effective_date desc
    ) = 1

),

acct_agg as (

    -- Account portfolio metrics per customer.
    select
        customer_id,
        count(*)                                                       as num_accounts,
        sum(case when account_status = 'O' then 1 else 0 end)          as num_active_accounts,
        max(case when account_type = 'CHECKING' then 'Y' else 'N' end) as has_checking,
        max(case when account_type = 'SAVINGS'  then 'Y' else 'N' end) as has_savings,
        max(case when account_type = 'CREDIT'   then 'Y' else 'N' end) as has_credit,
        max(case when account_type = 'LOAN'     then 'Y' else 'N' end) as has_loan,
        sum(coalesce(current_balance, 0))                              as total_balance,
        sum(case when account_type = 'CREDIT'
                 then coalesce(credit_limit, 0) else 0 end)            as total_credit_limit,
        sum(case when account_type = 'CREDIT'
                 then coalesce(current_balance, 0) else 0 end)         as credit_balance
    from {{ source('core_banking', 'accounts') }}
    group by customer_id

)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.date_of_birth,
    -- Derive age from date of birth.
    cast((current_date - c.date_of_birth) / 365.25 as smallint)            as age,
    c.customer_since,
    -- Tenure in months since account opening.
    cast(months_between(current_date, c.customer_since) as integer)        as tenure_months,
    c.customer_status,
    c.segment_code,
    c.branch_id,
    -- Concatenated primary address.
    trim(a.address_line_1) || coalesce(', ' || trim(a.address_line_2), '') as primary_address,
    a.city,
    a.state_code,
    a.zip_code,
    acct_agg.num_accounts,
    acct_agg.num_active_accounts,
    acct_agg.has_checking,
    acct_agg.has_savings,
    acct_agg.has_credit,
    acct_agg.has_loan,
    acct_agg.total_balance,
    acct_agg.total_credit_limit,
    -- Credit utilization = total credit balance / total credit limit.
    case
        when acct_agg.total_credit_limit > 0
        then cast(acct_agg.credit_balance / acct_agg.total_credit_limit * 100 as decimal(5,2))
        else 0.00
    end                                                                    as credit_utilization_pct,
    current_timestamp(6)                                                   as load_ts
from {{ source('core_banking', 'customers') }} c
left join addr a
    on c.customer_id = a.customer_id
left join acct_agg
    on c.customer_id = acct_agg.customer_id
where c.customer_status in ('A', 'I')   -- exclude closed customers
