{{
    config(
        alias='STG_CUSTOMER_360',
        database=var('staging_database'),
        schema=var('staging_schema'),
        post_hook=[
            "{{ assert_model_not_empty() }}",
            "{{ log_pipeline_run('01_stg_customer_360', 'FULL_LOAD') }}"
        ]
    )
}}

with primary_address as (

    select
        CUSTOMER_ID,
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        CITY,
        STATE_CODE,
        ZIP_CODE
    from {{ source('core_banking', 'addresses') }}
    where ADDRESS_TYPE = 'HOME'
      and (EXPIRATION_DATE is null or EXPIRATION_DATE > current_date())
    qualify row_number() over (
        partition by CUSTOMER_ID
        order by EFFECTIVE_DATE desc
    ) = 1

),

account_agg as (

    select
        CUSTOMER_ID,
        count(*)                                                        as NUM_ACCOUNTS,
        sum(case when ACCOUNT_STATUS = 'O' then 1 else 0 end)           as NUM_ACTIVE_ACCOUNTS,
        max(case when ACCOUNT_TYPE = 'CHECKING' then 'Y' else 'N' end)  as HAS_CHECKING,
        max(case when ACCOUNT_TYPE = 'SAVINGS'  then 'Y' else 'N' end)  as HAS_SAVINGS,
        max(case when ACCOUNT_TYPE = 'CREDIT'   then 'Y' else 'N' end)  as HAS_CREDIT,
        max(case when ACCOUNT_TYPE = 'LOAN'     then 'Y' else 'N' end)  as HAS_LOAN,
        sum(coalesce(CURRENT_BALANCE, 0))                               as TOTAL_BALANCE,
        sum(case when ACCOUNT_TYPE = 'CREDIT'
                 then coalesce(CREDIT_LIMIT, 0)
                 else 0 end)                                            as TOTAL_CREDIT_LIMIT,
        sum(case when ACCOUNT_TYPE = 'CREDIT'
                 then coalesce(CURRENT_BALANCE, 0)
                 else 0 end)                                            as CREDIT_BALANCE
    from {{ source('core_banking', 'accounts') }}
    group by CUSTOMER_ID

)

select
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.DATE_OF_BIRTH,
    -- Age in whole years; Teradata derived this as (CURRENT_DATE - DOB) / 365.25.
    cast(datediff(day, c.DATE_OF_BIRTH, current_date()) / 365.25 as number(5, 0))   as AGE,
    c.CUSTOMER_SINCE,
    cast(months_between(current_date(), c.CUSTOMER_SINCE) as number(9, 0))          as TENURE_MONTHS,
    c.CUSTOMER_STATUS,
    c.SEGMENT_CODE,
    c.BRANCH_ID,
    trim(a.ADDRESS_LINE_1)
        || coalesce(', ' || trim(a.ADDRESS_LINE_2), '')                             as PRIMARY_ADDRESS,
    a.CITY,
    a.STATE_CODE,
    a.ZIP_CODE,
    acct_agg.NUM_ACCOUNTS,
    acct_agg.NUM_ACTIVE_ACCOUNTS,
    acct_agg.HAS_CHECKING,
    acct_agg.HAS_SAVINGS,
    acct_agg.HAS_CREDIT,
    acct_agg.HAS_LOAN,
    acct_agg.TOTAL_BALANCE,
    acct_agg.TOTAL_CREDIT_LIMIT,
    case
        when acct_agg.TOTAL_CREDIT_LIMIT > 0
        then cast(acct_agg.CREDIT_BALANCE / acct_agg.TOTAL_CREDIT_LIMIT * 100 as number(5, 2))
        else 0.00
    end                                                                             as CREDIT_UTILIZATION_PCT,
    cast(current_timestamp() as timestamp_ntz(6))                                   as LOAD_TS
from {{ source('core_banking', 'customers') }} c
left join primary_address a
    on c.CUSTOMER_ID = a.CUSTOMER_ID
left join account_agg acct_agg
    on c.CUSTOMER_ID = acct_agg.CUSTOMER_ID
where c.CUSTOMER_STATUS in ('A', 'I')   -- Exclude closed customers
