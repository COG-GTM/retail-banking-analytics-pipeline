{{
    config(
        materialized = 'table',
        alias = 'STG_CUSTOMER_360',
        post_hook = "{{ log_etl_run(job_name='01_stg_customer_360', step_name='FULL_LOAD') }}"
    )
}}

/*
    Snowflake port of bteq/01_stg_customer_360.bteq.

    Denormalized customer-360 staging model joining CUSTOMERS, ACCOUNTS and
    ADDRESSES from the core banking database.

    BTEQ control flow is replaced by dbt semantics:
      - .SET ERRORLEVEL / .IF ERRORCODE <> 0 THEN .EXIT  -> dbt run fails the
        model (and the invocation) on any SQL error.
      - .IF ACTIVITYCOUNT = 0 THEN .EXIT 99              -> singular test
        tests/assert_stg_customer_360_not_empty.sql
      - ETL_RUN_LOG insert                               -> log_etl_run() post-hook
      - COLLECT STATISTICS                               -> dropped (Snowflake
        maintains micro-partition statistics automatically).
*/

with addresses as (

    select
        CUSTOMER_ID,
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        CITY,
        STATE_CODE,
        ZIP_CODE
    from {{ source('core_banking', 'ADDRESSES') }}
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
    from {{ source('core_banking', 'ACCOUNTS') }}
    group by CUSTOMER_ID

)

select
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.DATE_OF_BIRTH,
    /* Teradata: CAST((CURRENT_DATE - DATE_OF_BIRTH) / 365.25 AS SMALLINT) - the
       Teradata CAST rounds, so ROUND() preserves the legacy value. */
    cast(round(datediff('day', c.DATE_OF_BIRTH, current_date()) / 365.25) as smallint) as AGE,
    c.CUSTOMER_SINCE,
    cast(round(months_between(current_date(), c.CUSTOMER_SINCE)) as integer)           as TENURE_MONTHS,
    c.CUSTOMER_STATUS,
    c.SEGMENT_CODE,
    c.BRANCH_ID,
    /* NULL-propagating concatenation matches Teradata's || semantics. */
    trim(a.ADDRESS_LINE_1) || coalesce(', ' || trim(a.ADDRESS_LINE_2), '')             as PRIMARY_ADDRESS,
    a.CITY,
    a.STATE_CODE,
    a.ZIP_CODE,
    acct.NUM_ACCOUNTS,
    acct.NUM_ACTIVE_ACCOUNTS,
    acct.HAS_CHECKING,
    acct.HAS_SAVINGS,
    acct.HAS_CREDIT,
    acct.HAS_LOAN,
    acct.TOTAL_BALANCE,
    acct.TOTAL_CREDIT_LIMIT,
    case
        when acct.TOTAL_CREDIT_LIMIT > 0
        then cast(acct.CREDIT_BALANCE / acct.TOTAL_CREDIT_LIMIT * 100 as number(5, 2))
        else 0.00
    end                                                                                as CREDIT_UTILIZATION_PCT,
    cast(current_timestamp() as timestamp_ntz(6))                                      as LOAD_TS
from {{ source('core_banking', 'CUSTOMERS') }} c
left join addresses a
    on c.CUSTOMER_ID = a.CUSTOMER_ID
left join account_agg acct
    on c.CUSTOMER_ID = acct.CUSTOMER_ID
where c.CUSTOMER_STATUS in ('A', 'I')   /* Exclude closed customers */
