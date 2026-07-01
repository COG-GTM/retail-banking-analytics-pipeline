-- =============================================================================
-- stg_txn_summary
-- Aggregates transaction-level data into per-customer/account summary metrics
-- over a configurable lookback window (var: lookback_months).
-- Ported from bteq/02_stg_txn_summary.bteq (Teradata BTEQ -> Spark SQL / Delta).
-- Consumed by: sas/02_sas_txn_analytics.sas (reads the Delta table).
-- =============================================================================

with run_params as (
    -- Replaces the VT_RUN_PARAMS volatile table.
    select
        add_months(current_date(), -{{ var('lookback_months') }}) as period_start,
        current_date()                                            as period_end
),

top_merchant_category as (
    -- Top merchant category per account by total absolute spend in the window.
    select
        account_id,
        merchant_category
    from (
        select
            t2.account_id,
            t2.merchant_category,
            sum(abs(t2.amount)) as category_spend
        from {{ source('txn_processing', 'transactions') }} t2
        cross join run_params rp2
        where t2.transaction_date between rp2.period_start and rp2.period_end
          and t2.status_code = 'P'
          and t2.merchant_category is not null
        group by t2.account_id, t2.merchant_category
    ) category_totals
    qualify row_number() over (
        partition by account_id
        order by category_spend desc
    ) = 1
)

select
    acct.customer_id,
    acct.account_id,
    acct.account_type,
    rp.period_start                                             as summary_period_start,
    rp.period_end                                               as summary_period_end,
    -- ---- Volume Counts ----
    count(*)                                                    as txn_count_total,
    sum(case when tt.category = 'DEBIT'  then 1 else 0 end)     as txn_count_debit,
    sum(case when tt.category = 'CREDIT' then 1 else 0 end)     as txn_count_credit,
    sum(case when tt.category = 'FEE'    then 1 else 0 end)     as txn_count_fee,
    -- ---- Dollar Amounts ----
    sum(case when tt.category = 'DEBIT'
             then abs(t.amount) else 0 end)                     as amt_total_debit,
    sum(case when tt.category = 'CREDIT'
             then t.amount else 0 end)                          as amt_total_credit,
    sum(case when tt.category = 'FEE'
             then abs(t.amount) else 0 end)                     as amt_total_fees,
    -- ---- Averages ----
    avg(case when tt.category = 'DEBIT'
             then abs(t.amount) else null end)                  as amt_avg_debit,
    avg(case when tt.category = 'CREDIT'
             then t.amount else null end)                       as amt_avg_credit,
    -- ---- Maximums ----
    max(case when tt.category = 'DEBIT'
             then abs(t.amount) else 0 end)                     as amt_max_single_debit,
    max(case when tt.category = 'CREDIT'
             then t.amount else 0 end)                          as amt_max_single_credit,
    -- ---- Merchant Diversity ----
    count(distinct t.merchant_name)                             as distinct_merchants,
    -- ---- Top merchant category (by spend) ----
    max(top_cat.merchant_category)                              as top_merchant_category,
    -- ---- Channel Mix ----
    cast(sum(case when t.channel_code = 'ATM' then 1 else 0 end) * 100.0
         / nullif(count(*), 0) as decimal(5, 2))                as pct_atm,
    cast(sum(case when t.channel_code = 'POS' then 1 else 0 end) * 100.0
         / nullif(count(*), 0) as decimal(5, 2))                as pct_pos,
    cast(sum(case when t.channel_code = 'WEB' then 1 else 0 end) * 100.0
         / nullif(count(*), 0) as decimal(5, 2))                as pct_web,
    cast(sum(case when t.channel_code in ('MOB') then 1 else 0 end) * 100.0
         / nullif(count(*), 0) as decimal(5, 2))                as pct_mobile,
    -- ---- Recency ----
    cast(datediff(current_date(), max(t.transaction_date)) as int) as days_since_last_txn,
    current_timestamp()                                         as load_ts
from {{ source('txn_processing', 'transactions') }} t
inner join {{ source('core_banking', 'accounts') }} acct
    on t.account_id = acct.account_id
inner join {{ source('txn_processing', 'transaction_types') }} tt
    on t.transaction_type_cd = tt.transaction_type_cd
cross join run_params rp
left join top_merchant_category top_cat
    on t.account_id = top_cat.account_id
where t.transaction_date between rp.period_start and rp.period_end
  and t.status_code = 'P'   -- Posted transactions only.
group by
    acct.customer_id,
    acct.account_id,
    acct.account_type,
    rp.period_start,
    rp.period_end,
    top_cat.merchant_category
