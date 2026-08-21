# TICKET-04 (MBA-2205): `02_stg_txn_summary.bteq` -> Snowflake SQL / dbt

Snowflake replacement for `bteq/02_stg_txn_summary.bteq`. The Teradata assets are
untouched and remain runnable against the legacy platform.

| Legacy (Teradata BTEQ) | Snowflake / dbt |
|---|---|
| `CREATE VOLATILE TABLE VT_RUN_PARAMS` with `${LOOKBACK_MONTHS}` shell substitution | `run_params` CTE driven by the `lookback_months` dbt var (`dbt run --vars '{lookback_months: 24}'`) |
| `ADD_MONTHS(CURRENT_DATE, -n)` | `DATEADD(month, -n, CURRENT_DATE)` |
| `DROP TABLE` + `CREATE MULTISET TABLE ... AS ... WITH DATA` | `materialized='table'` dbt model |
| `PRIMARY INDEX (CUSTOMER_ID, ACCOUNT_ID)` | `cluster_by=['CUSTOMER_ID', 'ACCOUNT_ID']` |
| `COLLECT STATISTICS` | not required (Snowflake maintains statistics automatically) |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` |
| `CURRENT_DATE - MAX(TRANSACTION_DATE)` | `DATEDIFF(day, MAX(TRANSACTION_DATE), PERIOD_END)` |
| `CURRENT_TIMESTAMP(6)` | `CURRENT_TIMESTAMP()` |
| `DECIMAL(18,2)` / `DECIMAL(5,2)` | `NUMBER(18,2)` / `NUMBER(5,2)` |
| `CASE WHEN ... THEN 1 ELSE 0 END` | `IFF(...)` |
| `.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE` | dbt fails the run on any SQL error |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | singular test `assert_stg_txn_summary_not_empty` |
| `INSERT INTO ETL_RUN_LOG ...` | `log_etl_run('02_stg_txn_summary')` macro run as a model `post_hook` |

## Behavioural changes

* **Grain fix.** The BTEQ query joined the "top merchant category" subquery on
  `ACCOUNT_ID` and then grouped by `top_cat.MERCHANT_CATEGORY`, which could emit
  more than one row per account. The dbt model resolves the top category to
  exactly one row per account (ties broken alphabetically) before joining, so
  the grain is one row per account, as the target DDL's primary index implies.
* **`PCT_OTHER_CHANNEL` added.** `CHANNEL_CODE` also carries `ACH` and `WIRE`, so
  the four legacy percentages did not sum to 100. The extra bucket makes the mix
  total 100 for every account, which is what the acceptance criteria require.
* **Revenue-flag aggregation added.** `TXN_COUNT_REVENUE` / `AMT_TOTAL_REVENUE`
  aggregate `TRANSACTION_TYPES.IS_REVENUE`.

## Parameters

| Var | Default | Purpose |
|---|---|---|
| `lookback_months` | 12 | Size of the summary window (mirrors `LOOKBACK_MONTHS` in `config/pipeline_config.cfg`) |
| `etl_staging_schema` | `ETL_STAGING` | Schema holding `ETL_RUN_LOG` |
| `txn_posted_status_code` | `P` | Posted-transaction filter |

## Validation

* `dbt build --select stg_txn_summary` runs the model, its column tests
  (grain uniqueness, not-null) and the three singular tests: not-empty,
  channel mix sums to 100, amount/count metrics non-negative.
* `analyses/recon_stg_txn_summary.sql` compares row counts and per-column sums
  against the legacy Teradata output landed as `LEGACY_STG_TXN_SUMMARY`.
