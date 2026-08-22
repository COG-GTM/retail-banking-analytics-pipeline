# TICKET-04 (MBA-2205): `02_stg_txn_summary.bteq` -> Snowflake / dbt

Migrated artifact: `dbt/models/staging/stg_txn_summary.sql`, materialized as
`ETL_STAGING.STAGING.STG_TXN_SUMMARY`.

## Construct mapping

| Teradata / BTEQ | Snowflake / dbt |
|---|---|
| `VT_RUN_PARAMS` volatile table | `run_params` CTE driven by `var('lookback_months')` |
| `ADD_MONTHS(CURRENT_DATE, -${LOOKBACK_MONTHS})` | `dateadd(month, -{{ var('lookback_months') }}, current_date())` |
| `LOOKBACK_MONTHS` shell export | dbt var, default 12 in `dbt/dbt_project.yml`, overridable with `--vars` |
| `DROP TABLE` + `CREATE TABLE ... WITH DATA` | `materialized='table'` (dbt replaces atomically) |
| `PRIMARY INDEX (CUSTOMER_ID, ACCOUNT_ID)` | dropped (Snowflake micro-partitions) |
| `COLLECT STATISTICS` | dropped |
| `NULLIFZERO(x)` | `nullif(x, 0)` |
| `CURRENT_DATE - MAX(TRANSACTION_DATE)` | `datediff(day, max(TRANSACTION_DATE), current_date())` |
| `CURRENT_TIMESTAMP(6)` | `current_timestamp()` |
| `.SET ERRORLEVEL` / `.IF ERRORCODE <> 0 THEN .EXIT` | dbt run failure semantics |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | singular test `assert_stg_txn_summary_not_empty` |
| `INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG` | model `post_hook` insert into `ETL_STAGING.STAGING.ETL_RUN_LOG` |

## Behavioural differences

- **Top merchant category**: the BTEQ query joined a per-account "top category"
  sub-select and then included `top_cat.MERCHANT_CATEGORY` in the `GROUP BY`. The
  dbt model resolves the top category to exactly one row per account before the
  join, so the output grain is strictly one row per account.
- **Channel mix**: the BTEQ script emitted `PCT_ATM/POS/WEB/MOBILE` only, which do
  not sum to 100 because `ACH` traffic is unclassified. The model adds `PCT_ACH`
  and `PCT_OTHER` so the mix sums to 100 (within `DECIMAL(5,2)` rounding), covered
  by `assert_stg_txn_summary_channel_mix_totals_100`.
- **Revenue flag**: `TXN_COUNT_REVENUE` and `AMT_TOTAL_REVENUE` are added from
  `TRANSACTION_TYPES.IS_REVENUE`, which the BTEQ version never aggregated.

## Running

```bash
dbt run  --select stg_txn_summary
dbt test --select stg_txn_summary
dbt run  --select stg_txn_summary --vars '{lookback_months: 6}'
```

## Assumptions (owned by other tickets)

- Snowflake databases `CORE_BANKING`, `TXN_PROCESSING`, `ETL_STAGING` with a `RAW`
  schema for source tables and a `STAGING` schema for staging output (TICKET-01),
  overridable through the `*_database` dbt vars.
- `ETL_STAGING.STAGING.ETL_RUN_LOG` exists with the same columns as the Teradata
  table (TICKET-01/TICKET-03).
- Connection profile, role and secret handling come from TICKET-02, so no
  `profiles.yml` is committed here.
