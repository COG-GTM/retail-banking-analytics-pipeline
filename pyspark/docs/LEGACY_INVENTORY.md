# Legacy Source Inventory (Phase 1)

Every file below was read end to end before any code was written. `local/duckdb/run_demo.py`
(referenced by `export_data.py`) does **not** exist in the repository and was **not** used as a
logic source. The committed CSVs under `data/02_bteq_staging/` and `data/03_sas_data_products/`
were produced by that missing engine (see `export_data.py`), so they are treated as
*non-authoritative reference outputs*, not as the specification. The specification is the
BTEQ/SAS/DDL text.

## 1. Job → module mapping

| # | Legacy job | PySpark module | Inputs | Output table | DDL contract |
|---|-----------|----------------|--------|--------------|--------------|
| 1 | `bteq/01_stg_customer_360.bteq` | `pyspark/jobs/stg_customer_360.py` | `CORE_BANKING_DB.CUSTOMERS`, `ACCOUNTS`, `ADDRESSES` | `ETL_STAGING_DB.STG_CUSTOMER_360` | `ddl/01_staging_tables.sql` |
| 2 | `bteq/02_stg_txn_summary.bteq` | `pyspark/jobs/stg_txn_summary.py` | `TXN_PROCESSING_DB.TRANSACTIONS`, `TRANSACTION_TYPES`, `CORE_BANKING_DB.ACCOUNTS` | `ETL_STAGING_DB.STG_TXN_SUMMARY` | `ddl/01_staging_tables.sql` |
| 3 | `bteq/03_stg_risk_factors.bteq` | `pyspark/jobs/stg_risk_factors.py` | `CUSTOMERS`, `ACCOUNTS`, `CUSTOMER_BUREAU_SCORES`, `TRANSACTIONS`, `TRANSACTION_TYPES` | `ETL_STAGING_DB.STG_RISK_FACTORS` | `ddl/01_staging_tables.sql` |
| 4 | `sas/01_sas_customer_segments.sas` | `pyspark/jobs/sas_customer_segments.py` | `STG_CUSTOMER_360` | `DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS` | `ddl/02_data_product_tables.sql` |
| 5 | `sas/02_sas_txn_analytics.sas` | `pyspark/jobs/sas_txn_analytics.py` | `STG_TXN_SUMMARY` | `DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS` | `ddl/02_data_product_tables.sql` |
| 6 | `sas/03_sas_risk_scoring.sas` | `pyspark/jobs/sas_risk_scoring.py` | `STG_RISK_FACTORS`, `STG_CUSTOMER_360` | `DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES` | `ddl/02_data_product_tables.sql` |
| 7 | `sas/04_sas_data_products.sas` | `pyspark/jobs/sas_data_products.py` | `STG_CUSTOMER_360`, `CUSTOMER_SEGMENTS`, `TRANSACTION_ANALYTICS`, `CUSTOMER_RISK_SCORES` | `DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE` | `ddl/02_data_product_tables.sql` |

Shared macros → reusable modules:

| Legacy macro | PySpark module |
|---|---|
| `sas/macros/connect_teradata.sas` | `pyspark/common/io.py` (`DataIO` + local/JDBC/in-memory implementations) |
| `sas/macros/log_step.sas` (+ `%init_audit`) | `pyspark/common/audit.py` (`AuditLog`, `PIPELINE_AUDIT`, `ETL_RUN_LOG`) |
| `sas/macros/validate_table.sas` | `pyspark/common/validation.py` (`validate_table`, `abort_on_failure`) |
| `config/pipeline_config.cfg` | `pyspark/common/config.py` (`PipelineConfig.from_cfg_file`) |
| `ddl/*.sql` | `pyspark/common/schemas.py` (`TableSpec`, `enforce_schema`, `assert_schema`) |

## 2. Dependency graph and failure semantics

Extracted from `orchestration/run_full_pipeline.sh`, `bteq/run_bteq_pipeline.sh`,
`sas/run_sas_pipeline.sh`.

```
Phase 1 (BTEQ, strictly sequential)
  01_stg_customer_360 -> 02_stg_txn_summary -> 03_stg_risk_factors
        |
        v  (BTEQ phase failure blocks the whole SAS phase)
Phase 2 (SAS, strictly sequential)
  01_sas_customer_segments -> 02_sas_txn_analytics -> 03_sas_risk_scoring -> 04_sas_data_products
        |
        v
Phase 3 post-validation: row counts of the 4 data product tables
```

Data-level dependencies (a superset of the shell ordering is *not* used; the shell ordering is
authoritative and reproduced exactly):

* `sas_customer_segments`, `sas_risk_scoring`, `sas_data_products` read `STG_CUSTOMER_360`.
* `sas_txn_analytics` reads `STG_TXN_SUMMARY`; `sas_risk_scoring` reads `STG_RISK_FACTORS`.
* `sas_data_products` reads all three data products plus `STG_CUSTOMER_360`.

Failure semantics to preserve:

| Legacy construct | Meaning | Port |
|---|---|---|
| `.SET ERRORLEVEL 3807 SEVERITY 0` + `.IF ERRORCODE <> 0 THEN .GOTO ...` around `DROP TABLE` | "table does not exist" on drop is not an error | overwrite-mode writes; a missing target is not an error |
| `.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE` after each DML | abort the job on any statement failure | exceptions propagate out of `run()`; the driver aborts the DAG |
| `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` | zero-row guard | `validation.min_rows` check; `abort_on_failure` raises |
| shell `set -euo pipefail`, BTEQ rc != 0 | any BTEQ step failure stops the phase **and** the SAS phase | driver stops at the first failed job; downstream jobs are reported as `SKIPPED` |
| SAS rc >= 2 = error (rc 1 = warnings, tolerated) | only rc>=2 aborts | `JobResult.status` WARNING vs FAILED; only FAILED aborts |
| `%abort cancel` after `%validate_table` failure | validation failure aborts before load | `abort_on_failure()` raises before the write |
| `ETL_RUN_LOG` insert at end of each BTEQ job | audit row per job with row count | `AuditLog.log_run(...)` -> `ETL_STAGING_DB.ETL_RUN_LOG` |
| `%log_step` -> `WORK.PIPELINE_AUDIT` | in-session audit trail printed at the end of job 04 | `AuditLog.log_step(...)` -> `ETL_STAGING_DB.PIPELINE_AUDIT` |

## 3. Constructs without a direct Spark equivalent

| Legacy construct | Where | Port decision |
|---|---|---|
| `QUALIFY ROW_NUMBER() OVER (...) = 1` | bteq 01 (addresses), 02 (top merchant category), 03 (daily balance, bureau) | `row_number()` over the same window + `.filter(rn == 1)`, plus an explicit deterministic tiebreaker column |
| `ORDER BY SUM(ABS(amount)) OVER (PARTITION BY acct, category) DESC` inside `QUALIFY` | bteq 02 | pre-aggregate spend per (account, category) with a window sum, then rank |
| `CROSS JOIN` volatile table `VT_RUN_PARAMS` | bteq 02 | literal `period_start` / `period_end` columns derived from the run date |
| `NULLIFZERO(x)` | bteq 02 | `nullif(x, 0)` |
| `STDDEV_POP` | bteq 03 | `stddev_pop` |
| `ADD_MONTHS`, `MONTHS_BETWEEN`, `CURRENT_DATE - n` | bteq 01/02/03 | `add_months`, `months_between(..., roundOff=False)`, `date_sub`; Teradata `MONTHS_BETWEEN` truncation reproduced by casting to int |
| `(CURRENT_DATE - DOB)/365.25` | bteq 01 | `datediff` day count / 365.25 cast to smallint (Teradata date subtraction is a day count) |
| correlated `NOT IN (SELECT ... WHERE t2.ACCOUNT_ID = t.ACCOUNT_ID ...)` | bteq 03 (new merchants) | **rewritten** as a windowed first-seen set + left-anti join (never ported as-is) |
| `COUNT(DISTINCT CASE WHEN ... END)` | bteq 03 | `count_distinct(when(...))` |
| `PROC STDIZE method=std` | sas 01 | z-score standardisation with the **sample** standard deviation (`stddev_samp`), matching SAS |
| `PROC FASTCLUS maxclusters=5 least=2 replace=full` | sas 01 | Spark ML `KMeans(k=5, maxIter=50, tol=0.001, seed fixed)`; cluster *ids* are not comparable to SAS, cluster *labelling* is re-derived from the ordered profile exactly as the legacy code does |
| `PROC RANK groups=100` | sas 02 | `ntile(100) - 1` over the ranked variable (PROC RANK emits 0..99) with a deterministic tiebreaker |
| `PROC MEANS median= qrange=` | sas 02 | `percentile_approx` at 0.5 / 0.75 / 0.25 with a large accuracy parameter |
| `PROC LOGISTIC selection=stepwise slentry=.10 slstay=.05` | sas 03 | Spark ML `LogisticRegression` + an explicit stepwise wrapper driven by Wald p-values from `LogisticRegressionSummary`; degenerate (single-class) targets fall back to the base rate |
| data step `MERGE ... IN=` (4-way) | sas 04 | successive `full_outer` joins carrying `_base/_seg/_txn/_risk` presence flags, then `filter(_base)` |
| `round(x, 0.01)` | sas 01/03 | `round(x, 2)` (both are half-up away from zero for positive values) |
| `mean(a, b, c, d)` over boolean expressions | sas 01 | `(int(a)+int(b)+int(c)+int(d))/4` |
| `ifc(cond, 'Y', 'N')` | sas 03 | `when(cond, 'Y').otherwise('N')` |
| `today()` / `datetime()` / `CURRENT_DATE` | everywhere | a single pinned **run date** from config, so the whole DAG is internally consistent and reproducible |
| `%sysfunc(intnx(month, today(), 0, beginning))` + `yymmn7.` | sas 02 | `date_format(trunc(run_date,'MM'), 'yyyy-MM')` -> `REPORTING_PERIOD` |
| `PROC APPEND ... FORCE` after `DELETE FROM` | sas 01/02/03/04 | overwrite write mode (job 02 deletes only the current `REPORTING_PERIOD` -> partition overwrite) |
| `COLLECT STATISTICS` | bteq 01/02/03, sas 04 | no-op on Spark; recorded as an audit step so the DAG shape is preserved |

## 4. Hardcoded constants, defaults and cutoffs (ported verbatim)

`config/pipeline_config.cfg`: `LOOKBACK_MONTHS=12`, `RISK_SCORE_THRESHOLD=700`,
`DB_CORE=CORE_BANKING_DB`, `DB_TXN=TXN_PROCESSING_DB`, `DB_STG=ETL_STAGING_DB`,
`DB_DP=DATA_PRODUCTS_DB`, `LOG_LEVEL=INFO`.

| Constant | Value | Source |
|---|---|---|
| age divisor | `365.25` | bteq 01 |
| customer status filter | `('A','I')` staging, `'A'` in SAS | bteq 01/03, sas 01/03/04 |
| credit utilisation when no limit | `0.00` | bteq 01 |
| posted-transaction filter | `STATUS_CODE = 'P'` | bteq 02/03 |
| channel buckets | `ATM`, `POS`, `WEB`, `MOB` | bteq 02 |
| daily-balance window | `ADD_MONTHS(CURRENT_DATE, -3)` | bteq 03 |
| payment-history window | `ADD_MONTHS(CURRENT_DATE, -24)` | bteq 03 |
| overdraft/NSF + large withdrawal window | `ADD_MONTHS(CURRENT_DATE, -12)` | bteq 03 |
| merchant-risk window | `ADD_MONTHS(CURRENT_DATE, -6)` | bteq 03 |
| large withdrawal threshold | `ABS(AMOUNT) >= 5000` | bteq 03 |
| NSF detection | `CATEGORY='FEE' AND DESCRIPTION LIKE '%NSF%'` | bteq 03 |
| high-risk merchant categories | `GAMBLING, WIRE_TRANSFER_INTL, CRYPTO_EXCHANGE, PAWN_SHOP` | bteq 03 |
| international channel | `CHANNEL_CODE = 'INTL'` | bteq 03 |
| velocity windows | `CURRENT_DATE - 7`, `CURRENT_DATE - 30` | bteq 03 |
| balance windows | `CURRENT_DATE - 30`, `CURRENT_DATE - 90` | bteq 03 |
| default `PAYMENT_ONTIME_PCT` | `100.00` when no payments | bteq 03 |
| default `MONTHS_SINCE_LAST_LATE` | `999` | bteq 03 |
| tenure groups | `<12`, `<36`, `<84`, else | sas 01 |
| age groups | `<25`, `<41`, `<57`, `<76`, else | sas 01 |
| balance tiers | `<1000`, `<10000`, `<100000`, else | sas 01 |
| segment labels by ordered profile | `PREMIUM_WEALTH, ENGAGED_MAINSTREAM, GROWING_DIGITAL, CREDIT_DEPENDENT, VALUE_BASIC` | sas 01 |
| LTV heuristic | `log_balance * tenure_months * product_breadth * 10` rounded to 0.01 | sas 01 |
| cross-sell rule | `PRODUCT_BREADTH < 0.50 AND ACCT_RATIO >= 0.75` | sas 01 |
| upsell rule | `BALANCE_TIER = 'MODERATE' AND TENURE_GROUP <> 'NEW (<1yr)'` | sas 01 |
| retention-risk rule | `ACCT_RATIO < 0.50 AND TENURE_MONTHS >= 60` | sas 01 |
| model versions | `SEG_V3.2`, `TXN_V2.1`, `RISK_V4.0`, `MASTER_V1.5` | sas 01-04 |
| active-account rule (txn) | `DAYS_SINCE_LAST_TXN <= 30` | sas 02 |
| spend trend cutoff | `± AVG_TRANSACTION_SIZE * 5` | sas 02 |
| interest proxy | `TOTAL_DEBIT_AMT * 0.02` | sas 02 |
| anomaly rule | `TOTAL_DEBIT_AMT > median + 3*IQR AND IQR > 0` | sas 02 |
| bureau imputation | `680` when `<= 0` or missing | sas 03 |
| bureau normalisation | `(score-300)/(850-300)*100` | sas 03 |
| default proxy target | `PAYMENT_LATE_CNT > 2` | sas 03 |
| composite risk weights | `0.30 / 0.25 / 0.15 / 0.20 / 0.10` | sas 03 |
| risk tier cutoffs | `<20 LOW`, `<40 MODERATE`, `<60 ELEVATED`, `<80 HIGH`, else `CRITICAL` | sas 03 |
| watch list rule | `RISK_TIER='CRITICAL' AND PD > 0.5` | sas 03 |
| review-required rule | `COMPOSITE >= 60 AND VELOCITY_RATIO > 2.0` | sas 03 |
| velocity ratio | `(DEBIT_7D * (30/7)) / DEBIT_30D`, else `1` | sas 03 |
| master-profile defaults | `UNCLASSIFIED`, `UNKNOWN`, zeros, `'N'` flags | sas 04 |
| `%validate_table` production minimum | `min_rows=1000` | sas 01-04 |

## 5. Legacy quirks preserved verbatim (never "fixed")

1. `sas 03`: `CREDIT_RISK_COMPONENT = 100 - BUREAU_SCORE_NORM` and the fourth composite term
   `(100 - BUREAU_SCORE_COMPONENT)` are **numerically identical**, so the bureau score is
   effectively weighted `0.30 + 0.20` and no credit-utilisation term enters the composite,
   despite the label `CREDIT_UTILIZATION`. Ported as written.
2. `sas 03`: the driver array's fourth element is the expression `(100 - BUREAU_SCORE_COMPONENT)`
   and its strict `>` comparisons mean element 4 can only ever become the *secondary* driver,
   never the primary. Ported as written.
3. `sas 03`: `PAYMENT_HISTORY_COMP` enters the composite as `100 - PAYMENT_HISTORY_COMP`, which
   equals `BEHAVIOUR_RISK_COMPONENT`; both are therefore counted (weights 0.25 and 0.10).
4. `sas 01`: `DIGITAL_ADOPTION_SCORE` is hardcoded to `0` and `CHANNEL_PREFERENCE` to `''`
   ("enriched later"), even though the txn analytics job computes a digital percentage.
5. `sas 02`: `ANOMALY_FLAG` is initialised to `'N'` before the IQR test; `SPEND_PERCENTILE` comes
   from `PROC RANK groups=100` so it is a 0-99 integer bucket, not a true percentile.
6. `bteq 02`: the `top_cat` subquery is joined on `ACCOUNT_ID` only (no date/period predicate on
   the outer side) and `TOP_MERCHANT_CATEGORY` is both grouped by and wrapped in `MAX()`.
7. `bteq 02`: `DISTINCT_MERCHANTS` counts `MERCHANT_NAME` including rows whose merchant is NULL
   (SQL `COUNT(DISTINCT)` ignores NULLs - preserved).
8. `bteq 03`: `ACCOUNT_OVERDRAFT_CNT` counts *transactions* with a negative running balance, not
   distinct overdraft events, and it is not restricted to any account type.
9. `bteq 03`: the "on-time payment" test compares the transaction date against
   `ADD_MONTHS(OPEN_DATE, months_between(txn_date, open_date)::int + 1)`, which is true for
   essentially every payment, so `PAYMENT_ONTIME_PCT` is ~100 and `PAYMENT_LATE_CNT` ~0 - which in
   turn makes the `DEFAULT_FLAG` target in sas 03 all-zero. Ported as written.
10. `bteq 03`: `EXTERNAL_CREDIT_SCORE` defaults to `0` (not NULL) when the bureau row is missing,
    and sas 03 then imputes `680` for any score `<= 0`.
11. `sas 04`: `TRANSACTION_ANALYTICS` is filtered by `EFFECTIVE_DATE = today()`, so a same-day run
    is assumed; a re-run on a later date would silently produce empty txn columns.
12. `sas 02`: `DIGITAL_TXN_PCT` weights `(PCT_WEB + PCT_MOBILE)` by each account's transaction
    count, i.e. a weighted mean, while `TOP_SPEND_CATEGORY` is `MAX()` (alphabetical), not the
    largest-spend category across accounts.

## 6. Gaps found in the legacy sources (documented, not silently patched)

* `ddl/00_source_tables.sql` does **not** define `CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES`, which
  `bteq/03_stg_risk_factors.bteq` reads and for which a source CSV exists. Its schema is inferred
  from the CSV + usage (`CUSTOMER_ID BIGINT, EXTERNAL_CREDIT_SCORE INTEGER, REPORT_DATE DATE`).
* No DDL exists for `ETL_STAGING_DB.ETL_RUN_LOG` or the SAS `WORK.PIPELINE_AUDIT` audit table;
  both schemas are inferred from their INSERT statements / `%init_audit`.
* `bteq/run_bteq_pipeline.sh` runs its steps in a `for` loop that ignores `run_bteq`'s return
  code under `set -e` semantics (the function's non-zero return *does* stop the loop because it is
  not in a condition context, so fail-fast holds); the port implements explicit fail-fast.
