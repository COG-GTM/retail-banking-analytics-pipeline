# TICKET-08 — `02_sas_txn_analytics.sas` + `03_sas_risk_scoring.sas` → PySpark + MLlib

| | |
|---|---|
| **Ticket** | T8 |
| **Branch** | `migrate/ticket-08-txn-risk-analytics` |
| **Owned paths** | `synapse/spark/txn_analytics.py`, `synapse/spark/risk_scoring.py`, `synapse/tests/ticket_08/**` |
| **Contracts OWNED** | none (implements Spark jobs `txn_analytics.py`, `risk_scoring.py` from C7 §3) |
| **Contracts CONSUMED** | C1 (`STG_TXN_SUMMARY`, `STG_RISK_FACTORS`, `STG_CUSTOMER_360` → `TRANSACTION_ANALYTICS`, `CUSTOMER_RISK_SCORES`), C2, C3, C4, C5 (incl. the period-scoped delete for `TRANSACTION_ANALYTICS`), C7 |
| **Depends on** | **T2 + T9 contract freeze** (Wave 2). Runtime: T9 `synapse/common/`, T4/T5/T6 data |
| **Blocks (runtime only)** | T9 `data_products.py` reads both outputs |

## Objective

Port the transaction-analytics program (percentile ranking + IQR anomaly detection,
`MODEL_VERSION = 'TXN_V2.1'`) and the risk-scoring program (logistic regression PD + weighted
composite score, `MODEL_VERSION = 'RISK_V4.0'`, `RISK_SCORE_THRESHOLD = 700`) to two PySpark jobs.

## Source files (read-only references)

- `sas/02_sas_txn_analytics.sas`
- `sas/03_sas_risk_scoring.sas`
- `sas/macros/*.sas` (replaced by `common.*`)
- `ddl/02_data_product_tables.sql` (`TRANSACTION_ANALYTICS`, `CUSTOMER_RISK_SCORES`; target shapes C1 §3.12–3.13)
- `config/pipeline_config.cfg` (`RISK_SCORE_THRESHOLD=700`)

## Target outputs

- `synapse/spark/txn_analytics.py` — `JOB_NAME = "02_TXN_ANALYTICS"`, `MODEL_VERSION = "TXN_V2.1"`, `TARGET_TABLE = "DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS"`, arg `--reporting-period` (default month of as-of date)
- `synapse/spark/risk_scoring.py` — `JOB_NAME = "03_RISK_SCORING"`, `MODEL_VERSION = "RISK_V4.0"`, `TARGET_TABLE = "DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES"`, arg `--risk-score-threshold` (default 700)

Both: `build_arg_parser()`, `run(spark, cfg, **job_params) -> int`, `main(argv=None) -> int`, `sys.exit(main())` guard (C7 §3).

## Translation requirements — `txn_analytics.py` (`sas/02_sas_txn_analytics.sas`)

| SAS | Spark requirement |
|---|---|
| `REPORTING_PERIOD = putn(intnx(month, today(), 0, beginning), yymmn7.)` | `'YYYY-MM'` of `cfg.as_of_date` (DDL comment and reference CSV use `YYYY-MM`, e.g. `2026-04`) |
| STEP 1 `select * from STGDB.STG_TXN_SUMMARY` | `sqlc.read_table(spark, cfg, "ETL_STAGING_DB.STG_TXN_SUMMARY")` |
| STEP 2 customer aggregation: `TOTAL_ACCOUNTS = count(distinct ACCOUNT_ID)`, `ACTIVE_ACCOUNTS = sum(DAYS_SINCE_LAST_TXN <= 30)`, `TOTAL_TRANSACTIONS`, `TOTAL_DEBIT_AMT`, `TOTAL_CREDIT_AMT`, `NET_CASH_FLOW = credit - debit`, `AVG_TRANSACTION_SIZE = sum(debit+credit)/sum(txn)` (0 if no txns), `TOTAL_FEES`, `TOP_SPEND_CATEGORY = max(TOP_MERCHANT_CATEGORY)` (**lexicographic max**, as SAS), `DIGITAL_TXN_PCT = sum(TXN_COUNT_TOTAL*(PCT_WEB+PCT_MOBILE)/100)/sum(TXN_COUNT_TOTAL)*100` | identical formulas; note SAS `sum(a + b)` returns missing for a row if either is missing — preserve with explicit null handling |
| STEP 3 trend: `NET_CASH_FLOW > AVG_TRANSACTION_SIZE*5` → `UP`; `< -AVG*5` → `DOWN`; else `STABLE`; `FEE_INCOME = TOTAL_FEES`; `INTEREST_INCOME = TOTAL_DEBIT_AMT * 0.02`; `REVENUE_CONTRIBUTION = FEE + INTEREST` | same |
| STEP 4 `PROC RANK groups=100` on `TOTAL_DEBIT_AMT` → `SPEND_PERCENTILE` | SAS formula: `FLOOR(r * 100 / (n + 1))`, where `r` = ascending rank with ties averaged (SAS default `TIES=MEAN`) and `n` = count of non-null values → integer 0–99. Implement exactly (window functions), not `percent_rank` |
| STEP 5 `PROC MEANS median= qrange=` (default `QNTLDEF=5`) | compute exact median and IQR (Q3−Q1) with the **QNTLDEF=5** definition: sort the n values; for p, let `np = n*p`; if `np` is an integer `j`, quantile = `(x_j + x_{j+1}) / 2`, else `x_{ceil(np)}`. Do **not** use `percentile_approx` |
| `ANOMALY_FLAG = 'Y'` if `TOTAL_DEBIT_AMT > median + 3*IQR and IQR > 0`, else `'N'` | same |
| `MODEL_VERSION`, `EFFECTIVE_DATE = today()`, `LOAD_TS` | `'TXN_V2.1'`, `cfg.as_of_date`, UTC now |
| STEP 6 `%validate_table(key_cols=CUSTOMER_ID, not_null=CUSTOMER_ID REPORTING_PERIOD TOTAL_TRANSACTIONS, min_rows=1000)` | `validate_dataframe(...)` per C3 §5 |
| STEP 7 `DELETE ... WHERE REPORTING_PERIOD = '&REPORTING_PERIOD.'` + `PROC APPEND` | `truncate_and_load(cfg, df, TARGET_TABLE, JOB_NAME, delete_predicate="REPORTING_PERIOD = ?", predicate_params=[reporting_period])` — period-scoped, **not** a full truncate (C5 §3) |
| `TOTAL_FEES` column (not in target table) | dropped by `conform_to_contract` |

Known reference divergences (`data/03_sas_data_products/transaction_analytics.csv`, 500 rows):
`SPEND_PERCENTILE` there is `rank/n*100` (not PROC RANK groups) and `TOP_SPEND_CATEGORY` is first
non-null rather than `max()` (~51% agreement). Port the SAS semantics and report agreement for
those two columns; anomaly flags should match exactly; all other columns within 0.01.

## Translation requirements — `risk_scoring.py` (`sas/03_sas_risk_scoring.sas`)

| SAS | Spark requirement |
|---|---|
| `%let RISK_THRESHOLD = %sysget(RISK_SCORE_THRESHOLD)` (700) | `--risk-score-threshold` / `cfg.risk_score_threshold` (default 700). In the SAS source it is read but **not used** in any calculation — expose it, log it in the `FULL_LOAD` START message, do not invent new logic with it |
| STEP 1 `STG_RISK_FACTORS r inner join STG_CUSTOMER_360 c` (+ `TENURE_MONTHS`, `NUM_ACTIVE_ACCOUNTS`, `TOTAL_BALANCE`, `CUSTOMER_STATUS`), `where c.CUSTOMER_STATUS = 'A'` | same join/filter via two `read_table` calls |
| STEP 2 impute `EXTERNAL_CREDIT_SCORE <= 0 or missing` → 680; `BUREAU_SCORE_NORM = (score-300)/(850-300)*100`; `BALANCE_TREND_RATIO` (30D/90D, else 1); `VELOCITY_RATIO = (7D*(30/7))/30D` (else 1); `DEFAULT_FLAG = PAYMENT_LATE_CNT > 2` | same |
| STEP 3 `PROC LOGISTIC descending` with `selection=stepwise slentry=0.10 slstay=0.05` on `BUREAU_SCORE_NORM CREDIT_UTIL_RATIO PAYMENT_ONTIME_PCT BALANCE_VOLATILITY VELOCITY_RATIO ACCOUNT_OVERDRAFT_CNT LARGE_WITHDRAWAL_CNT HIGH_RISK_MERCHANT_CNT TENURE_MONTHS`, `predicted=PROB_DEFAULT` | `pyspark.ml.classification.LogisticRegression` modelling P(`DEFAULT_FLAG = 1`). MLlib has no stepwise selection: implement forward-entry / backward-removal using likelihood-ratio tests between MLlib fits (entry p < 0.10, stay p < 0.05; chi-square p-values via `scipy.stats.chi2`), and document the approach. `lackfit` (Hosmer-Lemeshow) → log only, optional |
| Single-class target | If `DEFAULT_FLAG` has only one class (true for the sample data — all `PAYMENT_LATE_CNT` are 0), PROC LOGISTIC cannot fit and `PROB_DEFAULT` is missing → `coalesce(…, 0)` = 0. Mirror this: skip the fit, log a `WARNING` audit row, `PROBABILITY_OF_DEFAULT = 0` |
| STEP 4 components: `CREDIT_RISK = clamp(100 - BUREAU_SCORE_NORM)`, `BEHAVIOUR_RISK = clamp(100 - PAYMENT_ONTIME_PCT)`, `VELOCITY_RISK = clamp((VELOCITY_RATIO-1)*50)`, `BUREAU_SCORE_COMPONENT = clamp(BUREAU_SCORE_NORM)`, `PAYMENT_HISTORY_COMP = clamp(PAYMENT_ONTIME_PCT)` with `clamp = max(0, min(100, x))` | same |
| `COMPOSITE_RISK_SCORE = round(0.30*CREDIT + 0.25*BEHAVIOUR + 0.15*VELOCITY + 0.20*(100-BUREAU) + 0.10*(100-PAYMENT_HIST), 0.01)` | same |
| `PROBABILITY_OF_DEFAULT = round(coalesce(PROB_DEFAULT, 0), 0.000001)` | same |
| Tiers `<20` LOW, `<40` MODERATE, `<60` ELEVATED, `<80` HIGH, else CRITICAL | same |
| Drivers: loop over `[CREDIT_RISK, BEHAVIOUR_RISK, VELOCITY_RISK, 100-BUREAU_SCORE_COMPONENT]` with labels `CREDIT_UTILIZATION`, `PAYMENT_BEHAVIOUR`, `TRANSACTION_VELOCITY`, `BUREAU_SCORE`; strict `>` updates of `_max1`/`_max2`, starting at 0 (blank driver if nothing > 0) | replicate the loop **exactly** (strict `>`, first-wins on ties) in a UDF-free column expression or small pure-Python function applied via `mapInPandas`/UDF. (The SAS `array` containing the expression `(100 - BUREAU_SCORE_COMPONENT)` is not valid SAS; the evident intent is used.) Since `CREDIT_RISK == 100 - BUREAU_SCORE_COMPONENT` whenever both are in range, `BUREAU_SCORE` effectively never wins primary — expected |
| `SCORE_DELTA_30D = 0`; `WATCH_LIST_FLAG = (RISK_TIER = 'CRITICAL' and PD > 0.5)`; `REVIEW_REQUIRED_FLAG = (COMPOSITE >= 60 and VELOCITY_RATIO > 2.0)` | same |
| rename `PAYMENT_HISTORY_COMP` → `PAYMENT_HISTORY_COMPONENT`; `MODEL_VERSION`, `EFFECTIVE_DATE`, `LOAD_TS` | same; `'RISK_V4.0'`, `cfg.as_of_date`, UTC now |
| STEP 5 `%validate_table(key_cols=CUSTOMER_ID, not_null=CUSTOMER_ID COMPOSITE_RISK_SCORE RISK_TIER)`; `PROC FREQ` tier distribution | `validate_dataframe(...)`; log tier distribution as an audit `MESSAGE` |
| STEP 6 `DELETE FROM ...CUSTOMER_RISK_SCORES` + `PROC APPEND` | `truncate_and_load(cfg, df, TARGET_TABLE, JOB_NAME)` |

Known reference divergences (`data/03_sas_data_products/customer_risk_scores.csv`, 407 rows): the CSV
has a constant `PROBABILITY_OF_DEFAULT = 0.05` (SAS-faithful result is 0 on this single-class
data) and its driver columns use `>=` tie-breaking. Port SAS semantics; all component and
composite scores and tiers must match within 0.01; report driver agreement.

## Acceptance criteria

- [ ] `txn_analytics.run()` (local PySpark; `read_table` serving `data/02_bteq_staging/stg_txn_summary.csv`, write-back captured) yields **500** rows, `REPORTING_PERIOD = '2026-04'`, C1 §3.12 columns; the write-back call uses `delete_predicate="REPORTING_PERIOD = ?"`.
- [ ] Unit tests prove the PROC RANK groups=100 formula (ties) and QNTLDEF=5 quantiles on small hand-computed fixtures.
- [ ] `risk_scoring.run()` yields **407** rows, C1 §3.13 columns; single-class path produces PD 0 with a `WARNING` audit record; a synthetic two-class fixture exercises the stepwise LR path and produces PDs in (0,1).
- [ ] Parity vs. reference CSVs reported per column, with divergences explained as above.
- [ ] Both modules: `--help` works, `sys.exit(main())` guard present, audit `FULL_LOAD` START/SUCCESS records, validation failure blocks write-back.
- [ ] Fakes of `common.*` (until T9 merges) only under `synapse/tests/ticket_08/`, patched in both `sys.modules` and on the `common` package.

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T8] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_08/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_08/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
