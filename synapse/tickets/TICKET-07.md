# TICKET-07 — `01_sas_customer_segments.sas` → PySpark StandardScaler + MLlib KMeans

| | |
|---|---|
| **Ticket** | T7 |
| **Branch** | `migrate/ticket-07-customer-segments` |
| **Owned paths** | `synapse/spark/customer_segments.py`, `synapse/tests/ticket_07/**` |
| **Contracts OWNED** | none (implements Spark job `customer_segments.py` from C7 §3) |
| **Contracts CONSUMED** | C1 (`STG_CUSTOMER_360` → `CUSTOMER_SEGMENTS`), C2 (`common.audit`), C3 (`common.validation`), C4 (`common.config`, `common.sql_connector`), C5 (`common.writeback`), C7 (CLI / `run` / `main`) |
| **Depends on** | **T2 + T9 contract freeze** (Wave 2). Runtime: T9 `synapse/common/` implementation, T4 data |
| **Blocks (runtime only)** | T9 `data_products.py` reads `CUSTOMER_SEGMENTS` |

## Objective

Re-implement the SAS customer segmentation (PROC STDIZE + PROC FASTCLUS k=5 + rule-based
labels/flags) as a PySpark / Spark MLlib job producing `DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS`
with `MODEL_VERSION = 'SEG_V3.2'`.

## Source files (read-only references)

- `sas/01_sas_customer_segments.sas` (logic to port)
- `sas/macros/log_step.sas`, `sas/macros/validate_table.sas`, `sas/macros/connect_teradata.sas` (replaced by `common.*` — do not re-implement)
- `ddl/02_data_product_tables.sql` (`CUSTOMER_SEGMENTS`; target shape C1 §3.11)

## Target output

`synapse/spark/customer_segments.py` with `JOB_NAME = "01_CUSTOMER_SEG"`, `MODEL_VERSION = "SEG_V3.2"`,
`TARGET_TABLE = "DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS"`, and `build_arg_parser()`, `run(spark, cfg, kmeans_restarts=10, seed=42) -> int`,
`main(argv=None) -> int`, ending with `if __name__ == "__main__": sys.exit(main())` (C7 §3).

## Translation requirements (SAS step → Spark)

| SAS (step) | Spark requirement |
|---|---|
| `%include` macros, `%init_audit`, `%connect_teradata()` | `config_from_args` → `audit.init_audit(cfg)`; reads via `sqlc.read_table(spark, cfg, "ETL_STAGING_DB.STG_CUSTOMER_360", columns=[...], where="CUSTOMER_STATUS = 'A'")` |
| `%log_step(step=01_CUSTOMER_SEG, ...)` calls | `audit.audit_step` / `audit.log_step` with `JOB_NAME = '01_CUSTOMER_SEG'`, `FULL_LOAD` START/SUCCESS + optional `EXTRACT`/`FEATURES`/`MODEL`/`BUILD` (C2 §4) |
| STEP 1 extract (15 columns, `CUSTOMER_STATUS = 'A'`) | same columns and filter |
| STEP 2 `PRODUCT_BREADTH = mean((HAS_CHECKING='Y'), (HAS_SAVINGS='Y'), (HAS_CREDIT='Y'), (HAS_LOAN='Y'))` | average of four 0/1 indicators |
| `TENURE_GROUP`: `<12` `NEW (<1yr)`, `<36` `DEVELOPING (1-3yr)`, `<84` `ESTABLISHED (3-7yr)`, else `LOYAL (7yr+)` | same strict `<` boundaries (SAS uses `<`, not `<=`) |
| `AGE_GROUP`: `<25` GEN_Z, `<41` MILLENNIAL, `<57` GEN_X, `<76` BOOMER, else SILENT | same, strict `<` |
| `BALANCE_TIER`: `<1000` LOW, `<10000` MODERATE, `<100000` AFFLUENT, else HIGH_NET_WORTH | same, strict `<` |
| `DIGITAL_ADOPTION_SCORE = 0` | literal 0 |
| `LOG_BALANCE = log(max(TOTAL_BALANCE, 1))`, `ACCT_RATIO = NUM_ACTIVE_ACCOUNTS / max(NUM_ACCOUNTS, 1)` | natural log; note SAS `max()` ignores missing values (Spark `greatest()` does too); a missing `NUM_ACTIVE_ACCOUNTS` yields missing `ACCT_RATIO` — define explicit null handling before clustering and document it |
| STEP 3 `PROC STDIZE method=std` on `LOG_BALANCE TENURE_MONTHS CREDIT_UTILIZATION_PCT PRODUCT_BREADTH ACCT_RATIO AGE` | `VectorAssembler` + `StandardScaler(withMean=True, withStd=True)` (Spark uses the corrected *sample* std, same as STDIZE `method=std`) |
| STEP 4 `PROC FASTCLUS maxclusters=5 maxiter=50 converge=0.001` | `pyspark.ml.clustering.KMeans(k=5, maxIter=50, tol=0.001, seed=...)`. A single seed reproduces the reference clusters poorly (~43% agreement observed in an earlier spike); run `--kmeans-restarts` fits with seeds `seed, seed+1, ...` and keep the lowest `summary.trainingCost` (≈ scikit-learn `n_init`). Default 10; report agreement for 10 and for a larger value |
| SAS cluster numbers `1..5` → `SEGMENT_ID` | Spark predictions are `0..4` → `SEGMENT_ID = prediction + 1` |
| STEP 5 labels: clusters ordered by `avg(LOG_BALANCE)` desc → `PREMIUM_WEALTH`, `ENGAGED_MAINSTREAM`, `GROWING_DIGITAL`, `CREDIT_DEPENDENT`, `VALUE_BASIC`; `SUBSEGMENT_ID = 0` | same ordering/labels (deterministic tie-break by cluster id) |
| STEP 6 `LIFETIME_VALUE_SCORE = round(LOG_BALANCE * TENURE_MONTHS * PRODUCT_BREADTH * 10, 0.01)`, `ENGAGEMENT_SCORE = round(ACCT_RATIO * 100, 0.01)`, `PRODUCT_BREADTH_INDEX = round(PRODUCT_BREADTH * 100, 0.01)` | **Decision:** in SAS these read `c.*` from the FASTCLUS output, i.e. the *standardised* values, which looks unintended; the reference CSV uses raw values. Use the **raw (unstandardised)** features, and document the deviation in a code comment + PR description. Same rule for `ACCT_RATIO` in the flags below |
| `CHANNEL_PREFERENCE = ''` | empty string `''` |
| `CROSS_SELL_FLAG`: `PRODUCT_BREADTH < 0.50 and ACCT_RATIO >= 0.75` | same |
| `UPSELL_FLAG`: `BALANCE_TIER = 'MODERATE' and TENURE_GROUP not in ('NEW (<1yr)')` | same |
| `RETENTION_RISK_FLAG`: `ACCT_RATIO < 0.50 and TENURE_MONTHS >= 60` | same |
| `MODEL_VERSION = "&MODEL_VERSION."`, `EFFECTIVE_DATE = today()`, `LOAD_TS = datetime()` | `'SEG_V3.2'`, `cfg.as_of_date`, UTC `current_timestamp()` |
| STEP 7 `%validate_table(... key_cols=CUSTOMER_ID, not_null=CUSTOMER_ID SEGMENT_NAME SEGMENT_ID, min_rows=1000)` + `%abort cancel` | `validation.validate_dataframe(cfg, df, ..., key_cols=["CUSTOMER_ID"], not_null=["CUSTOMER_ID","SEGMENT_NAME","SEGMENT_ID"])` (min_rows from `default_min_rows`, C3 §2); exception stops the job before write-back |
| STEP 8 `DELETE FROM DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS` + `PROC APPEND ... FORCE` | `writeback.truncate_and_load(cfg, df, "DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS", JOB_NAME)` (C5) |
| `PROC DATASETS ... delete` cleanup | `unpersist()` any cached DataFrames |
| SAS `round(x, 0.01)` | `F.round(x, 2)` (half-up for these non-negative values) |

## Reference data

`data/03_sas_data_products/customer_segments.csv` (407 rows, as of 2026-04-10) was produced by
scikit-learn `KMeans(n_init=10, random_state=42)` on the SAS features scaled by sample std, with raw
features for LTV/engagement. Segment *labels* depend on cluster ordering, so compare **cluster
assignment agreement** (best label permutation) and exact equality for all deterministic columns
(groups, tiers, flags, breadth index, LTV, engagement).

## Acceptance criteria

- [ ] `run()` on local PySpark with `sqlc.read_table` monkeypatched to serve `data/02_bteq_staging/stg_customer_360.csv` and `writeback.truncate_and_load` patched to capture the DataFrame produces **407** rows with exactly the C1 §3.11 columns/types (via `writeback.conform_to_contract` once T9 lands; until then assert against C1 directly).
- [ ] Deterministic columns (`TENURE_GROUP`, `AGE_GROUP`, `BALANCE_TIER`, `PRODUCT_BREADTH_INDEX`, `LIFETIME_VALUE_SCORE`, `ENGAGEMENT_SCORE`, three flags, `DIGITAL_ADOPTION_SCORE`, `SUBSEGMENT_ID`, `MODEL_VERSION`, `EFFECTIVE_DATE`) match the reference CSV exactly; any known boundary differences explained.
- [ ] Cluster agreement with the reference ≥ 90% at some documented `--kmeans-restarts` value (report the value and runtime); result deterministic for a fixed seed.
- [ ] Audit records (memory sink) contain `FULL_LOAD` START/SUCCESS with row count; a validation failure (e.g. duplicated key) raises and skips write-back.
- [ ] `python synapse/spark/customer_segments.py --help` works; module ends with the `sys.exit(main())` guard.
- [ ] Until T9 merges, tests may use minimal fakes of `common.*` that follow the stubs in `synapse/common/contracts/stubs/` — place them under `synapse/tests/ticket_07/` only, and patch both `sys.modules` entries and the attribute on the parent `common` package so test order does not matter.

## Session rules (apply to every ticket)

- **Repo:** `COG-GTM/retail-banking-analytics-pipeline`. Branch from the latest `main` (which contains `synapse/` scaffolding and contracts) using the branch name above, and open **one PR to `main`** titled `[Synapse T7] <ticket title>`.
- **Write only your owned paths** (listed above). Do **not** modify anything outside `synapse/`; the Teradata/SAS sources (`bteq/`, `sas/`, `ddl/`, `orchestration/`, `config/`, `docs/`, `data/`, `export_data.py`) are read-only references. Do not edit `synapse/README.md`, `synapse/tickets/`, `synapse/common/contracts/` (unless you are the listed contract owner, and then only your own contract file), `synapse/requirements*.txt`, or the prep-created `__init__.py` files.
- **Contracts are frozen** (`synapse/common/contracts/`). Code to them exactly. If one is wrong or insufficient, do not work around it silently: add a **"Contract change request"** section to your PR description (what, why, impact on other tickets).
- **No live Azure deployment** and no paid resource creation. Validate locally (see `synapse/tests/README.md`): T-SQL against a `mcr.microsoft.com/mssql/server:2022-latest` container on your ticket's dedicated host port, PySpark 3.4.1 under Python 3.10. Remove only containers you started.
- **Tests** go in `synapse/tests/ticket_07/` only (own `conftest.py`; extra deps in `synapse/tests/ticket_07/requirements.txt`).
- **Reference data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/` were generated **as of 2026-04-10** by a DuckDB/scikit-learn re-implementation (not real Teradata/SAS). Pin the as-of date to `2026-04-10` in parity tests; treat known divergences listed in this brief as documented differences, not bugs.
- Never commit `__pycache__/`, virtualenvs, credentials, or generated data. Never hard-code secrets.
- **Done =** PR open, tests passing locally, and the PR description lists: files added, how it was validated (commands + results), parity results vs. the reference CSVs (where applicable), any contract change requests, and open questions.
