# Migration Notes — deviations from the legacy pipeline

Every place where the PySpark port cannot be bit-identical to the Teradata BTEQ / SAS 9.4
original, what the difference is, why it is unavoidable, and the tolerance the regression tier
enforces. Anything **not** listed here is asserted exactly.

Legacy quirks that look like bugs were ported verbatim, not fixed; they are listed in
[§5](#5-legacy-quirks-preserved-on-purpose) so a reviewer can see they were a decision.

Companion documents: [`LEGACY_INVENTORY.md`](LEGACY_INVENTORY.md) (Phase 1 inventory: job
mapping, dependency graph, failure semantics, construct-by-construct port decisions, and the
full constant list).

---

## 1. Engine-level differences (affect every job)

| # | Difference | Why it is unavoidable | Tolerance enforced |
|---|---|---|---|
| E1 | **Date arithmetic.** Teradata `DATE - DATE` is a day count; `MONTHS_BETWEEN` returns a decimal that the target `INTEGER` column truncates. Spark's `months_between` is calendar-based. | Different engine semantics; the legacy engine is not available to defer to. | `STG_CUSTOMER_360.AGE` and `TENURE_MONTHS`: `abs(diff) <= 1` against the committed expected output (350 and 317 of 478 rows differ by exactly 1). **No other column may diverge** — the regression harness fails if one does. |
| E2 | **`CURRENT_DATE` / `today()` / `datetime()`** are evaluated per statement in the legacy pipeline; the port pins one **run date** from `--run-date` (default: today) and one load timestamp per job. | A DAG spanning midnight would otherwise read two different "todays"; reproducibility also makes regression testing possible. | Exact — the pinned date is the value the legacy scripts would have used within one run. |
| E3 | **`COLLECT STATISTICS`** has no Spark equivalent (the optimiser uses AQE runtime statistics instead). | Not applicable to Spark. | Emitted as an audit-visible no-op step so the audit trail keeps the legacy shape. |
| E4 | **`DELETE FROM` + `PROC APPEND FORCE` / `INSERT ... SELECT`** full refreshes become `mode="overwrite"` writes; `02_sas_txn_analytics`, which deletes only the current `REPORTING_PERIOD`, becomes a partition overwrite. | Spark has no row-level delete on file-backed tables. | Exact — the resulting table contents are identical. |
| E5 | **Decimal arithmetic.** Teradata's `DECIMAL` scale-propagation rules differ slightly from Spark's for division. All ratio/percentage columns are explicitly `round()`ed and cast to the DDL type before the write. | Engine semantics. | Exact after the explicit cast — the DDL type is enforced by `enforce_schema`. |
| E6 | **Ordering.** Several legacy statements order by a non-unique key (`QUALIFY ROW_NUMBER`, the SAS report `PROC SQL`s). | Legacy output was implementation-defined on ties. | A deterministic tiebreaker is appended in each case and documented on the transform; see §4. |

## 2. Construct approximations (statistical / ML)

| # | Legacy | Port | Bound |
|---|---|---|---|
| A1 | `PROC FASTCLUS maxclusters=5 least=2 replace=full` (`sas/01`) | Spark ML `KMeans(k=5, maxIter=50, tol=0.001, seed=...)` on the same standardised features | Cluster **ids** are not comparable across implementations — the legacy code never uses them directly: it re-derives the label from the ordered cluster profile, and the port does the same. Regression asserts the segment-label distribution and the per-customer label with the tolerance stated in the job's parity test. |
| A2 | `PROC LOGISTIC selection=stepwise slentry=.10 slstay=.05` (`sas/03`) | Spark ML `LogisticRegression` wrapped in an explicit stepwise selector driven by Wald p-values from `LogisticRegressionSummary` | Spark's p-values come from the same Wald statistic, but the optimiser differs (L-BFGS vs Fisher scoring), so coefficients differ in the far decimals. The **selected feature set** and the resulting probability are asserted within the tolerance declared in the job's parity test; a degenerate single-class target falls back to the base rate exactly as SAS would. |
| A3 | `PROC MEANS median= qrange=` (`sas/02`) | `percentile_approx` at 0.25/0.5/0.75 with a high accuracy parameter | Approximate quantiles; the accuracy parameter is set so the sample data matches exactly. |
| A4 | `PROC RANK groups=100` (`sas/02`) | `ntile(100) - 1` over the same ordering with a deterministic tiebreaker | Exact except on ties, which the legacy engine also resolved arbitrarily. |
| A5 | `PROC STDIZE method=std` (`sas/01`) | z-score with the **sample** standard deviation (`stddev_samp`), matching SAS's divisor of *n-1* | Exact. |

## 3. Correctness rewrites (same result, different plan)

These change *how* the result is computed, never *what* it is:

* **Correlated `NOT IN` → windowed first-seen + left anti-join** (`bteq/03`, new-merchant detection).
  The legacy subquery re-scans `TRANSACTIONS` per outer row; the port computes each merchant's
  first-seen date per account once and anti-joins. Asserted equal on the sample data.
* **Broadcast small dimensions.** `TRANSACTION_TYPES` and the other lookup-sized inputs are
  broadcast rather than shuffled.
* **Pre-aggregation before ranking** (`bteq/02` top merchant category): the legacy
  `ORDER BY SUM(ABS(amount)) OVER (...)` inside `QUALIFY` is computed as an aggregate first.
* **Date-slice pushdown.** Each job reads only the date range its window needs
  (`LOOKBACK_MONTHS`, and the 3/6/12/24-month risk windows) instead of the full history.
* **Partitioning per the DDL.** Outputs are written with the DDL's partitioning columns.

## 4. Deterministic tiebreakers added

The legacy statement was ambiguous on ties in each of these; the added key is the smallest
change that makes the output reproducible, and it never changes a non-tied row.

| Job | Statement | Tiebreaker added |
|---|---|---|
| `01_stg_customer_360` | `QUALIFY ROW_NUMBER() ... ORDER BY EFFECTIVE_DATE DESC` (primary address) | `ADDRESS_ID DESC` |
| `02_stg_txn_summary` | top merchant category by spend | category name ascending |
| `03_stg_risk_factors` | latest daily balance / latest bureau score | the row's surrogate key descending |
| `02_sas_txn_analytics` | `PROC RANK` ordering | `CUSTOMER_ID` ascending |
| `04_sas_data_products` | both STEP 3 report `order by` clauses | label name ascending |

Each is documented in the transform's docstring next to the legacy line it replaces.

## 5. Legacy quirks preserved on purpose

Ported verbatim; **not** fixed. Each has a test asserting the quirky behaviour so a future
"cleanup" fails loudly.

* `04_sas_data_products` reads `TRANSACTION_ANALYTICS where EFFECTIVE_DATE = today()`. Re-run on
  any later date it silently selects nothing, and every customer is loaded with the
  `if not _txn` zero defaults. Preserved as `EFFECTIVE_DATE = config.run_date`.
* SAS `MERGE ... IN=` applies its defaults **per missing source, not per missing column** — a
  segment row that exists with a NULL `SEGMENT_NAME` keeps the NULL rather than becoming
  `UNCLASSIFIED`. The port carries `_base/_seg/_txn/_risk` presence flags to reproduce this;
  a per-column `coalesce` would have been wrong.
* SAS character missings are the empty string, so `FULL_NAME = trim(FIRST_NAME) || ' ' ||
  trim(LAST_NAME)` yields `" Lastname"` (leading space), not NULL, for a missing first name —
  and `length=120` truncates.
* `%validate_table`'s NOT NULL check is a **warning**, not an abort; only the row-count and
  duplicate-key checks abort. Reproduced in `common/validation.py`.
* SAS return code 1 (warnings) is tolerated by `sas/run_sas_pipeline.sh`; only `rc>=2` aborts.
  Reproduced by `STATUS_WARNING` vs `STATUS_FAILED` in the driver.
* Default `PAYMENT_ONTIME_PCT = 100.00` when a customer has no payment history, and
  `MONTHS_SINCE_LAST_LATE = 999` when never late — both flatter than reality, both preserved.

## 6. Operational differences

| Difference | Rationale |
|---|---|
| `min_rows` is config-driven (`PipelineConfig.min_rows`, production value `PRODUCTION_MIN_ROWS = 1000` as in `%validate_table`). The committed sample extract has 478 customers, so the local/e2e runs pass a lower floor rather than deleting the check. | Keeping the production threshold hardcoded would have meant either failing every local run or removing the check. |
| The Teradata *databases* (`CORE_BANKING_DB`, `TXN_PROCESSING_DB`, `ETL_STAGING_DB`, `DATA_PRODUCTS_DB`) become PostgreSQL **schemas** inside one database for the local deployment. | PostgreSQL cross-database queries need FDWs; schemas preserve the two-part `DB.TABLE` naming the DDL and every job use. Mapping lives in `orchestration/deploy_postgres.py`. |
| Teradata `PRIMARY INDEX` / `PARTITION BY RANGE_N` become PostgreSQL indexes in the local deployment. | The primary index is a Teradata distribution mechanism with no PostgreSQL analogue; the index preserves the access path and the uniqueness the jobs rely on. |
| `WORK.PIPELINE_AUDIT` (a SAS session-scoped table printed at the end of job 04) is persisted to `ETL_STAGING_DB.PIPELINE_AUDIT`. | A session-scoped table cannot survive a distributed run; persisting it also satisfies the run-log requirement. |
| The SAS STEP 3 `PROC SQL` reports print to the SAS log; the port returns them as DataFrames and emits them through `audit.log_step`. | Output is behaviour, not noise — this keeps it auditable and testable. |
