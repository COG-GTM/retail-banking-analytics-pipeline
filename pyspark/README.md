# Retail Banking Analytics Pipeline — PySpark

A faithful PySpark re-implementation of the legacy Teradata **BTEQ** staging jobs
and **SAS** analytics jobs, plus orchestration, designed to scale to a
multi-million-customer database. It preserves the original business and technical
logic while replacing Teradata/SAS-specific constructs with DataFrame/Spark SQL
equivalents.

## Architecture

```
 source tables (Teradata / CSV lake)
        │
        ▼   BTEQ staging phase  (bteq/*.bteq  →  jobs/staging_*.py)
 ┌─────────────────────────────────────────────────────────────┐
 │ 01_stg_customer_360 → 02_stg_txn_summary → 03_stg_risk_factors │
 └─────────────────────────────────────────────────────────────┘
        │   (BTEQ-before-SAS phase gate — fail-fast)
        ▼   SAS analytics phase  (sas/*.sas  →  jobs/dp_*.py)
 ┌─────────────────────────────────────────────────────────────┐
 │ 01_customer_segments → 02_txn_analytics → 03_risk_scoring     │
 │                                     → 04_customer_master_profile │
 └─────────────────────────────────────────────────────────────┘
        │
        ▼   four data products
 CUSTOMER_SEGMENTS · TRANSACTION_ANALYTICS · CUSTOMER_RISK_SCORES · CUSTOMER_MASTER_PROFILE
```

### Layout
| Path | Purpose |
|------|---------|
| `common/config.py` | Config loader mirroring `config/pipeline_config.cfg` (`LOOKBACK_MONTHS=12`, `RISK_SCORE_THRESHOLD=700`, `run_date`, model versions). |
| `common/io.py` | `DataIO` abstraction replacing `connect_teradata.sas` — `LocalDataIO` (CSV/Parquet, credential-free) and `JdbcDataIO` (Teradata). |
| `common/schemas.py` | DDL output contracts (`TableSpec`) + `enforce_schema` / `assert_schema` for exact column/type fidelity. |
| `common/validation.py` | Port of `validate_table.sas` — min-rows, key-uniqueness (fatal) and NOT NULL (warning) checks. |
| `common/audit.py` | Port of `log_step.sas` + `PIPELINE_AUDIT` / `ETL_RUN_LOG` schemas; structured JSON logging with a correlation `run_id`. |
| `common/dates.py` | Teradata date arithmetic (age, tenure, recency, month-end clamping). |
| `common/spark.py` | Reproducible `SparkSession` factory with AQE + skew-join handling. |
| `jobs/staging_*.py` | The three BTEQ staging jobs. |
| `jobs/dp_*.py` | The four SAS analytics jobs (`dp_customer_segments`, `dp_txn_analytics`, `dp_risk_scoring`, `dp_customer_master_profile`). |
| `jobs/stepwise_logistic.py` | Explicit stepwise-selection wrapper around `pyspark.ml` `LogisticRegression` (see `MIGRATION_NOTES.md`). |
| `orchestration/pipeline.py` | Framework-agnostic DAG + fail-fast executor (ports `run_full_pipeline.sh`). |
| `orchestration/airflow_dag.py` | Airflow DAG wiring the same task graph (optional dependency). |

Each job module exposes pure `transform`-style functions (unit-testable on
in-memory DataFrames) plus a thin `run(spark, io, config, audit)` that reads
sources, validates, writes the target and records the audit trail.

## Legacy → Spark mapping (highlights)
| Legacy construct | PySpark equivalent |
|------------------|--------------------|
| `QUALIFY ROW_NUMBER() OVER (...)` | `Window` + `row_number()` filter |
| `VT_RUN_PARAMS` lookback | config-driven `run_date` / `lookback_start` date literals |
| `STDDEV_POP` | `F.stddev_pop` |
| `PROC STDIZE method=std` + `PROC FASTCLUS(k=5)` | `StandardScaler` + `pyspark.ml` `KMeans`, ordered-balance cluster labelling |
| `PROC RANK groups=100` | `ntile(100)` |
| `PROC MEANS` IQR anomaly | `percentile_approx` (median + 3·IQR) |
| `PROC LOGISTIC selection=stepwise` | explicit stepwise wrapper (`jobs/stepwise_logistic.py`) |
| 4-way `MERGE ... IN=` | left joins from `STG_CUSTOMER_360` with the same missing-field defaults |
| correlated `NOT IN` new-merchant subquery | windowed first-seen-per-(account, merchant) + anti-join |

## Scalability
- Small `TRANSACTION_TYPES` dimension is **broadcast** in all transaction joins.
- Adaptive Query Execution + skew-join handling enabled in `common/spark.py`.
- `TRANSACTION_ANALYTICS` is written **partitioned by `reporting_period`** (matches the DDL).
- Each window job reads only the required date slice (7/30/90/180/365-day windows).
- The correlated new-merchant subquery is rewritten as a windowed anti-join (no per-row correlation).

## Running

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt         # from the pyspark/ directory

# One job (local CSV in, Parquet lake out):
python -m jobs.staging_customer_360 --source-dir ../data --lake-dir /tmp/lake --run-date 2026-04-10

# Whole pipeline (framework-agnostic driver):
python -m orchestration.run_pipeline --source-dir ../data --lake-dir /tmp/lake --run-date 2026-04-10
```

For production the same jobs run against Teradata/Delta via `JdbcDataIO` (or a
Databricks IO backend) — no path/schema literals are hard-coded (config-driven).

## Testing

Run from the `pyspark/` directory (this keeps the real `pyspark` library
resolvable while exposing `common`/`jobs`/`orchestration` as top-level packages):

```bash
cd pyspark
pytest                      # everything except the slow performance tier
pytest -m unit              # pure transform unit tests
pytest -m functional        # per-job runs on curated fixtures; schema == DDL
pytest -m regression        # parity vs committed legacy outputs (keyed by customer_id)
pytest -m nonfunctional     # validation, audit emission, config, abort-on-failure
pytest -m e2e               # full DAG on a synthetic medium dataset
pytest -m performance       # scaled synthetic data + runtime SLAs (opt-in, slow)
pytest --cov --cov-report=term-missing --cov-fail-under=90
```

| Tier | What it covers |
|------|----------------|
| **unit** | every pure transformation function, on small in-memory DataFrames |
| **functional** | each job end-to-end on the committed CSV fixtures; output schema asserted against the DDL |
| **regression** | PySpark output vs the committed legacy outputs keyed by `customer_id` — exact for staging, tolerance-based for KMeans/logistic |
| **nonfunctional** | ported validation rules, audit-log emission, config-driven params, abort-on-failure |
| **performance** | scaled synthetic data (10M+ customers), runtime SLAs, skew/spill checks |
| **e2e** | one full-DAG run validating all four data products |

## Cutover: strangler / parallel-run
Migrate with a **strangler-fig** approach rather than a big-bang switch:
1. **Shadow-run** the PySpark pipeline alongside the live BTEQ/SAS pipeline on the same `run_date`, writing to a parallel schema.
2. **Reconcile** with the regression harness (row counts, per-`customer_id` parity for staging; tolerance bands for the KMeans segment and logistic probability outputs).
3. **Redirect consumers** table-by-table (start with staging, then each data product) once parity is signed off.
4. **Decommission** the corresponding BTEQ/SAS job only after its PySpark replacement has run clean in production for an agreed bake-in period.

## Logistic-regression fidelity caveat
Spark has no native stepwise selection, so `jobs/stepwise_logistic.py` reproduces
the `PROC LOGISTIC selection=stepwise (slentry=0.10, slstay=0.05)` entry/removal
logic explicitly around `pyspark.ml` `LogisticRegression`. The **same candidate
feature set** is offered to selection (nothing is silently dropped). The
resulting `probability_of_default` is validated on a **tolerance basis** vs SAS
(different optimiser/regularisation), whereas the deterministic composite risk
score, tiers and flags match exactly. Full details in `MIGRATION_NOTES.md`.
