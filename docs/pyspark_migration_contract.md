# PySpark Migration — Interface Contract (risk scoring slice)

Binding contract for the parallel migration of `sas/03_sas_risk_scoring.sas` to
PySpark. Every module below is implemented independently; this document is the
only coordination point, so **signatures, column names and types here are
authoritative**. If SAS behaviour and this document disagree, SAS wins — raise
it rather than silently diverging.

## Ground rules

1. **Package**: everything lives in the top-level `risk_scoring/` package at the
   repo root. Do not create a directory named `pyspark/` — it shadows the
   installed `pyspark` distribution when the repo root is on `sys.path`.
2. **Column naming**: `UPPER_SNAKE` everywhere, matching the Teradata DDL and
   the SAS variable names. `risk_scoring.connections.normalise_columns` handles
   the lower-cased CSV headers.
3. **Numeric types**: intermediate DataFrames carry `double`, because SAS holds
   every numeric variable as a 64-bit float. The `DECIMAL` types of
   `ddl/02_data_product_tables.sql` are applied **once, at the sink**. Use
   `risk_scoring.schemas.analytic_schema()` when reading staging tables.
4. **Purity**: transformation functions take DataFrames and return DataFrames.
   No module reads config from the environment directly — it arrives as a
   `PipelineConfig`. No `print()`; use the audit logger or `logging`.
5. **Determinism**: no `monotonically_increasing_id()`, no unordered
   `first()`/`collect()`-dependent logic. The pipeline must be re-runnable with
   identical output.
6. **No credentials in source.** The SAS `{SAS004}XXXX...` placeholders are not
   credentials; `TeradataConfig.password` reads `$TD_PASSWORD`.

## Already implemented (do not rewrite)

| Module | Contents |
| --- | --- |
| `risk_scoring/config.py` | `PipelineConfig.load()`, `DatabaseRefs`, `TeradataConfig`, `parse_pipeline_cfg()` |
| `risk_scoring/session.py` | `build_spark_session(app_name, master=None, extra_conf=None)` |
| `risk_scoring/schemas.py` | column-name constants, `STG_RISK_FACTORS_SCHEMA`, `CUSTOMER_RISK_SCORES_SCHEMA`, `analytic_schema()` |
| `risk_scoring/connections.py` | `DataBackend`, `CsvBackend`, `JdbcBackend`, `Connections`, `build_backend()` |

`PipelineConfig` fields you may rely on: `model_version` (`"RISK_V4.0"`),
`risk_score_threshold` (`700`), `min_rows` (`1000`, config-driven), `job_name`,
`io_backend` (`"csv"` | `"jdbc"`), `data_dir`, `output_dir`, `databases`,
`teradata`.

## Data available for local development

`PIPELINE_IO_BACKEND=csv` (the default) reads the committed extracts:

| Logical table | File | Rows |
| --- | --- | --- |
| `ETL_STAGING_DB.STG_RISK_FACTORS` | `data/02_bteq_staging/stg_risk_factors.csv` | 478 |
| `ETL_STAGING_DB.STG_CUSTOMER_360` | `data/02_bteq_staging/stg_customer_360.csv` | 478 |
| oracle output | `data/03_sas_data_products/customer_risk_scores.csv` | 407 |

## Module contracts

### `risk_scoring/ingestion.py` — STEP 1 + STEP 2

```python
def read_risk_raw(connections: Connections) -> DataFrame
def build_risk_features(risk_raw: DataFrame) -> DataFrame
```

`read_risk_raw` returns every `STG_RISK_FACTORS` column plus `TENURE_MONTHS`,
`NUM_ACTIVE_ACCOUNTS`, `TOTAL_BALANCE`, `CUSTOMER_STATUS` from
`STG_CUSTOMER_360`, inner-joined on `CUSTOMER_ID`, filtered to
`CUSTOMER_STATUS = 'A'`.

`build_risk_features` adds exactly `BUREAU_SCORE_NORM`, `BALANCE_TREND_RATIO`,
`VELOCITY_RATIO`, `DEFAULT_FLAG` (`int`) and re-imputes `EXTERNAL_CREDIT_SCORE`
in place. All existing columns are preserved.

### `risk_scoring/model.py` — STEP 3

```python
@dataclass
class ModelResult:
    scored: DataFrame          # risk_features + PROB_DEFAULT (double)
    selected_features: list[str]
    coefficients: dict[str, float]
    intercept: float
    steps: list[str]           # human-readable stepwise trace

def train_and_score(risk_features: DataFrame, config: PipelineConfig) -> ModelResult
```

Predictors, in `MODEL` statement order, are `schemas.MODEL_PREDICTORS`; target
is `DEFAULT_FLAG`. Fit and score on the same rows (SAS keeps no hold-out set).
`PROB_DEFAULT` is P(`DEFAULT_FLAG` = 1), i.e. `probability[1]`, because the SAS
`descending` option makes 1 the modelled level.

### `risk_scoring/scoring.py` — STEP 4

```python
def classify_risk(risk_scored: DataFrame, config: PipelineConfig) -> DataFrame
```

Input is `ModelResult.scored`. Output has exactly
`schemas.CUSTOMER_RISK_SCORES_COLUMNS`, in that order, still in analytic
(`double`) types — the sink applies the DECIMAL casts. Note the rename
`PAYMENT_HISTORY_COMP` → `PAYMENT_HISTORY_COMPONENT`.

### `risk_scoring/validation.py` — STEP 5

```python
class ValidationError(RuntimeError): ...

@dataclass
class ValidationResult:
    table: str
    row_count: int
    passed: bool
    errors: list[str]
    warnings: list[str]

def validate_table(
    df: DataFrame,
    *,
    table: str,
    key_cols: Sequence[str] = (),
    not_null: Sequence[str] = (),
    min_rows: int = 1,
    audit: AuditLog | None = None,
) -> ValidationResult
```

Replicates the asymmetry of `%validate_table` exactly: a row count below
`min_rows` and duplicate `key_cols` are **errors** (`passed=False`, and the
caller aborts, mirroring `%abort cancel`); `not_null` violations only
**warn**. SAS also short-circuits — a failed row-count check returns before the
uniqueness check runs.

### `risk_scoring/audit.py` — `%init_audit` / `%log_step`

```python
class AuditLog:
    def __init__(self, job_name: str) -> None
    def log_step(self, *, step: str, status: str, msg: str = "", rowcount: int | None = None) -> None
    def to_dataframe(self, spark: SparkSession) -> DataFrame   # WORK.PIPELINE_AUDIT
    @property
    def records(self) -> list[AuditRecord]
```

`status` ∈ `{"START", "SUCCESS", "WARNING", "ERROR"}`. Emits structured log
records (`logging`, JSON-ish key=value) and accumulates the in-session audit
trail that SAS kept in `WORK.PIPELINE_AUDIT`.

### `risk_scoring/sink.py` — STEP 6

```python
def write_customer_risk_scores(
    df: DataFrame, connections: Connections, *, audit: AuditLog | None = None
) -> int
```

Casts to `CUSTOMER_RISK_SCORES_SCHEMA`, projects to
`CUSTOMER_RISK_SCORES_COLUMNS` and performs a full truncate-load via
`connections.write_data_product(...)` (`mode("overwrite")`), replacing
`DELETE FROM ... ; PROC APPEND FORCE`. Returns the row count written.
`COLLECT STATISTICS` has no PySpark equivalent and is dropped.

### `risk_scoring/driver.py` — orchestration (owned by the integrator)

Wires STEP 1 → STEP 6 in order. Not to be written by the module authors.

## Exact SAS logic to replicate

### STEP 2 — feature preparation

```sas
if EXTERNAL_CREDIT_SCORE <= 0 or EXTERNAL_CREDIT_SCORE = . then EXTERNAL_CREDIT_SCORE = 680;
BUREAU_SCORE_NORM = (EXTERNAL_CREDIT_SCORE - 300) / (850 - 300) * 100;
if AVG_DAILY_BALANCE_90D > 0 then BALANCE_TREND_RATIO = AVG_DAILY_BALANCE_30D / AVG_DAILY_BALANCE_90D;
else BALANCE_TREND_RATIO = 1;
if DEBIT_VELOCITY_30D > 0 then VELOCITY_RATIO = (DEBIT_VELOCITY_7D * (30/7)) / DEBIT_VELOCITY_30D;
else VELOCITY_RATIO = 1;
DEFAULT_FLAG = (PAYMENT_LATE_CNT > 2);
```

Upstream BTEQ semantics that must be preserved: `PAYMENT_ONTIME_PCT` already
defaults to `100.00` when a customer has no payments, `EXTERNAL_CREDIT_SCORE`
defaults to `0` when the bureau join misses (hence the re-imputation to 680
above), and every other measure is `COALESCE`d to `0`.

### STEP 4 — composite score and classification

```sas
CREDIT_RISK_COMPONENT    = max(0, min(100, 100 - BUREAU_SCORE_NORM));
BEHAVIOUR_RISK_COMPONENT = max(0, min(100, 100 - PAYMENT_ONTIME_PCT));
VELOCITY_RISK_COMPONENT  = max(0, min(100, (VELOCITY_RATIO - 1) * 50));
BUREAU_SCORE_COMPONENT   = max(0, min(100, BUREAU_SCORE_NORM));
PAYMENT_HISTORY_COMP     = max(0, min(100, PAYMENT_ONTIME_PCT));

COMPOSITE_RISK_SCORE = round(
    CREDIT_RISK_COMPONENT    * 0.30 +
    BEHAVIOUR_RISK_COMPONENT * 0.25 +
    VELOCITY_RISK_COMPONENT  * 0.15 +
    (100 - BUREAU_SCORE_COMPONENT) * 0.20 +
    (100 - PAYMENT_HISTORY_COMP)   * 0.10
, 0.01);

PROBABILITY_OF_DEFAULT = round(coalesce(PROB_DEFAULT, 0), 0.000001);
```

The redundant `(100 - BUREAU_SCORE_COMPONENT)` and `(100 - PAYMENT_HISTORY_COMP)`
terms are deliberate — port the expression literally.

Risk tiers: `<20 LOW`, `<40 MODERATE`, `<60 ELEVATED`, `<80 HIGH`, else
`CRITICAL`.

Top-two drivers, SAS source:

```sas
array _comp[4] CREDIT_RISK_COMPONENT BEHAVIOUR_RISK_COMPONENT
               VELOCITY_RISK_COMPONENT (100 - BUREAU_SCORE_COMPONENT);
array _lbl[4] $40 _temporary_ (
    'CREDIT_UTILIZATION' 'PAYMENT_BEHAVIOUR' 'TRANSACTION_VELOCITY' 'BUREAU_SCORE');
_max1 = 0; _max2 = 0;
do i = 1 to 4;
    if _comp[i] > _max1 then do;
        _max2 = _max1;
        SECONDARY_RISK_DRIVER = PRIMARY_RISK_DRIVER;
        _max1 = _comp[i];
        PRIMARY_RISK_DRIVER = _lbl[i];
    end;
    else if _comp[i] > _max2 then do;
        _max2 = _comp[i];
        SECONDARY_RISK_DRIVER = _lbl[i];
    end;
end;
```

Both comparisons are **strict**, so ties keep the earlier array index as
primary. Both `_max` values start at `0`, so a component of exactly `0` never
sets a driver — with all-zero components both driver fields stay empty
(SAS: `""`; PySpark: `NULL`). Encode the index into the sort key so
`sort_array` cannot reorder ties.

Flags and metadata:

```sas
SCORE_DELTA_30D      = 0;
WATCH_LIST_FLAG      = ifc(RISK_TIER = 'CRITICAL' and PROBABILITY_OF_DEFAULT > 0.5, 'Y', 'N');
REVIEW_REQUIRED_FLAG = ifc(COMPOSITE_RISK_SCORE >= 60 and VELOCITY_RATIO > 2.0, 'Y', 'N');
MODEL_VERSION        = "&MODEL_VERSION.";   /* config.model_version */
EFFECTIVE_DATE       = today();             /* current_date() */
LOAD_TS              = datetime();          /* current_timestamp() */
```

`WATCH_LIST_FLAG` is evaluated against the **rounded** `PROBABILITY_OF_DEFAULT`.

### STEP 5 call site

```sas
%validate_table(lib=WORK, table=CUSTOMER_RISK_FINAL,
                key_cols=CUSTOMER_ID,
                not_null=CUSTOMER_ID COMPOSITE_RISK_SCORE RISK_TIER,
                min_rows=1000);
```

## Known deviations (document, do not "fix")

| SAS | PySpark | Why |
| --- | --- | --- |
| `selection=stepwise (slentry=0.10, slstay=0.05)` | manual stepwise or full model | MLlib has no stepwise selection |
| `PROC LOGISTIC` Fisher scoring | MLlib LBFGS | different optimiser → `PROBABILITY_OF_DEFAULT` is not bit-reproducible |
| `lackfit` (Hosmer-Lemeshow) | not ported | diagnostic only, no effect on output |
| `COLLECT STATISTICS` | dropped | no equivalent |
| `DELETE` + `PROC APPEND FORCE` | `mode("overwrite")` | target has no partition column; full truncate-load |
| `PROC FREQ` tier distribution | validation report | reporting moves to `validation/` |
| `min_rows=1000` hardcoded | `PipelineConfig.min_rows` | sample dataset scores ~407 customers |

## Definition of done for each module

* Module implemented under `risk_scoring/` with the exact signature above.
* Unit tests under `tests/`, runnable with `python -m pytest tests/ -v`, using a
  local `SparkSession` and small in-memory fixtures (no CSV dependency for the
  pure transforms).
* Every SAS branch covered by at least one test, including the div-by-zero
  guards, the clamps and the tie-breaking.
* No lint errors from `python -m compileall` / `ruff check` if available.
