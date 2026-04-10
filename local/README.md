# Running the Demo Locally

Three options for executing this pipeline with real data, from lightest to
most production-faithful.

---

## Option 1: DuckDB + Python (Zero Install)

**Best for:** Quick demos, no sign-ups, runs anywhere with Python 3.10+.

DuckDB natively supports `QUALIFY`, `ROW_NUMBER()`, and window functions, so
the Teradata SQL translates with minimal changes. Python + scikit-learn
replaces SAS.

```bash
# From the repo root -- single command, everything runs:
uv run local/duckdb/run_demo.py

# Or with custom customer count:
uv run local/duckdb/run_demo.py --customers 10000
```

**What happens:**
1. Generates 5,000 synthetic banking customers (accounts, addresses, transactions)
2. Loads into an in-memory DuckDB database
3. Runs the three BTEQ transformations as DuckDB SQL
4. Runs Python analytics (k-means segmentation, logistic regression risk scoring, IQR anomaly detection)
5. Builds the golden-record master profile
6. Prints summary reports with row counts and distributions

**Requirements:** Python 3.10+ and `uv` (or `pip install faker duckdb pandas scikit-learn`)

---

## Option 2: ClearScape Analytics + Real BTEQ (Actual Teradata)

**Best for:** Running the *actual* BTEQ scripts against real Teradata, unchanged.

[ClearScape Analytics Experience](https://clearscape.teradata.com) is Teradata's
**free cloud sandbox** -- no credit card, no time limit on the environment.
The official `teradata/bteq` Docker image connects to it.

### Setup

```bash
# 1. Sign up at https://clearscape.teradata.com (free)
#    Note your hostname, username, and password.

# 2. Run the setup script (pulls Docker image, tests connection)
cd local/clearscape
./setup.sh

# 3. Load synthetic data into your ClearScape instance
./load_data.sh

# 4. Run the full pipeline (real BTEQ + Python analytics)
./run_pipeline.sh
```

### What happens:
1. `setup.sh` pulls the `teradata/bteq` Docker image and tests your connection
2. `load_data.sh` generates 5,000 synthetic customers and bulk-loads via `teradatasql`
3. `run_pipeline.sh`:
   - Adapts the three BTEQ scripts for ClearScape's single-database model
     (strips `CORE_BANKING_DB.` / `ETL_STAGING_DB.` prefixes via `sed`)
   - Pipes each script through `docker run teradata/bteq` -- **real BTEQ execution**
   - Runs Python analytics against the Teradata staging tables

### Requirements:
- Docker (for `teradata/bteq` image)
- Python 3.10+ with `uv`
- Free ClearScape account

### ClearScape Adaptation Notes

ClearScape gives you a single user database rather than separate
`CORE_BANKING_DB` / `ETL_STAGING_DB` / `DATA_PRODUCTS_DB`. The `run_pipeline.sh`
script handles this automatically by stripping database prefixes from the BTEQ
scripts before execution. The SQL logic is identical.

---

## Option 3: Both (Recommended for Demos)

Run Option 1 first for an instant local demo, then set up Option 2 to show
the same pipeline running against real Teradata.

```bash
# Quick local demo
uv run local/duckdb/run_demo.py

# Then set up real Teradata
cd local/clearscape && ./setup.sh && ./load_data.sh && ./run_pipeline.sh
```

---

## Synthetic Data

All options use the same data generator (`local/generate_data.py`), which
produces:

| Table | Approx Rows (5K customers) | Description |
|-------|---------------------------|-------------|
| CUSTOMERS | 5,000 | Demographics, status, segment |
| ACCOUNTS | ~12,500 | 1-4 accounts per customer (checking, savings, credit, loan) |
| ADDRESSES | ~7,500 | HOME (all), MAIL (30%), WORK (20%) |
| TRANSACTION_TYPES | 15 | Reference data (deposits, POS, fees, etc.) |
| TRANSACTIONS | ~750,000 | 12 months of activity, 50-200 per account |
| CUSTOMER_BUREAU_SCORES | ~10,000 | 1-3 credit bureau pulls per customer |

You can also generate CSVs standalone:

```bash
uv run local/generate_data.py --output-dir ./data --customers 5000
```

---

## Python ↔ SAS Mapping

The Python analytics (`local/python_sas/run_all.py`) faithfully replicate the
four SAS programs:

| SAS Program | Python Equivalent | Key Libraries |
|------------|-------------------|---------------|
| `01_sas_customer_segments.sas` | Step 1: Customer Segmentation | `sklearn.cluster.KMeans`, `sklearn.preprocessing.StandardScaler` |
| `02_sas_txn_analytics.sas` | Step 2: Transaction Analytics | `pandas.qcut`, IQR via `numpy` |
| `03_sas_risk_scoring.sas` | Step 3: Risk Scoring | `sklearn.linear_model.LogisticRegression` |
| `04_sas_data_products.sas` | Step 4: Master Profile | `pandas.merge` (left joins) |

| SAS Construct | Python Equivalent |
|--------------|-------------------|
| `PROC FASTCLUS` | `sklearn.cluster.KMeans` |
| `PROC STDIZE` | `sklearn.preprocessing.StandardScaler` |
| `PROC LOGISTIC` | `sklearn.linear_model.LogisticRegression` |
| `PROC RANK` | `pandas.Series.rank(pct=True)` |
| `PROC MEANS` (IQR) | `numpy.percentile` Q1/Q3 |
| `DATA step MERGE` | `pandas.merge(..., how='left')` |
| `%validate_table` | Row count + `.duplicated()` + `.isna()` checks |
| `LIBNAME teradata` | `teradatasql.connect()` or `duckdb.connect()` |
