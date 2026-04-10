# /// script
# requires-python = ">=3.10"
# dependencies = ["pandas>=2.0", "scikit-learn>=1.3", "numpy>=1.24"]
# ///
"""
run_all.py – Python analytics runner replacing four SAS programs.

Supports two backends:
  - Teradata (ClearScape) via teradatasql
  - DuckDB for local / demo usage

Usage:
  python run_all.py --backend teradata --host HOST --user USER --password PASS
  python run_all.py --backend duckdb --db-path /path/to/pipeline_demo.duckdb
"""
from __future__ import annotations

import argparse
import sys
import time
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Model version constants
# ---------------------------------------------------------------------------
SEG_MODEL_VERSION = "SEG_V3.2"
TXN_MODEL_VERSION = "TXN_V2.1"
RISK_MODEL_VERSION = "RISK_V4.0"
MASTER_MODEL_VERSION = "MASTER_V1.5"


# ====================================================================== #
#  Database Abstraction Layer                                             #
# ====================================================================== #
class DatabaseBackend(ABC):
    """Thin abstraction so every step speaks the same API."""

    @abstractmethod
    def read_table(self, table_name: str) -> pd.DataFrame: ...

    @abstractmethod
    def write_table(self, table_name: str, df: pd.DataFrame, schema: str) -> None: ...

    @abstractmethod
    def execute(self, sql: str) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


class TeradataBackend(DatabaseBackend):
    """ClearScape Teradata – flat table names, no schema prefix."""

    def __init__(self, host: str, user: str, password: str) -> None:
        try:
            import teradatasql  # type: ignore[import-untyped]
        except ImportError:
            sys.exit("teradatasql is not installed. pip install teradatasql")
        self._conn = teradatasql.connect(
            host=host, user=user, password=password
        )

    def read_table(self, table_name: str) -> pd.DataFrame:
        # Strip any schema prefix – Teradata uses flat names in ClearScape
        flat = table_name.rsplit(".", 1)[-1].upper()
        return pd.read_sql(f"SELECT * FROM {flat}", self._conn)

    def write_table(self, table_name: str, df: pd.DataFrame, schema: str) -> None:
        flat = table_name.upper()
        cur = self._conn.cursor()
        try:
            cur.execute(f"DELETE FROM {flat}")
        except Exception:
            pass  # table may not exist yet
        df.to_sql(flat, self._conn, if_exists="replace", index=False)

    def execute(self, sql: str) -> None:
        self._conn.cursor().execute(sql)

    def close(self) -> None:
        self._conn.close()


class DuckDBBackend(DatabaseBackend):
    """Local DuckDB – tables use schema.table_name."""

    def __init__(self, db_path: str) -> None:
        try:
            import duckdb  # type: ignore[import-untyped]
        except ImportError:
            sys.exit("duckdb is not installed. pip install duckdb")
        self._conn = duckdb.connect(db_path)
        self._conn.execute("CREATE SCHEMA IF NOT EXISTS data_products")

    def read_table(self, table_name: str) -> pd.DataFrame:
        return self._conn.execute(f"SELECT * FROM {table_name}").fetchdf()

    def write_table(self, table_name: str, df: pd.DataFrame, schema: str) -> None:
        fqn = f"{schema}.{table_name}"
        self._conn.execute(f"DROP TABLE IF EXISTS {fqn}")
        self._conn.execute(f"CREATE TABLE {fqn} AS SELECT * FROM df")

    def execute(self, sql: str) -> None:
        self._conn.execute(sql)

    def close(self) -> None:
        self._conn.close()


# ====================================================================== #
#  Helper: table name resolution                                          #
# ====================================================================== #
def _stg(table: str, backend: DatabaseBackend) -> str:
    """Return the fully-qualified staging table name."""
    if isinstance(backend, DuckDBBackend):
        return f"etl_staging.{table}"
    return table  # Teradata: flat


def _prod(table: str, backend: DatabaseBackend) -> str:
    """Return the fully-qualified data-product table name."""
    if isinstance(backend, DuckDBBackend):
        return f"data_products.{table}"
    return table.upper()


# ====================================================================== #
#  Step 1 – Customer Segmentation  (replaces 01_sas_customer_segments)    #
# ====================================================================== #
def step1_customer_segmentation(db: DatabaseBackend) -> pd.DataFrame:
    t0 = time.time()
    df = db.read_table(_stg("stg_customer_360", db))
    df.columns = df.columns.str.lower()

    # ---- feature engineering ------------------------------------------------
    for col in ("has_checking", "has_savings", "has_credit", "has_loan"):
        df[col] = df[col].astype(str).str.upper()

    df["product_breadth"] = df[["has_checking", "has_savings", "has_credit", "has_loan"]].apply(
        lambda r: np.mean([v == "Y" for v in r]), axis=1
    )

    df["tenure_group"] = pd.cut(
        df["tenure_months"],
        bins=[-np.inf, 12, 36, 84, np.inf],
        labels=["NEW", "DEVELOPING", "ESTABLISHED", "LOYAL"],
    )

    df["age_group"] = pd.cut(
        df["age"],
        bins=[-np.inf, 25, 40, 56, 75, np.inf],
        labels=["GEN_Z", "MILLENNIAL", "GEN_X", "BOOMER", "SILENT"],
    )

    df["balance_tier"] = pd.cut(
        df["total_balance"],
        bins=[-np.inf, 1_000, 10_000, 100_000, np.inf],
        labels=["LOW", "MODERATE", "AFFLUENT", "HIGH_NET_WORTH"],
    )

    # ---- clustering ---------------------------------------------------------
    df["log_balance"] = np.log1p(df["total_balance"].clip(lower=0))
    df["account_activity_ratio"] = np.where(
        df["num_accounts"] > 0,
        df["num_active_accounts"] / df["num_accounts"],
        0,
    )

    feat_cols = [
        "log_balance", "tenure_months", "credit_utilization_pct",
        "product_breadth", "account_activity_ratio", "age",
    ]
    X = df[feat_cols].fillna(0).values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    km = KMeans(n_clusters=5, random_state=42, n_init=10)
    df["cluster_raw"] = km.fit_predict(X_scaled)

    # sort clusters by mean balance descending
    cluster_means = df.groupby("cluster_raw")["total_balance"].mean().sort_values(ascending=False)
    label_map_ordered = dict(zip(
        cluster_means.index,
        ["PREMIUM_WEALTH", "ENGAGED_MAINSTREAM", "GROWING_DIGITAL",
         "CREDIT_DEPENDENT", "VALUE_BASIC"],
    ))
    df["customer_segment"] = df["cluster_raw"].map(label_map_ordered)

    # ---- scores & flags -----------------------------------------------------
    df["lifetime_value_score"] = (
        df["log_balance"] * df["tenure_months"] * df["product_breadth"] * 10
    ).round(2)

    df["engagement_score"] = (df["account_activity_ratio"] * 100).round(2)
    df["digital_adoption_score"] = 0.0

    df["cross_sell_flag"] = np.where(
        (df["product_breadth"] < 0.50) & (df["engagement_score"] >= 75), "Y", "N"
    )
    df["upsell_flag"] = np.where(
        (df["balance_tier"].astype(str) == "MODERATE") & (df["tenure_group"].astype(str) != "NEW"),
        "Y", "N",
    )
    df["retention_risk_flag"] = np.where(
        (df["engagement_score"] < 50) & (df["tenure_months"] >= 60), "Y", "N"
    )

    df["model_version"] = SEG_MODEL_VERSION
    df["scored_at"] = pd.Timestamp.utcnow()

    out_cols = [
        "customer_id", "customer_segment", "tenure_group", "age_group",
        "balance_tier", "product_breadth", "lifetime_value_score",
        "engagement_score", "digital_adoption_score",
        "cross_sell_flag", "upsell_flag", "retention_risk_flag",
        "model_version", "scored_at",
    ]
    result = df[out_cols].copy()

    db.write_table("customer_segments", result, "data_products")

    elapsed = time.time() - t0
    print(f"[Step 1] CUSTOMER_SEGMENTATION")
    print(f"  Rows:  {len(result)}")
    print(f"  Time:  {elapsed:.1f}s")
    seg_counts = result["customer_segment"].value_counts().to_dict()
    print(f"  Key metrics: segment distribution = {seg_counts}")
    return result


# ====================================================================== #
#  Step 2 – Transaction Analytics  (replaces 02_sas_txn_analytics)        #
# ====================================================================== #
def step2_transaction_analytics(db: DatabaseBackend) -> pd.DataFrame:
    t0 = time.time()
    txn = db.read_table(_stg("stg_txn_summary", db))
    txn.columns = txn.columns.str.lower()

    # ---- aggregate to customer level ----------------------------------------
    agg = txn.groupby("customer_id").agg(
        total_accounts=("account_id", "nunique"),
        active_accounts=("days_since_last_txn", lambda s: (s <= 30).sum()),
        total_transactions=("total_txn_count", "sum"),
        total_debit_amt=("total_debit_amt", "sum"),
        total_credit_amt=("total_credit_amt", "sum"),
        total_fees=("total_fees", "sum"),
        avg_transaction_size=("avg_txn_amt", "mean"),
        pct_web_mean=("pct_web", "mean"),
        pct_mobile_mean=("pct_mobile", "mean"),
        top_spend_category=("top_spend_category", lambda s: s.mode().iloc[0] if len(s.mode()) > 0 else "UNKNOWN"),
    ).reset_index()

    agg["net_cash_flow"] = agg["total_credit_amt"] - agg["total_debit_amt"]
    agg["digital_txn_pct"] = (agg["pct_web_mean"] + agg["pct_mobile_mean"]).round(2)

    # ---- trend --------------------------------------------------------------
    agg["cash_flow_trend"] = np.select(
        [
            agg["net_cash_flow"] > agg["avg_transaction_size"] * 5,
            agg["net_cash_flow"] < -agg["avg_transaction_size"] * 5,
        ],
        ["UP", "DOWN"],
        default="STABLE",
    )

    # ---- percentile ---------------------------------------------------------
    agg["spend_percentile"] = agg["total_debit_amt"].rank(pct=True).round(4)

    # ---- anomaly (IQR method) -----------------------------------------------
    q1 = agg["total_debit_amt"].quantile(0.25)
    q3 = agg["total_debit_amt"].quantile(0.75)
    iqr = q3 - q1
    anomaly_threshold = agg["total_debit_amt"].median() + 3 * iqr
    agg["anomaly_flag"] = np.where(agg["total_debit_amt"] > anomaly_threshold, "Y", "N")

    # ---- revenue estimates --------------------------------------------------
    agg["fee_income"] = agg["total_fees"].round(2)
    agg["interest_income"] = (agg["total_debit_amt"] * 0.02).round(2)

    agg["effective_date"] = pd.Timestamp.utcnow().date()
    agg["model_version"] = TXN_MODEL_VERSION

    out_cols = [
        "customer_id", "total_accounts", "active_accounts",
        "total_transactions", "total_debit_amt", "total_credit_amt",
        "net_cash_flow", "avg_transaction_size", "digital_txn_pct",
        "top_spend_category", "cash_flow_trend", "spend_percentile",
        "anomaly_flag", "fee_income", "interest_income",
        "effective_date", "model_version",
    ]
    result = agg[out_cols].copy()

    db.write_table("transaction_analytics", result, "data_products")

    elapsed = time.time() - t0
    print(f"[Step 2] TRANSACTION_ANALYTICS")
    print(f"  Rows:  {len(result)}")
    print(f"  Time:  {elapsed:.1f}s")
    anomaly_n = (result["anomaly_flag"] == "Y").sum()
    print(f"  Key metrics: anomalies={anomaly_n}, "
          f"median_debit={result['total_debit_amt'].median():.0f}, "
          f"trend distribution = {result['cash_flow_trend'].value_counts().to_dict()}")
    return result


# ====================================================================== #
#  Step 3 – Risk Scoring  (replaces 03_sas_risk_scoring)                  #
# ====================================================================== #
def step3_risk_scoring(db: DatabaseBackend) -> pd.DataFrame:
    t0 = time.time()
    risk = db.read_table(_stg("stg_risk_factors", db))
    cust = db.read_table(_stg("stg_customer_360", db))
    risk.columns = risk.columns.str.lower()
    cust.columns = cust.columns.str.lower()

    # inner join – active customers only
    cust_active = cust[cust["customer_status"].str.upper() == "ACTIVE"]
    df = risk.merge(cust_active[["customer_id", "tenure_months"]], on="customer_id", how="inner")

    # ---- feature prep -------------------------------------------------------
    df["bureau_score"] = df["bureau_score"].where(df["bureau_score"] > 0, 680)
    df["bureau_score_norm"] = ((df["bureau_score"] - 300) / (850 - 300) * 100).clip(0, 100)

    df["balance_trend_ratio"] = np.where(
        df["avg_daily_balance_90d"] > 0,
        df["avg_daily_balance_30d"] / df["avg_daily_balance_90d"],
        1.0,
    )
    df["velocity_ratio"] = np.where(
        df["debit_velocity_30d"] > 0,
        (df["debit_velocity_7d"] * 30 / 7) / df["debit_velocity_30d"],
        1.0,
    )

    df["default_flag"] = np.where(df["payment_late_cnt"] > 2, 1, 0)

    # ---- logistic regression model ------------------------------------------
    feature_cols = [
        "bureau_score_norm", "credit_util_ratio", "payment_ontime_pct",
        "balance_volatility", "velocity_ratio", "account_overdraft_cnt",
        "large_withdrawal_cnt", "high_risk_merchant_cnt", "tenure_months",
    ]
    X = df[feature_cols].fillna(0).values
    y = df["default_flag"].values

    if len(np.unique(y)) < 2:
        # only one class – skip fitting
        df["prob_default"] = 0.0
    else:
        lr = LogisticRegression(max_iter=1000, random_state=42)
        lr.fit(X, y)
        df["prob_default"] = lr.predict_proba(X)[:, 1].round(4)

    # ---- composite risk score (0-100, higher = riskier) ---------------------
    df["credit_risk_component"] = (100 - df["bureau_score_norm"]).clip(0, 100)
    df["behaviour_risk_component"] = (100 - df["payment_ontime_pct"]).clip(0, 100)
    df["velocity_risk_component"] = ((df["velocity_ratio"] - 1) * 50).clip(0, 100)

    df["composite_risk_score"] = (
        0.30 * df["credit_risk_component"]
        + 0.25 * df["behaviour_risk_component"]
        + 0.15 * df["velocity_risk_component"]
        + 0.20 * (100 - df["bureau_score_norm"]).clip(0, 100)
        + 0.10 * (100 - df["payment_ontime_pct"]).clip(0, 100)
    ).round(2)

    # ---- risk tier ----------------------------------------------------------
    df["risk_tier"] = pd.cut(
        df["composite_risk_score"],
        bins=[-np.inf, 20, 40, 60, 80, np.inf],
        labels=["LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"],
    )

    # ---- primary risk drivers (top 2 components) ----------------------------
    component_map = {
        "credit_risk_component": "CREDIT_UTILIZATION",
        "behaviour_risk_component": "PAYMENT_BEHAVIOUR",
        "velocity_risk_component": "TRANSACTION_VELOCITY",
        "bureau_component": "BUREAU_SCORE",
    }
    # build a small frame of component values per row for ranking
    comp_df = pd.DataFrame({
        "credit_risk_component": df["credit_risk_component"],
        "behaviour_risk_component": df["behaviour_risk_component"],
        "velocity_risk_component": df["velocity_risk_component"],
        "bureau_component": (100 - df["bureau_score_norm"]).clip(0, 100),
    })

    def _top2_drivers(row: pd.Series) -> str:
        top = row.nlargest(2).index.tolist()
        return ",".join(component_map[c] for c in top)

    df["primary_risk_drivers"] = comp_df.apply(_top2_drivers, axis=1)

    # ---- flags --------------------------------------------------------------
    df["watch_list_flag"] = np.where(
        (df["risk_tier"].astype(str) == "CRITICAL") & (df["prob_default"] > 0.5),
        "Y", "N",
    )
    df["review_required_flag"] = np.where(
        (df["composite_risk_score"] >= 60) & (df["velocity_ratio"] > 2.0),
        "Y", "N",
    )

    df["model_version"] = RISK_MODEL_VERSION
    df["scored_at"] = pd.Timestamp.utcnow()

    out_cols = [
        "customer_id", "composite_risk_score", "risk_tier",
        "prob_default", "credit_risk_component", "behaviour_risk_component",
        "velocity_risk_component", "balance_trend_ratio", "velocity_ratio",
        "primary_risk_drivers", "watch_list_flag", "review_required_flag",
        "model_version", "scored_at",
    ]
    result = df[out_cols].copy()

    db.write_table("customer_risk_scores", result, "data_products")

    elapsed = time.time() - t0
    print(f"[Step 3] RISK_SCORING")
    print(f"  Rows:  {len(result)}")
    print(f"  Time:  {elapsed:.1f}s")
    tier_dist = result["risk_tier"].value_counts().to_dict()
    watch_n = (result["watch_list_flag"] == "Y").sum()
    print(f"  Key metrics: risk tier distribution = {tier_dist}, watch_list={watch_n}")
    return result


# ====================================================================== #
#  Step 4 – Master Profile  (replaces 04_sas_data_products)               #
# ====================================================================== #
def step4_master_profile(db: DatabaseBackend) -> pd.DataFrame:
    t0 = time.time()
    cust = db.read_table(_stg("stg_customer_360", db))
    cust.columns = cust.columns.str.lower()

    seg = db.read_table(_prod("customer_segments", db))
    seg.columns = seg.columns.str.lower()

    txn = db.read_table(_prod("transaction_analytics", db))
    txn.columns = txn.columns.str.lower()

    risk = db.read_table(_prod("customer_risk_scores", db))
    risk.columns = risk.columns.str.lower()

    # keep only latest effective_date rows for txn
    if "effective_date" in txn.columns:
        latest = txn["effective_date"].max()
        txn = txn[txn["effective_date"] == latest]

    # ---- left joins ---------------------------------------------------------
    base = cust[["customer_id"]].drop_duplicates()

    result = base.merge(
        seg[["customer_id", "customer_segment", "lifetime_value_score",
             "engagement_score", "digital_adoption_score",
             "cross_sell_flag", "upsell_flag", "retention_risk_flag"]],
        on="customer_id", how="left",
    )

    txn_cols = [
        "customer_id", "total_accounts", "active_accounts",
        "total_transactions", "total_debit_amt", "total_credit_amt",
        "net_cash_flow", "avg_transaction_size", "digital_txn_pct",
        "top_spend_category", "cash_flow_trend", "spend_percentile",
        "anomaly_flag", "fee_income", "interest_income",
    ]
    result = result.merge(txn[[c for c in txn_cols if c in txn.columns]],
                          on="customer_id", how="left")

    risk_cols = [
        "customer_id", "composite_risk_score", "risk_tier",
        "prob_default", "primary_risk_drivers",
        "watch_list_flag", "review_required_flag",
    ]
    # rename watch/review to avoid suffix collision with segment flags
    risk_sub = risk[[c for c in risk_cols if c in risk.columns]].rename(columns={
        "watch_list_flag": "risk_watch_list_flag",
        "review_required_flag": "risk_review_required_flag",
    })
    result = result.merge(risk_sub, on="customer_id", how="left")

    # ---- defaults for missing -----------------------------------------------
    result["customer_segment"] = result["customer_segment"].fillna("UNCLASSIFIED")
    for col in ("lifetime_value_score", "engagement_score", "digital_adoption_score"):
        result[col] = result[col].fillna(0)
    for col in ("cross_sell_flag", "upsell_flag", "retention_risk_flag"):
        result[col] = result[col].fillna("N")

    txn_num_cols = [
        "total_accounts", "active_accounts", "total_transactions",
        "total_debit_amt", "total_credit_amt", "net_cash_flow",
        "avg_transaction_size", "digital_txn_pct", "spend_percentile",
        "fee_income", "interest_income",
    ]
    for col in txn_num_cols:
        if col in result.columns:
            result[col] = result[col].fillna(0)

    result["risk_tier"] = result["risk_tier"].fillna("UNKNOWN")
    result["risk_watch_list_flag"] = result["risk_watch_list_flag"].fillna("N")
    result["risk_review_required_flag"] = result["risk_review_required_flag"].fillna("N")
    # composite_risk_score and prob_default left as NaN → None

    result["model_version"] = MASTER_MODEL_VERSION
    result["created_at"] = pd.Timestamp.utcnow()

    db.write_table("customer_master_profile", result, "data_products")

    elapsed = time.time() - t0
    print(f"[Step 4] MASTER_PROFILE")
    print(f"  Rows:  {len(result)}")
    print(f"  Time:  {elapsed:.1f}s")
    seg_dist = result["customer_segment"].value_counts().to_dict()
    print(f"  Key metrics: segment distribution in master = {seg_dist}")
    return result


# ====================================================================== #
#  CLI / main                                                             #
# ====================================================================== #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Python analytics runner – replaces SAS pipeline programs.",
    )
    p.add_argument(
        "--backend", required=True, choices=["teradata", "duckdb"],
        help="Database backend to use.",
    )
    # Teradata options
    p.add_argument("--host", default=None, help="Teradata host (ClearScape).")
    p.add_argument("--user", default=None, help="Teradata user.")
    p.add_argument("--password", default=None, help="Teradata password.")
    # DuckDB options
    p.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    return p


def _connect(args: argparse.Namespace) -> DatabaseBackend:
    if args.backend == "teradata":
        if not all([args.host, args.user, args.password]):
            sys.exit("Teradata backend requires --host, --user, and --password.")
        return TeradataBackend(args.host, args.user, args.password)
    else:
        if not args.db_path:
            sys.exit("DuckDB backend requires --db-path.")
        return DuckDBBackend(args.db_path)


def main() -> None:
    args = build_parser().parse_args()
    db = _connect(args)

    print("=" * 60)
    print("  Python Analytics Runner")
    print(f"  Backend : {args.backend}")
    print("=" * 60)

    total_t0 = time.time()
    products: dict[str, int] = {}

    try:
        seg = step1_customer_segmentation(db)
        products["customer_segments"] = len(seg)
        print()

        txn = step2_transaction_analytics(db)
        products["transaction_analytics"] = len(txn)
        print()

        risk = step3_risk_scoring(db)
        products["customer_risk_scores"] = len(risk)
        print()

        master = step4_master_profile(db)
        products["customer_master_profile"] = len(master)
        print()
    finally:
        db.close()

    total_elapsed = time.time() - total_t0

    # ---- summary table ------------------------------------------------------
    print("=" * 60)
    print("  DATA PRODUCTS SUMMARY")
    print("-" * 60)
    print(f"  {'Table':<30} {'Rows':>10}")
    print("-" * 60)
    for name, rows in products.items():
        print(f"  {name:<30} {rows:>10,}")
    print("-" * 60)
    print(f"  Total elapsed: {total_elapsed:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
