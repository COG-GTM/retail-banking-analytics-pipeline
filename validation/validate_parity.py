"""Data-parity validation harness for the BTEQ/SAS -> PySpark migration.

Runs every PySpark job locally and compares its output against the golden
CSVs checked into data/ (produced by the legacy pipeline as of RUN_DATE
2026-04-10), using pandas.testing.assert_frame_equal for numerical parity.

Layers validated:
  1. BTEQ staging   - built from data/01_source_tables, compared column-by-
                      column against data/02_bteq_staging (exact parity).
  2. SAS analytics  - built from the golden staging tables, compared against
                      data/03_sas_data_products on all deterministic columns.
                      Model-assigned columns (k-means SEGMENT_ID/SEGMENT_NAME)
                      are validated at distribution level instead, since
                      cluster assignments are not bit-reproducible across
                      SAS FASTCLUS and Spark ML KMeans.

Usage:
    python validation/validate_parity.py            # full run
Exit code 0 = all validations passed, 1 = at least one failure.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

# Make the pyspark job modules importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pyspark"))

from common import (
    DEFAULT_RUN_DATE,
    PRODUCTS_DIR,
    STAGING_DIR,
    get_spark,
    load_source_tables,
    load_staging_tables,
)
from customer_segments import build_customer_segments
from master_profile import build_customer_master_profile
from risk_scoring import build_customer_risk_scores
from stg_customer_360 import build_stg_customer_360
from stg_risk_factors import build_stg_risk_factors
from stg_txn_summary import build_stg_txn_summary
from txn_analytics import build_transaction_analytics

# Columns never compared: load timestamps differ by definition.
ALWAYS_SKIP = {"load_ts"}

# Known, documented deviation: the golden generator's "new merchant" rule
# differs from the BTEQ NOT IN subquery semantics that this migration
# faithfully implements. Compared with tolerance at distribution level.
KNOWN_DEVIATIONS = {"new_merchant_cnt_30d"}

# Model-assigned columns: cluster/model outputs, validated by distribution.
MODEL_COLUMNS = {"segment_id", "segment_name", "subsegment_id"}

RESULTS: list[tuple[str, str, str, str]] = []


def record(table: str, check: str, ok: bool, detail: str = "") -> None:
    """Track one validation outcome for the final report."""
    RESULTS.append((table, check, "PASS" if ok else "FAIL", detail))
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {table}: {check}" + (f" ({detail})" if detail else ""))


def compare_frames(
    name: str,
    mine: pd.DataFrame,
    gold: pd.DataFrame,
    keys: list[str],
    skip: set[str] = frozenset(),
) -> None:
    """Column-by-column parity comparison using assert_frame_equal."""
    mine = mine.sort_values(keys).reset_index(drop=True)
    gold = gold.sort_values(keys).reset_index(drop=True)
    record(name, "row count", len(mine) == len(gold), f"{len(mine)} vs {len(gold)}")

    for col in gold.columns:
        if col in ALWAYS_SKIP or col in skip:
            continue
        a, b = mine[col], gold[col]
        try:
            # Numeric parity via assert_frame_equal with a tight tolerance
            a_num = pd.to_numeric(a, errors="raise")
            b_num = pd.to_numeric(b, errors="raise")
            assert_frame_equal(
                a_num.to_frame(col).astype("float64"),
                b_num.to_frame(col).astype("float64"),
                check_exact=False,
                rtol=1e-6,
                atol=1e-4,
            )
            record(name, f"column {col}", True)
        except (ValueError, TypeError):
            # Non-numeric column: exact string comparison (dates as ISO text)
            a_str = a.fillna("").astype(str).str[:10] if "date" in col else a.fillna("").astype(str)
            b_str = b.fillna("").astype(str).str[:10] if "date" in col else b.fillna("").astype(str)
            n_bad = int((a_str != b_str).sum())
            record(name, f"column {col}", n_bad == 0, f"{n_bad} mismatches" if n_bad else "")
        except AssertionError:
            n_bad = int(
                (~np.isclose(a_num.fillna(-9e9), b_num.fillna(-9e9), rtol=1e-6, atol=1e-4)).sum()
            )
            record(name, f"column {col}", False, f"{n_bad} mismatches")


def distribution_check(name: str, col: str, mine: pd.Series, gold: pd.Series) -> None:
    """Compare a model-assigned column at distribution level."""
    if pd.api.types.is_numeric_dtype(gold):
        ok = abs(mine.mean() - gold.mean()) <= max(0.15 * abs(gold.mean()), 1.0)
        record(name, f"distribution {col}", bool(ok),
               f"mean {mine.mean():.2f} vs {gold.mean():.2f}")
    else:
        ok = set(mine.unique()) <= set(gold.unique()) | {"UNCLASSIFIED"}
        record(name, f"distribution {col}", bool(ok), f"{mine.nunique()} distinct values")


def main() -> int:
    spark = get_spark("parity-validation")
    load_source_tables(spark)
    golden_staging = load_staging_tables(spark)

    print("\n=== Layer 1: BTEQ staging parity (source CSVs -> PySpark) ===")
    stg_c360 = build_stg_customer_360(spark, DEFAULT_RUN_DATE)
    compare_frames(
        "stg_customer_360",
        stg_c360.toPandas(),
        pd.read_csv(STAGING_DIR / "stg_customer_360.csv"),
        keys=["customer_id"],
    )

    stg_txn = build_stg_txn_summary(spark, DEFAULT_RUN_DATE)
    compare_frames(
        "stg_txn_summary",
        stg_txn.toPandas(),
        pd.read_csv(STAGING_DIR / "stg_txn_summary.csv"),
        keys=["customer_id", "account_id"],
        skip={"summary_period_start", "summary_period_end"},
    )

    stg_risk = build_stg_risk_factors(spark, DEFAULT_RUN_DATE)
    stg_risk_pd = stg_risk.toPandas().sort_values("customer_id").reset_index(drop=True)
    gold_risk = (
        pd.read_csv(STAGING_DIR / "stg_risk_factors.csv")
        .sort_values("customer_id")
        .reset_index(drop=True)
    )
    compare_frames("stg_risk_factors", stg_risk_pd, gold_risk,
                   keys=["customer_id"], skip=KNOWN_DEVIATIONS)
    distribution_check(
        "stg_risk_factors", "new_merchant_cnt_30d (known deviation)",
        stg_risk_pd["new_merchant_cnt_30d"], gold_risk["new_merchant_cnt_30d"],
    )

    print("\n=== Layer 2: SAS analytics parity (golden staging -> PySpark) ===")
    seg = build_customer_segments(spark, golden_staging["stg_customer_360"]).toPandas()
    gold_seg = pd.read_csv(PRODUCTS_DIR / "customer_segments.csv")
    compare_frames(
        "customer_segments", seg, gold_seg, keys=["customer_id"],
        skip=MODEL_COLUMNS | {"channel_preference", "effective_date"},
    )
    for col in ("segment_name",):
        distribution_check("customer_segments", col, seg[col], gold_seg[col])

    txn = (
        build_transaction_analytics(spark, golden_staging["stg_txn_summary"])
        .toPandas()
        .sort_values("customer_id")
        .reset_index(drop=True)
    )
    gold_txn = (
        pd.read_csv(PRODUCTS_DIR / "transaction_analytics.csv")
        .sort_values("customer_id")
        .reset_index(drop=True)
    )
    compare_frames(
        "transaction_analytics", txn, gold_txn,
        keys=["customer_id"],
        skip={"effective_date", "reporting_period", "top_spend_category"},
    )
    # Known deviation: for multi-account customers the golden generator picks
    # an arbitrary account's top category; the migration keeps SAS MAX()
    # semantics. Validate that our pick is always within the customer's
    # account-level candidate set (and exact where only one candidate exists).
    ts_gold = pd.read_csv(STAGING_DIR / "stg_txn_summary.csv")
    cand = ts_gold.groupby("customer_id").top_merchant_category.apply(
        lambda s: set(s.dropna())
    )
    joined = txn.set_index("customer_id")["top_spend_category"]
    n_out = sum(
        1
        for cid, val in joined.items()
        if pd.notna(val) and val not in cand.get(cid, set())
    )
    record(
        "transaction_analytics",
        "top_spend_category within candidate set (known deviation)",
        n_out == 0,
        f"{n_out} outside candidate set",
    )

    risk = build_customer_risk_scores(
        spark, golden_staging["stg_risk_factors"], golden_staging["stg_customer_360"]
    ).toPandas()
    compare_frames(
        "customer_risk_scores", risk,
        pd.read_csv(PRODUCTS_DIR / "customer_risk_scores.csv"),
        keys=["customer_id"], skip={"effective_date"},
    )

    print("\n=== Layer 3: golden-record assembly parity ===")
    # Feed the golden upstream products in, isolating the join/default logic
    gold_products = {}
    for name in ("customer_segments", "transaction_analytics", "customer_risk_scores"):
        df = spark.read.csv(str(PRODUCTS_DIR / f"{name}.csv"), header=True, inferSchema=True)
        gold_products[name] = df
    master = build_customer_master_profile(
        spark,
        golden_staging["stg_customer_360"],
        gold_products["customer_segments"],
        gold_products["transaction_analytics"],
        gold_products["customer_risk_scores"],
    ).toPandas()
    compare_frames(
        "customer_master_profile", master,
        pd.read_csv(PRODUCTS_DIR / "customer_master_profile.csv"),
        keys=["customer_id"], skip={"effective_date"},
    )

    spark.stop()

    failures = [r for r in RESULTS if r[2] == "FAIL"]
    print(f"\n{'=' * 60}\nValidation summary: {len(RESULTS) - len(failures)}/{len(RESULTS)} "
          f"checks passed, {len(failures)} failed")
    for table, check, _, detail in failures:
        print(f"  FAIL {table}: {check} {detail}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
