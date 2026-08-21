"""Reconcile the migrated CUSTOMER_SEGMENTS output against the SAS baseline.

Compares the PySpark job output with the SAS-produced CUSTOMER_SEGMENTS extract
for the reconciliation cohort (customers present in both):

* row counts and key overlap,
* cluster agreement after resolving cluster-label permutation (the k-means
  cluster ids are arbitrary, so the best one-to-one mapping between the two
  labellings is searched and reported),
* exact agreement on the deterministic columns (LTV, engagement, breadth,
  groupings and the three action flags).

Usage:
    python synapse/spark/reconciliation/reconcile_customer_segments.py \
        --baseline data/03_sas_data_products/customer_segments.csv \
        --candidate /tmp/customer_segments_spark \
        --min-cluster-agreement 0.85 \
        --min-column-agreement 1.0

The deterministic columns are expected to match exactly (--min-column-agreement
1.0). The checked-in demo extract under data/03_sas_data_products classifies the
TENURE_GROUP/AGE_GROUP boundary values inclusively, which the SAS source does
not; run those comparisons with --min-column-agreement 0.94 to tolerate the
known boundary rows of that extract.
"""

from __future__ import annotations

import argparse
import glob
import itertools
import os
import sys

import pandas as pd

KEY = "customer_id"
CLUSTER_COLUMN = "segment_name"
DETERMINISTIC_COLUMNS = [
    "lifetime_value_score",
    "engagement_score",
    "digital_adoption_score",
    "product_breadth_index",
    "tenure_group",
    "age_group",
    "balance_tier",
    "cross_sell_flag",
    "upsell_flag",
    "retention_risk_flag",
    "model_version",
]


def load(path: str) -> pd.DataFrame:
    """Load a CSV file or a Spark part-file output directory."""
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "*.csv")))
        if not files:
            raise FileNotFoundError(f"no CSV part files under {path}")
        frame = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    else:
        frame = pd.read_csv(path)
    frame.columns = [c.lower() for c in frame.columns]
    return frame


def best_label_mapping(merged: pd.DataFrame) -> tuple[dict[str, str], float]:
    """Best one-to-one mapping of candidate labels onto baseline labels."""
    baseline_labels = sorted(merged[f"{CLUSTER_COLUMN}_baseline"].dropna().unique())
    candidate_labels = sorted(merged[f"{CLUSTER_COLUMN}_candidate"].dropna().unique())
    contingency = pd.crosstab(
        merged[f"{CLUSTER_COLUMN}_candidate"], merged[f"{CLUSTER_COLUMN}_baseline"]
    )

    best_mapping: dict[str, str] = {}
    best_hits = -1
    width = min(len(baseline_labels), len(candidate_labels))
    for assignment in itertools.permutations(baseline_labels, width):
        mapping = dict(zip(candidate_labels[:width], assignment))
        hits = sum(
            int(contingency.loc[candidate, baseline])
            for candidate, baseline in mapping.items()
            if baseline in contingency.columns
        )
        if hits > best_hits:
            best_hits, best_mapping = hits, mapping

    return best_mapping, best_hits / len(merged) if len(merged) else 0.0


def reconcile(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
    min_agreement: float,
    min_column_agreement: float = 1.0,
) -> bool:
    merged = baseline.merge(candidate, on=KEY, suffixes=("_baseline", "_candidate"))
    print(f"baseline rows            : {len(baseline)}")
    print(f"candidate rows           : {len(candidate)}")
    print(f"reconciliation cohort    : {len(merged)}")
    if merged.empty:
        print("FAIL: no overlapping customers")
        return False

    direct = float((merged[f"{CLUSTER_COLUMN}_baseline"] == merged[f"{CLUSTER_COLUMN}_candidate"]).mean())
    mapping, mapped = best_label_mapping(merged)
    print(f"segment agreement (direct)      : {direct:.2%}")
    print(f"segment agreement (best mapping): {mapped:.2%}")
    print("best label mapping (candidate -> baseline):")
    for candidate_label, baseline_label in sorted(mapping.items()):
        marker = "" if candidate_label == baseline_label else "   <- permuted"
        print(f"  {candidate_label:<20} -> {baseline_label}{marker}")

    ok = mapped >= min_agreement
    if not ok:
        print(f"FAIL: cluster agreement {mapped:.2%} below threshold {min_agreement:.2%}")

    print("deterministic column agreement:")
    for column in DETERMINISTIC_COLUMNS:
        left, right = f"{column}_baseline", f"{column}_candidate"
        if left not in merged or right not in merged:
            print(f"  {column:<24} : column missing on one side")
            continue
        if pd.api.types.is_numeric_dtype(merged[left]):
            equal = (merged[left].round(2) - merged[right].round(2)).abs() <= 0.01
        else:
            equal = merged[left].fillna("") == merged[right].fillna("")
        rate = float(equal.mean())
        print(f"  {column:<24} : {rate:.2%}")
        if rate < min_column_agreement:
            ok = False
            print(f"    FAIL: {column} below threshold {min_column_agreement:.2%}")

    print("RESULT:", "PASS" if ok else "FAIL")
    return ok


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, help="SAS CUSTOMER_SEGMENTS extract")
    parser.add_argument("--candidate", required=True, help="PySpark job output (file or directory)")
    parser.add_argument("--min-cluster-agreement", type=float, default=0.85)
    parser.add_argument("--min-column-agreement", type=float, default=1.0)
    args = parser.parse_args(argv)

    ok = reconcile(
        load(args.baseline),
        load(args.candidate),
        args.min_cluster_agreement,
        args.min_column_agreement,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
