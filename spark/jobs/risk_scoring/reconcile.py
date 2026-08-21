"""Reconcile the PySpark output against the SAS CUSTOMER_RISK_SCORES baseline.

    python -m risk_scoring.reconcile --candidate build/risk_scoring/customer_risk_scores \\
        --baseline data/03_sas_data_products/customer_risk_scores.csv

Reports, for the reconciliation cohort: row-count parity, composite score and probability
differences against the agreed tolerance, tier-assignment agreement and risk-driver agreement.
Exits non-zero when a tolerance is breached.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

SCORE_TOLERANCE = 0.01
PROBABILITY_TOLERANCE = 0.000001


@dataclass
class ReconciliationResult:
    baseline_rows: int
    candidate_rows: int
    matched_rows: int
    max_score_diff: float
    max_probability_diff: float
    tier_agreement_pct: float
    primary_driver_agreement_pct: float
    secondary_driver_agreement_pct: float
    score_breaches: int
    probability_breaches: int

    @property
    def passed(self) -> bool:
        return self.score_breaches == 0 and self.probability_breaches == 0


def _read(path: str) -> pd.DataFrame:
    target = Path(path)
    if target.is_dir():
        parts = sorted(target.glob("*.csv"))
        if not parts:
            raise FileNotFoundError(f"No CSV part files under {target}")
        frame = pd.concat([pd.read_csv(part) for part in parts], ignore_index=True)
    else:
        frame = pd.read_csv(target)
    frame.columns = [c.upper() for c in frame.columns]
    return frame


def reconcile(
    candidate: pd.DataFrame,
    baseline: pd.DataFrame,
    score_tolerance: float = SCORE_TOLERANCE,
    probability_tolerance: float = PROBABILITY_TOLERANCE,
) -> ReconciliationResult:
    merged = baseline.merge(candidate, on="CUSTOMER_ID", suffixes=("_SAS", "_SPARK"))
    score_diff = (merged["COMPOSITE_RISK_SCORE_SAS"] - merged["COMPOSITE_RISK_SCORE_SPARK"]).abs()
    prob_diff = (
        merged["PROBABILITY_OF_DEFAULT_SAS"] - merged["PROBABILITY_OF_DEFAULT_SPARK"]
    ).abs()
    matched = len(merged)

    def agreement(column: str) -> float:
        if matched == 0:
            return 0.0
        left = merged[f"{column}_SAS"].fillna("")
        right = merged[f"{column}_SPARK"].fillna("")
        return round(float((left == right).mean()) * 100.0, 4)

    return ReconciliationResult(
        baseline_rows=len(baseline),
        candidate_rows=len(candidate),
        matched_rows=matched,
        max_score_diff=float(score_diff.max()) if matched else 0.0,
        max_probability_diff=float(prob_diff.max()) if matched else 0.0,
        tier_agreement_pct=agreement("RISK_TIER"),
        primary_driver_agreement_pct=agreement("PRIMARY_RISK_DRIVER"),
        secondary_driver_agreement_pct=agreement("SECONDARY_RISK_DRIVER"),
        score_breaches=int((score_diff > score_tolerance).sum()),
        probability_breaches=int((prob_diff > probability_tolerance).sum()),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile risk scores against the SAS baseline")
    parser.add_argument("--candidate", required=True, help="PySpark output CSV file or directory")
    parser.add_argument("--baseline", required=True, help="SAS baseline CSV")
    parser.add_argument("--score-tolerance", type=float, default=SCORE_TOLERANCE)
    parser.add_argument("--probability-tolerance", type=float, default=PROBABILITY_TOLERANCE)
    parser.add_argument(
        "--ignore-probability",
        action="store_true",
        help="Skip the probability tolerance check (model refit expected to differ)",
    )
    args = parser.parse_args(argv)

    result = reconcile(
        _read(args.candidate),
        _read(args.baseline),
        args.score_tolerance,
        args.probability_tolerance,
    )
    print(json.dumps(asdict(result), indent=2))
    if args.ignore_probability:
        return 0 if result.score_breaches == 0 else 1
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
