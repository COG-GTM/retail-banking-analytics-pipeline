"""Compare the PySpark ``CUSTOMER_RISK_SCORES`` output against the oracle extract.

The oracle is the committed reference output of the original pipeline,
``data/03_sas_data_products/customer_risk_scores.csv``. It also replaces the
``PROC FREQ`` risk-tier monitoring block that the SAS program printed.

Three parity regimes, per the migration contract:

* **exact** — the deterministic, model-independent fields: composite score,
  the five components, tier, ``SCORE_DELTA_30D`` and both flags. These decide
  the verdict. Numeric comparison is at the precision of the target DDL, since
  the PySpark sink casts to DECIMAL while the oracle CSV carries full float
  precision.
* **known divergence** — ``PRIMARY_RISK_DRIVER``/``SECONDARY_RISK_DRIVER``,
  where the *oracle* deviates from the SAS source. Reported in full, but
  excluded from the verdict; the emitted report explains why.
* **distribution** — ``PROBABILITY_OF_DEFAULT``, because MLlib's LBFGS fit is
  not bit-reproducible against SAS's Fisher-scored ``PROC LOGISTIC``.

Exits non-zero when the exact regime fails, so it is safe to wire into CI.

Usage::

    python validation/compare_to_oracle.py \\
        --actual output/customer_risk_scores_csv \\
        --oracle data/03_sas_data_products/customer_risk_scores.csv \\
        --out validation/parity_report.md

Stdlib only — no pandas/numpy dependency.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

KEY = "CUSTOMER_ID"

#: column -> number of decimal places at which equality is asserted.
EXACT_NUMERIC_COLUMNS = {
    "COMPOSITE_RISK_SCORE": 2,
    "CREDIT_RISK_COMPONENT": 2,
    "BEHAVIOUR_RISK_COMPONENT": 2,
    "VELOCITY_RISK_COMPONENT": 2,
    "BUREAU_SCORE_COMPONENT": 2,
    "PAYMENT_HISTORY_COMPONENT": 2,
    "SCORE_DELTA_30D": 2,
}
EXACT_STRING_COLUMNS = (
    "RISK_TIER",
    "REVIEW_REQUIRED_FLAG",
    # Derived from PROBABILITY_OF_DEFAULT, but only through a `> 0.5` test that
    # no row comes near, so it is deterministic in practice and worth gating.
    "WATCH_LIST_FLAG",
)
#: Deterministic in the SAS sense, but the oracle itself deviates from the SAS
#: source here, so they are reported separately and excluded from the verdict.
#: See the "Known oracle divergence" section of the emitted report.
KNOWN_DIVERGENCE_STRING_COLUMNS = (
    "PRIMARY_RISK_DRIVER",
    "SECONDARY_RISK_DRIVER",
)
DISTRIBUTION_NUMERIC_COLUMNS = ("PROBABILITY_OF_DEFAULT",)
#: Also compared exactly above; the distribution view shows the counts.
DISTRIBUTION_CATEGORICAL_COLUMNS = ("WATCH_LIST_FLAG",)

MAX_SAMPLES = 10


@dataclass
class ColumnDiff:
    column: str
    compared: int = 0
    mismatches: int = 0
    max_abs_diff: float = 0.0
    samples: list[str] = field(default_factory=list)

    @property
    def match_rate(self) -> float:
        return 1.0 if not self.compared else 1 - self.mismatches / self.compared


def read_rows(path: Path) -> list[dict[str, str]]:
    """Read a CSV file, or every ``part-*.csv`` in a Spark output directory."""
    files = (
        sorted(p for p in path.glob("*.csv") if not p.name.startswith("_"))
        if path.is_dir()
        else [path]
    )
    if not files:
        raise FileNotFoundError(f"No CSV files found at {path}")
    rows: list[dict[str, str]] = []
    for file in files:
        with file.open(newline="") as handle:
            for row in csv.DictReader(handle):
                rows.append({(k or "").upper(): v for k, v in row.items()})
    if not rows:
        raise ValueError(f"No data rows found at {path}")
    return rows


def index_by_key(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row[KEY]: row for row in rows}


def duplicate_keys(rows: list[dict[str, str]]) -> list[str]:
    """Keys appearing more than once, which :func:`index_by_key` would collapse.

    The sink's own key-uniqueness gate should make this impossible, but the
    comparison must be able to see the failure rather than silently de-duplicate
    its way to full coverage.
    """
    counts = Counter(row[KEY] for row in rows)
    return sorted((key for key, n in counts.items() if n > 1), key=int)


def to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def norm_str(value: str | None) -> str:
    return (value or "").strip()


def compare_numeric_column(
    column: str,
    decimals: int,
    keys: list[str],
    actual: dict[str, dict[str, str]],
    oracle: dict[str, dict[str, str]],
) -> ColumnDiff:
    diff = ColumnDiff(column)
    for key in keys:
        left, right = to_float(actual[key].get(column)), to_float(oracle[key].get(column))
        if left is None and right is None:
            continue
        diff.compared += 1
        if left is None or right is None:
            diff.mismatches += 1
            if len(diff.samples) < MAX_SAMPLES:
                diff.samples.append(f"{KEY}={key}: actual={left!r} oracle={right!r}")
            continue
        diff.max_abs_diff = max(diff.max_abs_diff, abs(left - right))
        if round(left, decimals) != round(right, decimals):
            diff.mismatches += 1
            if len(diff.samples) < MAX_SAMPLES:
                diff.samples.append(
                    f"{KEY}={key}: actual={left:.6f} oracle={right:.6f} "
                    f"delta={left - right:+.6f}"
                )
    return diff


def compare_string_column(
    column: str,
    keys: list[str],
    actual: dict[str, dict[str, str]],
    oracle: dict[str, dict[str, str]],
) -> ColumnDiff:
    diff = ColumnDiff(column)
    for key in keys:
        left, right = norm_str(actual[key].get(column)), norm_str(oracle[key].get(column))
        diff.compared += 1
        if left != right:
            diff.mismatches += 1
            if len(diff.samples) < MAX_SAMPLES:
                diff.samples.append(f"{KEY}={key}: actual={left!r} oracle={right!r}")
    return diff


def quantile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def describe(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    return {
        "n": len(values),
        "mean": statistics.fmean(values),
        "stdev": statistics.pstdev(values),
        "min": min(values),
        "p25": quantile(values, 0.25),
        "p50": quantile(values, 0.50),
        "p75": quantile(values, 0.75),
        "max": max(values),
    }


def ks_statistic(left: list[float], right: list[float]) -> float:
    """Two-sample Kolmogorov-Smirnov statistic (max CDF gap)."""
    if not left or not right:
        return float("nan")
    ordered_left, ordered_right = sorted(left), sorted(right)
    grid = sorted(set(ordered_left) | set(ordered_right))
    gap = 0.0
    i = j = 0
    for value in grid:
        while i < len(ordered_left) and ordered_left[i] <= value:
            i += 1
        while j < len(ordered_right) and ordered_right[j] <= value:
            j += 1
        gap = max(gap, abs(i / len(ordered_left) - j / len(ordered_right)))
    return gap


def markdown_table(header: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(header) + " |",
             "| " + " | ".join("---" for _ in header) + " |"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def display_path(path: Path) -> str:
    """Path relative to the repo root, so the report does not embed a machine."""
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def build_report(actual_path: Path, oracle_path: Path) -> tuple[str, bool]:
    actual_rows = read_rows(actual_path)
    oracle_rows = read_rows(oracle_path)
    actual = index_by_key(actual_rows)
    oracle = index_by_key(oracle_rows)
    duplicates = duplicate_keys(actual_rows)

    only_actual = sorted(set(actual) - set(oracle), key=int)
    only_oracle = sorted(set(oracle) - set(actual), key=int)
    common = sorted(set(actual) & set(oracle), key=int)

    sections: list[str] = ["# Risk Scoring Parity Report", ""]
    sections.append(
        f"* PySpark output: `{display_path(actual_path)}` — {len(actual)} rows\n"
        f"* Oracle: `{display_path(oracle_path)}` — {len(oracle)} rows\n"
        f"* Joined on `{KEY}`: {len(common)} common, "
        f"{len(only_actual)} only in PySpark, {len(only_oracle)} only in oracle"
    )
    if only_actual[:MAX_SAMPLES]:
        sections.append(f"* Sample keys only in PySpark: {only_actual[:MAX_SAMPLES]}")
    if only_oracle[:MAX_SAMPLES]:
        sections.append(f"* Sample keys only in oracle: {only_oracle[:MAX_SAMPLES]}")
    if duplicates:
        sections.append(
            f"* **Duplicate `{KEY}` in the PySpark output: {len(duplicates)} key(s)**, "
            f"sample {duplicates[:MAX_SAMPLES]}"
        )

    # ---- exact parity ----------------------------------------------------- #
    diffs = [
        compare_numeric_column(column, decimals, common, actual, oracle)
        for column, decimals in EXACT_NUMERIC_COLUMNS.items()
    ] + [
        compare_string_column(column, common, actual, oracle)
        for column in EXACT_STRING_COLUMNS
    ]
    exact_ok = (
        all(d.mismatches == 0 for d in diffs)
        and not only_actual
        and not only_oracle
        and not duplicates
    )

    sections += ["", "## Exact parity (deterministic fields)", ""]
    sections.append(
        markdown_table(
            ["Column", "Compared", "Mismatches", "Match rate", "Max abs diff"],
            [
                [
                    d.column,
                    str(d.compared),
                    str(d.mismatches),
                    f"{d.match_rate:.4%}",
                    f"{d.max_abs_diff:.6g}" if d.column in EXACT_NUMERIC_COLUMNS else "-",
                ]
                for d in diffs
            ],
        )
    )
    mismatching = [d for d in diffs if d.samples]
    if mismatching:
        sections += ["", "### Mismatch samples", ""]
        for d in mismatching:
            sections.append(f"**{d.column}**")
            sections += [f"- {s}" for s in d.samples]
            sections.append("")

    # ---- known oracle divergence ------------------------------------------ #
    driver_diffs = [
        compare_string_column(column, common, actual, oracle)
        for column in KNOWN_DIVERGENCE_STRING_COLUMNS
    ]
    sections += ["", "## Known oracle divergence: risk-driver tie-breaking", ""]
    sections.append(
        "`CREDIT_RISK_COMPONENT` and `(100 - BUREAU_SCORE_COMPONENT)` are the same\n"
        "quantity whenever the clamps are inactive, so array index 0\n"
        "(`CREDIT_UTILIZATION`) and index 3 (`BUREAU_SCORE`) tie on nearly every row.\n"
        "The SAS source resolves the tie with a strict `>` comparison walked in array\n"
        "order, which keeps the **first** index; the oracle extract keeps `BUREAU_SCORE`.\n"
        "This port follows the SAS source, so these two columns are expected to differ\n"
        "from the oracle and are excluded from the verdict above. No simple tie rule\n"
        "reproduces the oracle either (last-index-wins still leaves 9 primary and 16\n"
        "secondary mismatches), so the oracle's ordering is not a documented rule to\n"
        "port."
    )
    sections.append("")
    sections.append(
        markdown_table(
            ["Column", "Compared", "Mismatches", "Agreement with oracle"],
            [
                [d.column, str(d.compared), str(d.mismatches), f"{d.match_rate:.2%}"]
                for d in driver_diffs
            ],
        )
    )
    sections.append("")

    # ---- distribution parity ---------------------------------------------- #
    sections += ["", "## Distribution parity (model-dependent fields)", ""]
    for column in DISTRIBUTION_NUMERIC_COLUMNS:
        left = [v for v in (to_float(actual[k].get(column)) for k in common) if v is not None]
        right = [v for v in (to_float(oracle[k].get(column)) for k in common) if v is not None]
        stats_left, stats_right = describe(left), describe(right)
        sections.append(f"### {column}")
        sections.append("")
        sections.append(
            markdown_table(
                ["Statistic", "PySpark", "Oracle", "Delta"],
                [
                    [
                        name,
                        f"{stats_left.get(name, float('nan')):.6f}",
                        f"{stats_right.get(name, float('nan')):.6f}",
                        f"{stats_left.get(name, float('nan')) - stats_right.get(name, float('nan')):+.6f}",
                    ]
                    for name in ("n", "mean", "stdev", "min", "p25", "p50", "p75", "max")
                ],
            )
        )
        sections.append("")
        sections.append(f"Two-sample KS statistic: `{ks_statistic(left, right):.4f}`")
        sections.append("")

    for column in DISTRIBUTION_CATEGORICAL_COLUMNS:
        left = Counter(norm_str(actual[k].get(column)) for k in common)
        right = Counter(norm_str(oracle[k].get(column)) for k in common)
        agree = sum(
            1 for k in common
            if norm_str(actual[k].get(column)) == norm_str(oracle[k].get(column))
        )
        sections.append(f"### {column}")
        sections.append("")
        sections.append(
            markdown_table(
                ["Value", "PySpark", "Oracle", "Delta"],
                [
                    [value, str(left.get(value, 0)), str(right.get(value, 0)),
                     f"{left.get(value, 0) - right.get(value, 0):+d}"]
                    for value in sorted(set(left) | set(right))
                ],
            )
        )
        sections.append("")
        sections.append(
            f"Row-level agreement: {agree}/{len(common)} "
            f"({agree / len(common):.2%})" if common else "No common rows."
        )
        sections.append("")

    # ---- tier distribution (replaces PROC FREQ) --------------------------- #
    sections += ["## Risk tier distribution (replaces `PROC FREQ`)", ""]
    tiers_actual = Counter(norm_str(r.get("RISK_TIER")) for r in actual.values())
    tiers_oracle = Counter(norm_str(r.get("RISK_TIER")) for r in oracle.values())
    order = ["LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"]
    known = [t for t in order if t in set(tiers_actual) | set(tiers_oracle)]
    unknown = sorted((set(tiers_actual) | set(tiers_oracle)) - set(order))
    sections.append(
        markdown_table(
            ["Risk tier", "PySpark", "PySpark %", "Oracle", "Oracle %", "Delta"],
            [
                [
                    tier,
                    str(tiers_actual.get(tier, 0)),
                    f"{tiers_actual.get(tier, 0) / max(len(actual), 1):.2%}",
                    str(tiers_oracle.get(tier, 0)),
                    f"{tiers_oracle.get(tier, 0) / max(len(oracle), 1):.2%}",
                    f"{tiers_actual.get(tier, 0) - tiers_oracle.get(tier, 0):+d}",
                ]
                for tier in known + unknown
            ],
        )
    )
    sections += ["", "## Verdict", ""]
    if exact_ok:
        sections.append("Exact parity on deterministic fields: **PASS**")
    else:
        reasons = []
        if duplicates:
            reasons.append(f"the output has duplicate `{KEY}` values")
        if only_actual or only_oracle:
            reasons.append("the row sets differ")
        if mismatching:
            reasons.append("see the mismatch samples above")
        sections.append(
            "Exact parity on deterministic fields: **FAIL** — " + "; ".join(reasons)
        )
    return "\n".join(sections) + "\n", exact_ok


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--actual",
        type=Path,
        default=REPO_ROOT / "output" / "customer_risk_scores_csv",
        help="PySpark output: a CSV file or a Spark CSV output directory",
    )
    parser.add_argument(
        "--oracle",
        type=Path,
        default=REPO_ROOT / "data" / "03_sas_data_products" / "customer_risk_scores.csv",
    )
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "validation" / "parity_report.md")
    parser.add_argument(
        "--allow-mismatch",
        action="store_true",
        help=("always exit 0; by default a failing exact regime exits 1 so the "
              "comparison can gate CI"),
    )
    args = parser.parse_args(argv)

    report, exact_ok = build_report(args.actual, args.oracle)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report)
    print(report)
    return 0 if exact_ok or args.allow_mismatch else 1


if __name__ == "__main__":
    raise SystemExit(main())
