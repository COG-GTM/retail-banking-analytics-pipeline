"""Parity harness: compare a PySpark output DataFrame to the committed legacy
output (the CSV fixtures under ``data/``), keyed by a business key.

Staging tables are expected to match **exactly**; KMeans/logistic-derived data
product columns are compared with a numeric tolerance (see ``compare``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from pyspark.sql import DataFrame


@dataclass
class ParityReport:
    total: int
    matched: int
    key_mismatches: list = field(default_factory=list)   # keys only in one side
    value_mismatches: list = field(default_factory=list)  # (key, col, expected, actual)

    @property
    def ok(self) -> bool:
        return not self.key_mismatches and not self.value_mismatches


def _to_dict(df: DataFrame, key: str, cols: list[str]) -> dict:
    rows = df.select(key, *cols).collect()
    return {r[key]: {c: r[c] for c in cols} for r in rows}


def _close(a, b, tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
        return abs(float(a) - float(b)) <= tol
    return a == b


def compare(
    actual: DataFrame,
    expected: DataFrame,
    key: str = "customer_id",
    cols: list[str] | None = None,
    tol: float = 0.0,
    tol_cols: dict[str, float] | None = None,
    ignore: tuple[str, ...] = ("load_ts",),
) -> ParityReport:
    """Compare ``actual`` vs ``expected`` keyed by ``key``.

    ``tol`` is the default absolute numeric tolerance; ``tol_cols`` overrides it
    per column (e.g. tolerance-based KMeans/logistic columns). ``ignore`` skips
    volatile columns (load timestamps).
    """
    tol_cols = tol_cols or {}
    if cols is None:
        cols = [c for c in expected.columns if c != key and c not in ignore]

    exp = _to_dict(expected, key, cols)
    act = _to_dict(actual, key, cols)

    report = ParityReport(total=len(exp), matched=0)
    for k, exp_row in exp.items():
        if k not in act:
            report.key_mismatches.append(k)
            continue
        act_row = act[k]
        row_ok = True
        for c in cols:
            col_tol = tol_cols.get(c, tol)
            if not _close(exp_row[c], act_row[c], col_tol):
                report.value_mismatches.append((k, c, exp_row[c], act_row[c]))
                row_ok = False
        if row_ok:
            report.matched += 1
    for k in act:
        if k not in exp:
            report.key_mismatches.append(k)
    return report
