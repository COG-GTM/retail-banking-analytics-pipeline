from __future__ import annotations

from dataclasses import dataclass, field

import pyspark.sql.functions as F


@dataclass
class ParityReport:
    checks: list = field(default_factory=list)  # (table, check, passed, detail)

    def add(self, table: str, check: str, passed: bool, detail: str = ""):
        self.checks.append((table, check, bool(passed), detail))

    @property
    def passed(self) -> bool:
        return all(c[2] for c in self.checks)

    def print_report(self):
        for table, check, passed, detail in self.checks:
            mark = "PASS" if passed else "FAIL"
            print(f"[{mark}] {table} :: {check} :: {detail}")


def _counts(df, col) -> dict:
    return {str(r[0]): r[1]
            for r in df.groupBy(col).count().collect()}


def _row_count_check(rep, table, a, e):
    ac, ec = a.count(), e.count()
    rep.add(table, "row_count", ac == ec, f"actual={ac} expected={ec}")
    return ac == ec


def _id_set_check(rep, table, a, e):
    diff = (a.select("customer_id").subtract(e.select("customer_id"))
            .union(e.select("customer_id")
                   .subtract(a.select("customer_id")))).count()
    rep.add(table, "customer_id_set", diff == 0, f"symmetric_diff={diff}")


def _dist_check(rep, table, col, a, e, tolerance_pp: float | None = None,
                sort_shares: bool = False):
    ac, ec = _counts(a, col), _counts(e, col)
    if tolerance_pp is None:
        rep.add(table, f"{col}_distribution", ac == ec,
                f"actual={ac} expected={ec}")
        return
    an, en = sum(ac.values()), sum(ec.values())
    if sort_shares:
        av = sorted(v / an for v in ac.values())
        ev = sorted(v / en for v in ec.values())
        ok = len(av) == len(ev) and all(
            abs(x - y) * 100 <= tolerance_pp for x, y in zip(av, ev))
        rep.add(table, f"{col}_share_vector", ok,
                f"actual={[round(x,3) for x in av]} "
                f"expected={[round(x,3) for x in ev]} tol={tolerance_pp}pp")
    else:
        keys = set(ac) | set(ec)
        worst = max(abs(ac.get(k, 0) / an - ec.get(k, 0) / en) * 100
                    for k in keys)
        rep.add(table, f"{col}_distribution", worst <= tolerance_pp,
                f"max_share_diff={worst:.2f}pp tol={tolerance_pp}pp")


def _max_abs_diff(a, e, col, join_on="customer_id"):
    j = (a.select("customer_id", F.col(col).cast("double").alias("a"))
         .join(e.select("customer_id",
                        F.col(col).cast("double").alias("e")), "customer_id"))
    row = j.agg(F.max(F.abs(F.col("a") - F.col("e"))).alias("d")).first()
    return float(row["d"]) if row and row["d"] is not None else 0.0


def _exact_diff_count(a, e, col, join_on="customer_id"):
    return (a.select("customer_id", F.col(col).alias("a"))
            .join(e.select("customer_id", F.col(col).alias("e")),
                  "customer_id")
            .filter(~F.col("a").eqNullSafe(F.col("e"))).count())


def compare_gold_outputs(actual: dict, expected: dict) -> ParityReport:
    """Parity of the 4 gold tables. k-means labels and logistic
    probabilities are compared with tolerances; deterministic columns
    must match exactly."""
    rep = ParityReport()

    a, e = actual["customer_segments"], expected["customer_segments"]
    _row_count_check(rep, "customer_segments", a, e)
    _id_set_check(rep, "customer_segments", a, e)
    _dist_check(rep, "customer_segments", "segment_name", a, e,
                tolerance_pp=10, sort_shares=True)
    for col in ("tenure_group", "age_group", "balance_tier",
                "cross_sell_flag", "upsell_flag", "retention_risk_flag"):
        _dist_check(rep, "customer_segments", col, a, e)

    a, e = actual["customer_risk_scores"], expected["customer_risk_scores"]
    _row_count_check(rep, "customer_risk_scores", a, e)
    _id_set_check(rep, "customer_risk_scores", a, e)
    _dist_check(rep, "customer_risk_scores", "risk_tier", a, e)
    d = _max_abs_diff(a, e, "composite_risk_score")
    rep.add("customer_risk_scores", "composite_risk_score_max_diff",
            d <= 0.01, f"max_abs_diff={d:.6f} tol=0.01")
    ma = a.agg(F.avg("probability_of_default")).first()[0] or 0.0
    me = e.agg(F.avg("probability_of_default")).first()[0] or 0.0
    rep.add("customer_risk_scores", "probability_of_default_mean",
            abs(float(ma) - float(me)) <= 0.05,
            f"actual={float(ma):.4f} expected={float(me):.4f} tol=0.05")

    a, e = (actual["transaction_analytics"],
            expected["transaction_analytics"])
    _row_count_check(rep, "transaction_analytics", a, e)
    _id_set_check(rep, "transaction_analytics", a, e)
    for col in ("total_transactions", "total_debit_amt", "net_cash_flow",
                "spend_percentile"):
        d = _max_abs_diff(a, e, col)
        rep.add("transaction_analytics", f"{col}_max_diff", d <= 0.01,
                f"max_abs_diff={d:.6f} tol=0.01")
    for col in ("anomaly_flag", "monthly_spend_trend"):
        n = _exact_diff_count(a, e, col)
        rep.add("transaction_analytics", f"{col}_exact", n == 0,
                f"mismatched_rows={n}")

    a, e = (actual["customer_master_profile"],
            expected["customer_master_profile"])
    _row_count_check(rep, "customer_master_profile", a, e)
    _id_set_check(rep, "customer_master_profile", a, e)
    for col, default in (("segment_name", "UNCLASSIFIED"),
                         ("risk_tier", "UNKNOWN")):
        ac = a.filter(F.col(col) == default).count()
        ec = e.filter(F.col(col) == default).count()
        rep.add("customer_master_profile", f"{col}_default_count",
                ac == ec, f"actual={ac} expected={ec}")

    return rep
