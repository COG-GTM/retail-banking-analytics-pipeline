from __future__ import annotations

import pyspark.sql.functions as F


def datediff_month(start_col, end_col):
    """DuckDB datediff('month', a, b) semantics: counts month-boundary
    crossings = (year(b)-year(a))*12 + (month(b)-month(a)).
    Deliberately NOT months_between (which returns fractional full periods)."""
    return (F.year(end_col) - F.year(start_col)) * 12 + \
        (F.month(end_col) - F.month(start_col))


def datediff_year(start_col, end_col):
    """DuckDB datediff('year', a, b): year-boundary crossings."""
    return F.year(end_col) - F.year(start_col)
