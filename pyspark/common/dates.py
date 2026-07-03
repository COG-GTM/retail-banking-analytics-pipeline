"""Date helpers that reproduce Teradata's date arithmetic in Spark columns.

The BTEQ scripts derive ``AGE`` and ``TENURE_MONTHS`` from ``CURRENT_DATE`` and
use ``ADD_MONTHS`` / date-minus-integer for the lookback windows.  These helpers
express the same semantics as Spark ``Column`` expressions driven by the
config-supplied ``run_date`` (never the wall clock inside a transform).

Fidelity notes (see MIGRATION_NOTES.md):
* Teradata ``CAST(x AS SMALLINT/INTEGER)`` truncates toward zero.  ``AGE`` and
  ``TENURE_MONTHS`` therefore use ``floor`` for the (always non-negative) values.
"""

from __future__ import annotations

import datetime as _dt

from pyspark.sql import Column
from pyspark.sql import functions as F


def age_expr(dob_col: str, run_date: _dt.date) -> Column:
    """CAST((CURRENT_DATE - DATE_OF_BIRTH) / 365.25 AS SMALLINT)."""
    days = F.datediff(F.lit(run_date), F.col(dob_col))
    return F.floor(days / F.lit(365.25)).cast("short")


def tenure_months_expr(since_col: str, run_date: _dt.date) -> Column:
    """CAST(MONTHS_BETWEEN(CURRENT_DATE, CUSTOMER_SINCE) AS INTEGER)."""
    months = F.months_between(F.lit(run_date), F.col(since_col))
    return F.floor(months).cast("int")


def days_since_expr(date_col: str, run_date: _dt.date) -> Column:
    """CAST(CURRENT_DATE - MAX(date) AS INTEGER) style recency."""
    return F.datediff(F.lit(run_date), F.col(date_col)).cast("int")


def load_timestamp() -> _dt.datetime:
    """Naive-UTC ``LOAD_TS`` marker (replaces SAS ``datetime()`` / CURRENT_TIMESTAMP)."""
    return _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
