"""Test helper: build DataFrames from a DDL schema string while coercing plain
Python ``int``/``float`` literals into the exact types PySpark's strict
``createDataFrame`` verifier requires (``Decimal`` for ``decimal(..)`` columns,
``float`` for ``double``/``float`` columns)."""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Iterable, Sequence

_FIELD_RE = re.compile(r"^\s*(\w+)\s+(.+?)\s*$")
_INT_TYPES = {"int", "integer", "smallint", "tinyint", "bigint", "long", "short"}
_FLOAT_TYPES = {"double", "float"}


def _split_fields(schema: str) -> list[str]:
    """Split on top-level commas only (so ``decimal(18,2)`` stays intact)."""
    fields, depth, start = [], 0, 0
    for i, ch in enumerate(schema):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            fields.append(schema[start:i])
            start = i + 1
    fields.append(schema[start:])
    return fields


def _parse_types(schema: str) -> list[str]:
    types = []
    for field in _split_fields(schema):
        m = _FIELD_RE.match(field)
        assert m, f"cannot parse schema field: {field!r}"
        types.append(m.group(2).strip().lower())
    return types


def _coerce(value, dtype: str):
    if value is None:
        return None
    if dtype.startswith("decimal"):
        return Decimal(str(value))
    if dtype in _FLOAT_TYPES:
        return float(value)
    if dtype in _INT_TYPES:
        return int(value)
    return value


def make_df(spark, schema: str, rows: Iterable[Sequence]):
    types = _parse_types(schema)
    coerced = [tuple(_coerce(v, t) for v, t in zip(row, types)) for row in rows]
    return spark.createDataFrame(coerced, schema)
