"""Helpers for building small, typed in-memory source DataFrames in tests."""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession

from common import schemas

# Sensible per-column placeholders so tests only specify the fields they care
# about while still producing a fully-typed row.
_PLACEHOLDERS = {
    "string": "X",
    "date": _dt.date(2020, 1, 1),
    "timestamp": _dt.datetime(2020, 1, 1, 0, 0, 0),
}


def _default_for(field) -> object:
    tname = field.dataType.typeName()
    if tname == "decimal":
        return Decimal("0")
    if tname in ("integer", "long", "short"):
        return 0
    if tname in ("double", "float"):
        return 0.0
    return _PLACEHOLDERS.get(tname)


def make_df(spark: SparkSession, spec: schemas.TableSpec, rows: list[dict]) -> DataFrame:
    """Build a DataFrame for ``spec`` from partial dicts.

    Unspecified nullable columns default to NULL (matching real source data);
    unspecified NOT NULL columns get a typed placeholder so rows stay valid.
    """
    struct = spec.struct
    materialised = []
    for row in rows:
        values = []
        for field in struct.fields:
            if field.name in row:
                v = row[field.name]
                if field.dataType.typeName() == "decimal" and v is not None and not isinstance(v, Decimal):
                    v = Decimal(str(v))
                values.append(v)
            elif field.nullable:
                values.append(None)
            else:
                values.append(_default_for(field))
        materialised.append(tuple(values))
    return spark.createDataFrame(materialised, schema=struct)


def customers(spark, rows):
    return make_df(spark, schemas.CUSTOMERS, rows)


def accounts(spark, rows):
    return make_df(spark, schemas.ACCOUNTS, rows)


def addresses(spark, rows):
    return make_df(spark, schemas.ADDRESSES, rows)


def transactions(spark, rows):
    return make_df(spark, schemas.TRANSACTIONS, rows)


def transaction_types(spark, rows):
    return make_df(spark, schemas.TRANSACTION_TYPES, rows)


def bureau_scores(spark, rows):
    return make_df(spark, schemas.CUSTOMER_BUREAU_SCORES, rows)
