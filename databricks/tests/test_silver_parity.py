from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from retail_banking.config import RunConfig
from tests.local_pipeline import run_bronze, run_silver

REPO_ROOT = Path(__file__).parents[2]
CASES = [
    ("stg_customer_360", ["customer_id"], 478),
    ("stg_txn_summary", ["customer_id", "account_id"], 1251),
    ("stg_risk_factors", ["customer_id"], 478),
]


@pytest.fixture(scope="session")
def pipeline(spark):
    cfg = RunConfig(
        catalog=None,
        run_date=pd.Timestamp("2026-04-10").date(),
        lookback_months=12,
        dq_min_rows=1,
        mlflow_enabled=False,
    )
    run_bronze(spark, cfg)
    run_silver(spark, cfg)
    return cfg


def _is_null(value) -> bool:
    return pd.isna(value)


def test_silver_parity(pipeline, spark):
    for table, keys, expected_count in CASES:
        reference = pd.read_csv(REPO_ROOT / "data" / "02_bteq_staging" / f"{table}.csv")
        ours = spark.table(pipeline.fqn(pipeline.silver_schema, table)).toPandas()
        assert len(reference) == expected_count
        assert len(ours) == expected_count
        merged = reference.merge(
            ours, on=keys, how="outer", suffixes=("_reference", "_ours"), indicator=True
        )
        assert set(merged["_merge"]) == {"both"}, f"{table} key mismatch"
        for column in reference.columns:
            if column == "load_ts" or column in keys:
                continue
            left = merged[f"{column}_ours"]
            right = merged[f"{column}_reference"]
            both_null = left.isna() & right.isna()
            numeric = (
                pd.to_numeric(left, errors="coerce").notna()
                | pd.to_numeric(right, errors="coerce").notna()
            )
            equal = both_null.copy()
            equal |= numeric & np.isclose(
                pd.to_numeric(left, errors="coerce"),
                pd.to_numeric(right, errors="coerce"),
                atol=0.011,
                equal_nan=True,
            )
            equal |= (~numeric) & (
                left.fillna("<NULL>").astype(str) == right.fillna("<NULL>").astype(str)
            )
            if not equal.all():
                examples = merged.loc[
                    ~equal, keys + [f"{column}_ours", f"{column}_reference"]
                ].head(5)
                raise AssertionError(
                    f"{table}.{column}: {(~equal).sum()} mismatches; examples:\n{examples.to_string(index=False)}"
                )
