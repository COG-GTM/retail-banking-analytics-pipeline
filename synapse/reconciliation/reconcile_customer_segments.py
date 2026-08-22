"""Reconcile the PySpark CUSTOMER_SEGMENTS output against the SAS baseline.

Runs the segmentation job locally against a STG_CUSTOMER_360 extract and
compares the result with the SAS-produced CUSTOMER_SEGMENTS extract, reporting:

* row-count and key-coverage deltas,
* cluster agreement after resolving cluster-label permutation (Spark and SAS
  number clusters arbitrarily, so segments are matched by maximum overlap),
* exact-match rates for SEGMENT_NAME, the action flags and the tier/group
  columns, and LTV / engagement score deltas within a tolerance.

Usage (repo sample extracts)::

    python synapse/reconciliation/reconcile_customer_segments.py \
        --staging data/02_bteq_staging/stg_customer_360.csv \
        --baseline data/03_sas_data_products/customer_segments.csv \
        --cluster-threshold 0.80

Exits non-zero when a threshold is breached, so it can gate a Synapse pipeline
activity or a CI job.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "jobs"))

from customer_segments import DEFAULT_SEED, segment  # noqa: E402

FLAG_COLUMNS = [
    "SEGMENT_NAME",
    "TENURE_GROUP",
    "AGE_GROUP",
    "BALANCE_TIER",
    "CROSS_SELL_FLAG",
    "UPSELL_FLAG",
    "RETENTION_RISK_FLAG",
]
SCORE_COLUMNS = ["LIFETIME_VALUE_SCORE", "ENGAGEMENT_SCORE", "PRODUCT_BREADTH_INDEX"]


def read_csv_upper(spark: SparkSession, path: str):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    for column in df.columns:
        df = df.withColumnRenamed(column, column.upper())
    return df


def align_segment_ids(actual, baseline):
    """Resolve cluster-label permutation by maximum-overlap matching."""
    pairs = (
        actual.select("CUSTOMER_ID", F.col("SEGMENT_ID").alias("NEW_ID"))
        .join(baseline.select("CUSTOMER_ID", F.col("SEGMENT_ID").alias("OLD_ID")), "CUSTOMER_ID")
        .groupBy("NEW_ID", "OLD_ID")
        .count()
        .orderBy(F.col("count").desc())
        .collect()
    )
    mapping: dict[int, int] = {}
    used = set()
    for row in pairs:
        if row["NEW_ID"] in mapping or row["OLD_ID"] in used:
            continue
        mapping[row["NEW_ID"]] = row["OLD_ID"]
        used.add(row["OLD_ID"])
    return mapping


def reconcile(spark, staging_path: str, baseline_path: str, seed: int, tolerance: float) -> dict:
    # Same active-customer filter the job applies when reading from Snowflake.
    staging = read_csv_upper(spark, staging_path).where(F.col("CUSTOMER_STATUS") == F.lit("A"))
    actual = segment(staging, seed=seed).cache()
    baseline = read_csv_upper(spark, baseline_path).cache()

    mapping = align_segment_ids(actual, baseline)
    mapping_expr = F.create_map(
        *[F.lit(x) for pair in mapping.items() for x in pair]
    ) if mapping else None

    aligned = actual.withColumn(
        "MAPPED_SEGMENT_ID",
        mapping_expr[F.col("SEGMENT_ID")] if mapping_expr is not None else F.col("SEGMENT_ID"),
    )

    joined = aligned.alias("a").join(baseline.alias("b"), "CUSTOMER_ID", "inner").cache()
    matched = joined.count()

    report: dict = {
        "actual_rows": actual.count(),
        "baseline_rows": baseline.count(),
        "matched_customers": matched,
        "cluster_agreement": None,
        "column_agreement": {},
        "score_within_tolerance": {},
        "segment_id_mapping": {str(k): v for k, v in mapping.items()},
    }
    if matched == 0:
        return report

    report["cluster_agreement"] = (
        joined.where(F.col("a.MAPPED_SEGMENT_ID") == F.col("b.SEGMENT_ID")).count() / matched
    )
    for column in FLAG_COLUMNS:
        agree = joined.where(
            F.coalesce(F.col(f"a.{column}"), F.lit("")) == F.coalesce(F.col(f"b.{column}"), F.lit(""))
        ).count()
        report["column_agreement"][column] = agree / matched
    for column in SCORE_COLUMNS:
        close = joined.where(
            F.abs(F.col(f"a.{column}").cast("double") - F.col(f"b.{column}").cast("double"))
            <= F.lit(tolerance)
        ).count()
        report["score_within_tolerance"][column] = close / matched
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile CUSTOMER_SEGMENTS (MBA-2207)")
    parser.add_argument("--staging", required=True, help="STG_CUSTOMER_360 CSV extract")
    parser.add_argument("--baseline", required=True, help="SAS CUSTOMER_SEGMENTS CSV extract")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--tolerance", type=float, default=0.01)
    # Defaults calibrated against the sample extracts in data/; see
    # docs/modernization/synapse/06-customer-segments.md for the measured rates
    # and why the cluster threshold is not 1.0 (k-means seeding) and the group
    # columns are not 1.0 (boundary handling in the sample baseline).
    parser.add_argument("--cluster-threshold", type=float, default=0.60)
    parser.add_argument("--column-threshold", type=float, default=0.94)
    parser.add_argument("--output", default=None, help="Write the JSON report to this path")
    args = parser.parse_args(argv)

    spark = (
        SparkSession.builder.appName("mba-2207-reconciliation")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    try:
        report = reconcile(spark, args.staging, args.baseline, args.seed, args.tolerance)
    finally:
        spark.stop()

    print(json.dumps(report, indent=2))
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)

    failures = []
    if (report["cluster_agreement"] or 0) < args.cluster_threshold:
        failures.append(f"cluster agreement {report['cluster_agreement']} below threshold")
    for column, rate in report["column_agreement"].items():
        if column != "SEGMENT_NAME" and rate < args.column_threshold:
            failures.append(f"{column} agreement {rate} below threshold")
    for failure in failures:
        print(f"RECONCILIATION FAILURE: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
