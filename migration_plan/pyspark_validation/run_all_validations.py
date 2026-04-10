#!/usr/bin/env python3
"""
Run All PySpark Validation Scripts
==================================
Executes every validation module and produces a consolidated report.

Usage:
    python run_all_validations.py
"""
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

# Add parent to path so we can import sibling modules
sys.path.insert(0, str(Path(__file__).resolve().parent))

import validate_stg_customer_360
import validate_stg_txn_summary
import validate_stg_risk_factors
import validate_customer_segments
import validate_txn_analytics
import validate_risk_scoring
import validate_data_products

VALIDATIONS = [
    ("STG_CUSTOMER_360  (BTEQ 01)", validate_stg_customer_360),
    ("STG_TXN_SUMMARY   (BTEQ 02)", validate_stg_txn_summary),
    ("STG_RISK_FACTORS   (BTEQ 03)", validate_stg_risk_factors),
    ("CUSTOMER_SEGMENTS  (SAS  01)", validate_customer_segments),
    ("TXN_ANALYTICS      (SAS  02)", validate_txn_analytics),
    ("RISK_SCORING       (SAS  03)", validate_risk_scoring),
    ("DATA_PRODUCTS      (SAS  04)", validate_data_products),
]


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("pyspark_migration_validation_suite")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 74)
    print("  PySpark Migration Validation Suite")
    print("  BTEQ/SAS -> PySpark Translation Parity Checks")
    print("=" * 74)

    results_summary = []
    overall_start = time.time()

    for label, module in VALIDATIONS:
        print(f"\n{'─' * 74}")
        print(f"  Running: {label}")
        print(f"{'─' * 74}")

        t0 = time.time()
        try:
            results = module.validate(spark)
            elapsed = time.time() - t0
            passed = results.get("overall_pass", False)

            for k, v in results.items():
                if isinstance(v, dict):
                    print(f"    {k}:")
                    for sub_k, sub_v in v.items():
                        print(f"      {str(sub_k):30s} : {sub_v}")
                elif isinstance(v, list):
                    print(f"    {k}: {v}")
                else:
                    status = "PASS" if v is True else ("FAIL" if v is False else str(v))
                    print(f"    {k:38s} : {status}")

            status_str = "PASS" if passed else "FAIL"
            results_summary.append((label, status_str, elapsed))
            print(f"\n  -> {label}: {status_str} ({elapsed:.1f}s)")

        except Exception as e:
            elapsed = time.time() - t0
            results_summary.append((label, "ERROR", elapsed))
            print(f"\n  -> {label}: ERROR ({elapsed:.1f}s)")
            print(f"     {type(e).__name__}: {e}")

    total_elapsed = time.time() - overall_start

    # ── Summary ──
    print(f"\n{'=' * 74}")
    print("  VALIDATION SUMMARY")
    print(f"{'=' * 74}")
    print(f"  {'Script':<40s} {'Status':<8s} {'Time':>6s}")
    print(f"  {'─' * 40} {'─' * 8} {'─' * 6}")

    pass_count = 0
    fail_count = 0
    error_count = 0

    for label, status, elapsed in results_summary:
        print(f"  {label:<40s} {status:<8s} {elapsed:5.1f}s")
        if status == "PASS":
            pass_count += 1
        elif status == "FAIL":
            fail_count += 1
        else:
            error_count += 1

    print(f"  {'─' * 56}")
    print(f"  Total: {len(results_summary)} validations | "
          f"{pass_count} passed | {fail_count} failed | {error_count} errors")
    print(f"  Elapsed: {total_elapsed:.1f}s")
    print(f"{'=' * 74}")

    spark.stop()

    # Exit code: 0 if all pass, 1 otherwise
    sys.exit(0 if fail_count == 0 and error_count == 0 else 1)


if __name__ == "__main__":
    main()
