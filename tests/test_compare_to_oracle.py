"""Unit tests for the oracle parity harness (no Spark needed)."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from validation.compare_to_oracle import (  # noqa: E402
    build_report,
    compare_numeric_column,
    compare_string_column,
    describe,
    index_by_key,
    ks_statistic,
    quantile,
    read_rows,
)

COLUMNS = [
    "CUSTOMER_ID",
    "COMPOSITE_RISK_SCORE",
    "RISK_TIER",
    "PROBABILITY_OF_DEFAULT",
    "CREDIT_RISK_COMPONENT",
    "BEHAVIOUR_RISK_COMPONENT",
    "VELOCITY_RISK_COMPONENT",
    "BUREAU_SCORE_COMPONENT",
    "PAYMENT_HISTORY_COMPONENT",
    "PRIMARY_RISK_DRIVER",
    "SECONDARY_RISK_DRIVER",
    "SCORE_DELTA_30D",
    "WATCH_LIST_FLAG",
    "REVIEW_REQUIRED_FLAG",
]


def row(customer_id: int, **overrides: object) -> dict[str, str]:
    base = {
        "CUSTOMER_ID": str(customer_id),
        "COMPOSITE_RISK_SCORE": "25.00",
        "RISK_TIER": "MODERATE",
        "PROBABILITY_OF_DEFAULT": "0.000000",
        "CREDIT_RISK_COMPONENT": "20.00",
        "BEHAVIOUR_RISK_COMPONENT": "10.00",
        "VELOCITY_RISK_COMPONENT": "0.00",
        "BUREAU_SCORE_COMPONENT": "80.00",
        "PAYMENT_HISTORY_COMPONENT": "90.00",
        "PRIMARY_RISK_DRIVER": "CREDIT_UTILIZATION",
        "SECONDARY_RISK_DRIVER": "BUREAU_SCORE",
        "SCORE_DELTA_30D": "0.00",
        "WATCH_LIST_FLAG": "N",
        "REVIEW_REQUIRED_FLAG": "N",
    }
    base.update({k: str(v) for k, v in overrides.items()})
    return base


def write_csv(path: Path, rows: list[dict[str, str]], *, lowercase: bool = False) -> Path:
    header = [c.lower() for c in COLUMNS] if lowercase else COLUMNS
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for r in rows:
            writer.writerow([r[c] for c in COLUMNS])
    return path


def test_read_rows_upper_cases_headers_and_reads_a_directory(tmp_path: Path) -> None:
    directory = tmp_path / "out_csv"
    directory.mkdir()
    write_csv(directory / "part-00000.csv", [row(1)], lowercase=True)
    write_csv(directory / "part-00001.csv", [row(2)], lowercase=True)
    (directory / "_SUCCESS").write_text("")

    rows = read_rows(directory)

    assert len(rows) == 2
    assert set(rows[0]) == set(COLUMNS)
    assert {r["CUSTOMER_ID"] for r in rows} == {"1", "2"}


def test_read_rows_rejects_a_missing_directory(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(FileNotFoundError):
        read_rows(empty)


def test_numeric_comparison_uses_target_ddl_precision() -> None:
    # The sink casts to DECIMAL(5,2); the oracle CSV carries full float
    # precision. Equality must be asserted at the DDL's 2 decimal places.
    actual = index_by_key([row(1, CREDIT_RISK_COMPONENT="14.91")])
    oracle = index_by_key([row(1, CREDIT_RISK_COMPONENT="14.909090909090907")])

    diff = compare_numeric_column("CREDIT_RISK_COMPONENT", 2, ["1"], actual, oracle)

    assert diff.mismatches == 0
    assert diff.compared == 1
    assert diff.max_abs_diff == pytest.approx(0.00090909, abs=1e-6)


def test_numeric_comparison_flags_a_difference_above_the_precision() -> None:
    actual = index_by_key([row(1, COMPOSITE_RISK_SCORE="25.00")])
    oracle = index_by_key([row(1, COMPOSITE_RISK_SCORE="25.02")])

    diff = compare_numeric_column("COMPOSITE_RISK_SCORE", 2, ["1"], actual, oracle)

    assert diff.mismatches == 1
    assert diff.match_rate == 0.0
    assert "delta=-0.020000" in diff.samples[0]


def test_string_comparison_reports_mismatched_values() -> None:
    actual = index_by_key([row(1, RISK_TIER="HIGH"), row(2)])
    oracle = index_by_key([row(1, RISK_TIER="ELEVATED"), row(2)])

    diff = compare_string_column("RISK_TIER", ["1", "2"], actual, oracle)

    assert (diff.compared, diff.mismatches) == (2, 1)
    assert diff.samples == ["CUSTOMER_ID=1: actual='HIGH' oracle='ELEVATED'"]


def test_quantile_interpolates_and_describe_summarises() -> None:
    values = [1.0, 2.0, 3.0, 4.0]

    assert quantile(values, 0.5) == pytest.approx(2.5)
    assert quantile(values, 0.0) == 1.0
    assert quantile(values, 1.0) == 4.0

    stats = describe(values)
    assert stats["n"] == 4
    assert stats["mean"] == pytest.approx(2.5)
    assert stats["min"] == 1.0 and stats["max"] == 4.0


def test_ks_statistic_is_zero_for_identical_and_one_for_disjoint_samples() -> None:
    assert ks_statistic([0.1, 0.2, 0.3], [0.1, 0.2, 0.3]) == pytest.approx(0.0)
    # The real case: a constant 0.0 against a constant 0.05.
    assert ks_statistic([0.0] * 5, [0.05] * 5) == pytest.approx(1.0)


def test_report_passes_when_deterministic_fields_match(tmp_path: Path) -> None:
    rows = [row(1), row(2, RISK_TIER="LOW", COMPOSITE_RISK_SCORE="10.00")]
    actual = write_csv(tmp_path / "actual.csv", rows)
    oracle = write_csv(tmp_path / "oracle.csv", rows)

    report, exact_ok = build_report(actual, oracle)

    assert exact_ok
    assert "**PASS**" in report
    assert "2 common, 0 only in PySpark, 0 only in oracle" in report


def test_driver_divergence_is_reported_but_does_not_fail_the_verdict(
    tmp_path: Path,
) -> None:
    # The oracle flips the CREDIT_UTILIZATION / BUREAU_SCORE tie; the port
    # follows the SAS source, so this must not be scored as a failure.
    actual = write_csv(tmp_path / "actual.csv", [row(1)])
    oracle = write_csv(
        tmp_path / "oracle.csv",
        [
            row(
                1,
                PRIMARY_RISK_DRIVER="BUREAU_SCORE",
                SECONDARY_RISK_DRIVER="CREDIT_UTILIZATION",
            )
        ],
    )

    report, exact_ok = build_report(actual, oracle)

    assert exact_ok
    assert "Known oracle divergence" in report
    assert "| PRIMARY_RISK_DRIVER | 1 | 1 | 0.00% |" in report


def test_report_fails_on_a_deterministic_mismatch(tmp_path: Path) -> None:
    actual = write_csv(tmp_path / "actual.csv", [row(1, RISK_TIER="HIGH")])
    oracle = write_csv(tmp_path / "oracle.csv", [row(1, RISK_TIER="ELEVATED")])

    report, exact_ok = build_report(actual, oracle)

    assert not exact_ok
    assert "**FAIL**" in report
    assert "### Mismatch samples" in report


def test_report_fails_when_row_coverage_differs(tmp_path: Path) -> None:
    actual = write_csv(tmp_path / "actual.csv", [row(1), row(2)])
    oracle = write_csv(tmp_path / "oracle.csv", [row(1)])

    report, exact_ok = build_report(actual, oracle)

    assert not exact_ok
    assert "1 common, 1 only in PySpark, 0 only in oracle" in report


def test_probability_distribution_and_tier_sections_are_emitted(tmp_path: Path) -> None:
    actual = write_csv(
        tmp_path / "actual.csv",
        [row(1, PROBABILITY_OF_DEFAULT="0.000000"), row(2, RISK_TIER="LOW")],
    )
    oracle = write_csv(
        tmp_path / "oracle.csv",
        [row(1, PROBABILITY_OF_DEFAULT="0.050000"), row(2, RISK_TIER="LOW")],
    )

    report, exact_ok = build_report(actual, oracle)

    assert exact_ok  # PROBABILITY_OF_DEFAULT is distribution-parity only
    # One of the two rows differs (0.0 vs 0.05), so the CDFs part company by 1/2.
    assert "Two-sample KS statistic: `0.5000`" in report
    assert "## Risk tier distribution (replaces `PROC FREQ`)" in report
    assert "| MODERATE | 1 | 50.00% | 1 | 50.00% | +0 |" in report
