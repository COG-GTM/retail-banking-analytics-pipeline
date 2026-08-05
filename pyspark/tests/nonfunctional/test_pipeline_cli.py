"""Non-functional tier: the driver's CLI surface — the replacement for the master shell script."""

from __future__ import annotations

import json

import pytest

from common.io import InMemoryDataIO
from orchestration.pipeline import PIPELINE, main, post_run_counts

pytestmark = pytest.mark.nonfunctional


def test_dry_run_lists_the_plan_and_touches_no_data(capsys, tmp_path) -> None:
    metrics = tmp_path / "plan.json"

    assert main(["--dry-run", "--run-date", "2026-04-10", "--metrics-json", str(metrics)]) == 0

    printed = capsys.readouterr().out
    assert "DRY RUN" in printed
    for node in PIPELINE:
        assert node.name in printed
        assert node.legacy_source in printed

    plan = json.loads(metrics.read_text(encoding="utf-8"))
    assert plan["dry_run"] is True
    assert plan["jobs"] == []


def test_dry_run_shows_the_legacy_three_phase_shape(capsys) -> None:
    main(["--dry-run", "--run-date", "2026-04-10"])
    printed = capsys.readouterr().out

    assert "1. BTEQ" in printed
    assert "2. SAS" in printed
    assert "3. Post: Validation & notification" in printed


def test_only_selects_a_subset(capsys) -> None:
    main(["--dry-run", "--run-date", "2026-04-10", "--only", "01_stg_customer_360"])
    printed = capsys.readouterr().out

    assert "01_stg_customer_360" in printed
    assert "04_sas_data_products" not in printed


def test_an_unmatched_only_is_an_error_not_an_empty_run() -> None:
    with pytest.raises(SystemExit, match="no jobs matched"):
        main(["--dry-run", "--run-date", "2026-04-10", "--only", "99_not_a_job"])


def test_post_run_counts_flag_missing_tables_with_minus_one() -> None:
    counts = post_run_counts(InMemoryDataIO())

    assert counts
    assert set(counts.values()) == {-1}
