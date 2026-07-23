"""Shared pytest fixtures: a session-scoped Delta-enabled SparkSession and a
per-test :class:`Config` pointing at an isolated temp Delta warehouse seeded from
the repo's ``data/`` CSVs.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from common.config import Config
from common.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("rbap-tests")
    yield session
    session.stop()


@pytest.fixture()
def cfg(tmp_path) -> Config:
    """Config with an isolated warehouse dir and the repo seed data dir."""
    data_dir = Path(__file__).resolve().parents[1] / "data"
    return dataclasses.replace(
        Config(),
        warehouse_dir=str(tmp_path / "warehouse"),
        data_dir=str(data_dir),
        run_date="2026-04-10",
    )
